#include "gateway/gateway_service.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <thread>
#include <algorithm>

#include <grpcpp/grpcpp.h>

#include "metrics.h"

namespace sparkpush::inference {
namespace {
using Clock = std::chrono::steady_clock;
int64_t ElapsedMs(Clock::time_point start) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      Clock::now() - start).count();
}
class ScopeExit {
 public:
  explicit ScopeExit(std::function<void()> fn) : fn_(std::move(fn)) {}
  ~ScopeExit() { fn_(); }
 private:
  std::function<void()> fn_;
};
}  // namespace

grpc::Status GatewayService::Generate(grpc::ServerContext* context,
    const GenerateRequest* request, grpc::ServerWriter<GenerateChunk>* writer) {
  auto& metrics = sparkpush::MetricsRegistry::Instance();
  const auto start = Clock::now();
  metrics.Increment("inference_requests_total");
  metrics.Increment("inference_requests_active");
  ScopeExit metrics_done([&] {
    metrics.Increment("inference_requests_active", -1);
    metrics.Observe("inference_total_latency_ms", ElapsedMs(start));
  });
  auto fail = [&](grpc::StatusCode code, const std::string& error) {
    metrics.Increment(code == grpc::StatusCode::CANCELLED ?
        "inference_requests_cancelled_total" : "inference_requests_failed_total");
    return grpc::Status(code, error);
  };
  // 空请求只认 prompt 和 messages，不认 chat_turns。只填 chat_turns 会在这里被拒。
  // Bridge 每次都写 prompt（最后一条），所以现有调用过得去。
  // 这组和 worker 的空请求检查必须一致。只放宽网关的话，请求占坑之后仍会被 worker 拒。
  if (request->request_id().empty() || request->model().empty() ||
      (request->prompt().empty() && request->messages().empty()))
    return fail(grpc::StatusCode::INVALID_ARGUMENT, "request_id, model and prompt/messages required");
  auto state = std::make_shared<RequestState>();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // 重复的 request_id 在入队前拒绝。后来的请求不能把这次挤掉。
    if (active_.count(request->request_id()))
      return fail(grpc::StatusCode::ALREADY_EXISTS, "request_id already active");
    // 现在没有健康 worker 就直接失败，不进队列干等。
    // 排队之后 worker 消失是下面的「workers became unavailable」。
    if (registry_->FindWorkersForModel(request->model()).empty()) {
      metrics.Increment("inference_scheduler_no_worker_total");
      return fail(grpc::StatusCode::UNAVAILABLE, "no healthy worker for model");
    }
    if (waiting_.size() >= options_.max_queue) {
      metrics.Increment("inference_requests_rejected_total");
      return fail(grpc::StatusCode::RESOURCE_EXHAUSTED, "inference queue full");
    }
    active_.emplace(request->request_id(), state);
    waiting_.push_back(request->request_id());
    metrics.Set("inference_requests_waiting", waiting_.size());
  }
  // 离开时同时删 active_ 和 waiting_。取消、超时、成功都走这里。
  // 已经 pop 过的 id 再 erase 是空操作。这次通知是给还没占到坑的队头的。
  ScopeExit remove_request([&] {
    std::lock_guard<std::mutex> lock(mutex_);
    active_.erase(request->request_id());
    waiting_.erase(std::remove(waiting_.begin(), waiting_.end(),
        request->request_id()), waiting_.end());
    metrics.Set("inference_requests_waiting", waiting_.size());
    capacity_changed_.notify_all();
  });
  std::optional<WorkerInfo> selected;
  const auto queue_deadline = Clock::now() + options_.queue_timeout;
  {
    // Selection and capacity reservation share this lock. No network calls
    // occur under it. FIFO admission is global; active RPCs run independently.
    // 译：选 worker 和占坑在同一把锁里完成，锁内不做任何网络调用。
    // 准入是全局 FIFO；一旦选出，真正的 RPC 在锁外各自跑。
    std::unique_lock<std::mutex> lock(mutex_);
    while (!selected) {
      if (context->IsCancelled() || state->cancelled)
        return fail(grpc::StatusCode::CANCELLED, "queued generation cancelled");
      // 两套时钟都要看。调用方 deadline 是 system_clock，排队超时是 steady_clock。
      // 排队会吃掉调用方的剩余时间。下游 deadline 要等占坑成功才从这里抄过去。
      if (std::chrono::system_clock::now() >= context->deadline() ||
          Clock::now() >= queue_deadline) {
        metrics.Increment("inference_queue_timeouts_total");
        return fail(grpc::StatusCode::DEADLINE_EXCEEDED, "inference queue timeout");
      }
      if (waiting_.front() == request->request_id()) {
        auto workers = registry_->FindWorkersForModel(request->model());
        // 轮到队头时已经没有健康 worker：立刻失败，不把 queue_timeout 耗完。
        if (workers.empty())
          return fail(grpc::StatusCode::UNAVAILABLE, "workers became unavailable");
        selected = scheduler_->SelectWorker(*request, workers);
        const auto key = request->model() + "\n" + request->session_id();
        auto hint = affinity_.find(key);
        // 十分钟内的亲和提示才有效。只有那个 worker 还没满，
        // 且它的在途数不超过「当前最优 + 1」时才改选它。
        // +1 是为了 KV cache 复用让一步负载偏差，而不是把请求钉死在一个已经很忙的节点上。
        if (selected && options_.session_affinity && !request->session_id().empty() &&
            hint != affinity_.end() &&
            Clock::now() - hint->second.touched < std::chrono::minutes(10)) {
          for (const auto& worker : workers) {
            if (worker.worker_id() == hint->second.worker_id &&
                (!worker.max_concurrent_requests() ||
                 worker.running_requests() < worker.max_concurrent_requests()) &&
                worker.running_requests() <= selected->running_requests() + 1) {
              selected = worker;
              metrics.Increment("inference_affinity_selections_total");
              break;
            }
          }
        }
        if (selected && registry_->ChangeRunning(selected->worker_id(), 1)) {
          waiting_.pop_front();
          metrics.Set("inference_requests_waiting", waiting_.size());
          state->downstream = std::make_shared<grpc::ClientContext>();
          state->downstream->set_deadline(context->deadline());
          if (options_.session_affinity && !request->session_id().empty()) {
            // 满 10000 整表清空，不是 LRU。清空后的一小段时间会话会失去亲和。
            // 占坑成功就记，不等这条流跑完。失败的请求也会把会话钉在这台 worker 上。
            if (affinity_.size() >= 10000) affinity_.clear();
            affinity_[key] = {selected->worker_id(), Clock::now()};
          }
          capacity_changed_.notify_all();
          break;
        }
        // 一台都选不中（都满了）或占坑失败：留在队头继续等。
        // 不要 pop。pop 会把这次失败让给后面的请求，FIFO 就破了。
        // 空列表已经在上面返回，这里不是「没有 worker」。
        selected.reset();
      }
      // 10ms 是满载时的轮询上限。Cancel 也会 notify，所以取消不用等满这一拍。
      capacity_changed_.wait_for(lock, std::chrono::milliseconds(10));
    }
  }
  metrics.Observe("inference_queue_wait_ms", ElapsedMs(start));
  // 成功、取消、DATA_LOSS 都要还租约。还的时候持锁并唤醒还在排队的队头。
  // 能走到这里 selected 一定有值：占坑失败的路径在锁里 continue，不会出来。
  ScopeExit release_worker([&] {
    std::lock_guard<std::mutex> lock(mutex_);
    registry_->ChangeRunning(selected->worker_id(), -1);
    capacity_changed_.notify_all();
  });
  auto downstream = state->downstream;
  std::cerr << "inference selected request_id=" << request->request_id()
            << " worker_id=" << selected->worker_id() << '\n';
  auto stub = InferenceWorker::NewStub(grpc::CreateChannel(
      selected->endpoint(), grpc::InsecureChannelCredentials()));
  // 调用方取消不会自动取消这条下游 RPC。10ms 轮询后 TryCancel，
  // 和 worker 里打断 backend 的监视线程是同一拍。
  std::atomic<bool> monitor_done{false};
  std::thread monitor([&] {
    while (!monitor_done.load()) {
      if (context->IsCancelled()) {
        downstream->TryCancel();
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });
  ScopeExit stop_monitor([&] {
    monitor_done.store(true);
    monitor.join();
  });
  auto reader = stub->Generate(downstream.get(), *request);
  bool first_token = false;
  bool terminal = false;
  uint64_t expected_sequence = 0;
  auto last_chunk = start;
  GenerateChunk chunk;
  while (reader->Read(&chunk)) {
    if (context->IsCancelled()) {
      downstream->TryCancel();
      break;
    }
    // 序号必须从 0 连续，终态块之后再来数据就是串流了。
    // 这种错误用 DATA_LOSS，Bridge 不会把半截流当成成功回答。
    if (chunk.request_id() != request->request_id() ||
        chunk.sequence() != expected_sequence++ || terminal) {
      downstream->TryCancel();
      return fail(grpc::StatusCode::DATA_LOSS, "invalid worker stream order");
    }
    if (!first_token && !chunk.text_delta().empty()) {
      first_token = true;
      metrics.Observe("inference_ttft_ms", ElapsedMs(start));
    } else if (!chunk.text_delta().empty()) {
      metrics.Observe("inference_chunk_interval_ms", ElapsedMs(last_chunk));
    }
    if (!chunk.text_delta().empty()) last_chunk = Clock::now();
    terminal = chunk.finished();
    // worker_id 以网关选中的为准，不转发 worker 自己填的值。
    chunk.set_worker_id(selected->worker_id());
    if (terminal) {
      metrics.Increment("inference_prompt_tokens_total", chunk.prompt_tokens());
      metrics.Increment("inference_completion_tokens_total", chunk.completion_tokens());
      metrics.Increment("inference_cached_prompt_tokens_total", chunk.cached_prompt_tokens());
      if (chunk.runtime_prefill_ms() > 0)
        metrics.Observe("inference_runtime_prefill_ms", chunk.runtime_prefill_ms());
      if (chunk.runtime_decode_ms() > 0)
        metrics.Observe("inference_runtime_decode_ms", chunk.runtime_decode_ms());
    }
    if (!writer->Write(chunk)) {
      downstream->TryCancel();
      break;
    }
  }
  grpc::Status status = reader->Finish();
  if (context->IsCancelled() || status.error_code() == grpc::StatusCode::CANCELLED)
    return fail(grpc::StatusCode::CANCELLED, "generation cancelled");
  if (!status.ok()) {
    std::cerr << "inference worker failed request_id=" << request->request_id()
              << " worker_id=" << selected->worker_id()
              << " error=" << status.error_message() << '\n';
    return fail(status.error_code(), status.error_message());
  }
  // worker 正常断开但没发终态块，同样是 DATA_LOSS。
  // 半截 token 不能被上游写成一条完整回复。
  if (!terminal) return fail(grpc::StatusCode::DATA_LOSS, "worker stream ended without terminal chunk");
  return grpc::Status::OK;
}

grpc::Status GatewayService::Cancel(grpc::ServerContext*,
    const CancelRequest* request, CancelReply* reply) {
  // 找不到也返回 OK，cancelled 保持 false。不是 NOT_FOUND。
  // 排队中的请求还没有 downstream，只置 cancelled，队头循环自己退出。
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = active_.find(request->request_id());
  if (it != active_.end()) {
    it->second->cancelled = true;
    if (it->second->downstream) it->second->downstream->TryCancel();
    capacity_changed_.notify_all();
    reply->set_cancelled(true);
  }
  return grpc::Status::OK;
}

grpc::Status GatewayService::RegisterWorker(grpc::ServerContext*,
    const RegisterWorkerRequest* request, RegisterWorkerReply* reply) {
  // gRPC 状态始终是 OK。拒绝写在 accepted 里。
  // worker 主循环把「RPC 失败」和「accepted=false」都当成需要重新 Register。
  reply->set_accepted(registry_->Register(request->worker()));
  capacity_changed_.notify_all();
  if (!reply->accepted()) reply->set_error("worker_id, endpoint and model_id required");
  return grpc::Status::OK;
}

grpc::Status GatewayService::Heartbeat(grpc::ServerContext*,
    const HeartbeatRequest* request, HeartbeatReply* reply) {
  reply->set_accepted(registry_->Heartbeat(request->worker()));
  capacity_changed_.notify_all();
  if (!reply->accepted()) reply->set_error("worker not registered or invalid heartbeat");
  return grpc::Status::OK;
}

}  // namespace sparkpush::inference
