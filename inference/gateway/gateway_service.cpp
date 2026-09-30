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
  if (request->request_id().empty() || request->model().empty() ||
      (request->prompt().empty() && request->messages().empty()))
    return fail(grpc::StatusCode::INVALID_ARGUMENT, "request_id, model and prompt/messages required");
  auto state = std::make_shared<RequestState>();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_.count(request->request_id()))
      return fail(grpc::StatusCode::ALREADY_EXISTS, "request_id already active");
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
    std::unique_lock<std::mutex> lock(mutex_);
    while (!selected) {
      if (context->IsCancelled() || state->cancelled)
        return fail(grpc::StatusCode::CANCELLED, "queued generation cancelled");
      if (std::chrono::system_clock::now() >= context->deadline() ||
          Clock::now() >= queue_deadline) {
        metrics.Increment("inference_queue_timeouts_total");
        return fail(grpc::StatusCode::DEADLINE_EXCEEDED, "inference queue timeout");
      }
      if (waiting_.front() == request->request_id()) {
        auto workers = registry_->FindWorkersForModel(request->model());
        if (workers.empty())
          return fail(grpc::StatusCode::UNAVAILABLE, "workers became unavailable");
        selected = scheduler_->SelectWorker(*request, workers);
        const auto key = request->model() + "\n" + request->session_id();
        auto hint = affinity_.find(key);
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
            if (affinity_.size() >= 10000) affinity_.clear();
            affinity_[key] = {selected->worker_id(), Clock::now()};
          }
          capacity_changed_.notify_all();
          break;
        }
        selected.reset();
      }
      capacity_changed_.wait_for(lock, std::chrono::milliseconds(10));
    }
  }
  metrics.Observe("inference_queue_wait_ms", ElapsedMs(start));
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
  if (!terminal) return fail(grpc::StatusCode::DATA_LOSS, "worker stream ended without terminal chunk");
  return grpc::Status::OK;
}

grpc::Status GatewayService::Cancel(grpc::ServerContext*,
    const CancelRequest* request, CancelReply* reply) {
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
