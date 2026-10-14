#include "worker/inference_worker_service.h"

#include <chrono>
#include <iostream>
#include <thread>

namespace sparkpush::inference {
namespace {
int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}
}  // namespace

InferenceWorkerService::InferenceWorkerService(WorkerInfo info,
    std::unique_ptr<GenerationBackend> backend)
    : info_(std::move(info)), backend_(std::move(backend)), memory_probe_(info_) {}

grpc::Status InferenceWorkerService::Generate(grpc::ServerContext* context,
    const GenerateRequest* request, grpc::ServerWriter<GenerateChunk>* writer) {
  // model 必须是本进程的 model_id。网关选 worker 时已经滤过，
  // 直接打到这台进程的 RPC 仍要拒，避免用别的模型名跑这次生成。
  // 空请求条件和网关相同：只看 prompt / messages，不看 chat_turns。
  if (request->request_id().empty() || request->model() != info_.model_id() ||
      (request->prompt().empty() && request->messages().empty()))
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid generation request"};
  auto cancelled = std::make_shared<std::atomic<bool>>(false);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    // 先占位再看容量，所以 size 已经包含本请求。
    // 同一 request_id 并发进来时，输的一方直接 ALREADY_EXISTS，不会跑两次生成。
    if (!active_.emplace(request->request_id(), cancelled).second)
      return {grpc::StatusCode::ALREADY_EXISTS, "request_id already active"};
    // max_concurrent 为 0 时跳过（调度器也把 0 当不设上限）。
    // 命令行不允许 0，直接构造本服务的测试仍会走到这里。
    // 用 > 而不是 >=：上面的 emplace 已经把本请求算进 size。
    if (info_.max_concurrent_requests() && active_.size() > info_.max_concurrent_requests()) {
      active_.erase(request->request_id());
      return {grpc::StatusCode::RESOURCE_EXHAUSTED, "worker capacity exhausted"};
    }
  }
  std::cerr << "inference worker started request_id=" << request->request_id()
            << " worker_id=" << info_.worker_id() << '\n';
  std::atomic<bool> monitor_done{false};
  // 生成阻塞在 backend 里，gRPC 取消不会自动打断它。
  // 监视线程每 10ms 看一次 context，置上 cancelled 让 backend 自己停。
  std::thread monitor([&] {
    while (!monitor_done.load()) {
      if (context->IsCancelled()) {
        cancelled->store(true);
        break;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });
  bool completed = false;
  bool backend_failed = false;
  try {
    completed = backend_->Generate(*request, *cancelled,
        [&](const GenerateChunk& chunk) {
          if (context->IsCancelled() || cancelled->load()) return false;
          return writer->Write(chunk);
        });
  } catch (const std::exception& error) {
    std::cerr << "inference worker failed request_id=" << request->request_id()
              << " worker_id=" << info_.worker_id()
              << " error=" << error.what() << '\n';
    backend_failed = true;
  } catch (...) {
    backend_failed = true;
  }
  monitor_done.store(true);
  monitor.join();
  // 先停监视线程再从表里删。删了之后 Cancel 就找不到这次请求了。
  {
    std::lock_guard<std::mutex> lock(mutex_);
    active_.erase(request->request_id());
  }
  if (backend_failed)
    return {grpc::StatusCode::INTERNAL, "generation backend failed"};
  if (!completed || context->IsCancelled() || cancelled->load()) {
    std::cerr << "inference worker cancelled request_id=" << request->request_id()
              << " worker_id=" << info_.worker_id() << '\n';
    return {grpc::StatusCode::CANCELLED, "generation cancelled"};
  }
  std::cerr << "inference worker completed request_id=" << request->request_id()
            << " worker_id=" << info_.worker_id() << '\n';
  return grpc::Status::OK;
}

grpc::Status InferenceWorkerService::Cancel(grpc::ServerContext*,
    const CancelRequest* request, CancelReply* reply) {
  // 不在表里也返回 OK，cancelled 保持默认 false。
  // 只置标志。不要在这把锁里调 backend，生成线程自己看到标志后停。
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = active_.find(request->request_id());
  if (it != active_.end()) {
    it->second->store(true);
    reply->set_cancelled(true);
  }
  return grpc::Status::OK;
}

grpc::Status InferenceWorkerService::GetStatus(grpc::ServerContext*,
    const GetWorkerStatusRequest*, WorkerInfo* reply) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    *reply = info_;
    // running_requests 用本进程 active_ 的大小，不用 info_ 里的旧值。
    // 这是心跳带给网关的准入数。llama 自己的队列写在 waiting_requests / runtime_*，
    // GetRuntimeStatus 不要覆盖这里的 running_requests。
    reply->set_running_requests(active_.size());
  }
  // 这两步在锁外。探针会 dlopen / 切 CUDA context，不能关在活跃请求锁里。
  backend_->GetRuntimeStatus(reply);
  memory_probe_.Sample(reply);
  reply->set_timestamp_ms(NowMs());
  return grpc::Status::OK;
}

}  // namespace sparkpush::inference
