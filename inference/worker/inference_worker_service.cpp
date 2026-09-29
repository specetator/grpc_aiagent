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
    : info_(std::move(info)), backend_(std::move(backend)) {}

grpc::Status InferenceWorkerService::Generate(grpc::ServerContext* context,
    const GenerateRequest* request, grpc::ServerWriter<GenerateChunk>* writer) {
  if (request->request_id().empty() || request->model() != info_.model_id() ||
      (request->prompt().empty() && request->messages().empty()))
    return {grpc::StatusCode::INVALID_ARGUMENT, "invalid generation request"};
  auto cancelled = std::make_shared<std::atomic<bool>>(false);
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!active_.emplace(request->request_id(), cancelled).second)
      return {grpc::StatusCode::ALREADY_EXISTS, "request_id already active"};
  }
  std::cerr << "inference worker started request_id=" << request->request_id()
            << " worker_id=" << info_.worker_id() << '\n';
  std::atomic<bool> monitor_done{false};
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
  std::lock_guard<std::mutex> lock(mutex_);
  *reply = info_;
  reply->set_running_requests(active_.size());
  reply->set_timestamp_ms(NowMs());
  return grpc::Status::OK;
}

}  // namespace sparkpush::inference
