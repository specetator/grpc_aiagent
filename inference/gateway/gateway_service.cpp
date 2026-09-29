#include "gateway/gateway_service.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <iostream>
#include <thread>

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
  auto workers = registry_->FindWorkersForModel(request->model());
  auto selected = scheduler_->SelectWorker(*request, workers);
  if (!selected) {
    metrics.Increment("inference_scheduler_no_worker_total");
    return fail(grpc::StatusCode::UNAVAILABLE, "no healthy worker for model");
  }
  if (!registry_->ChangeRunning(selected->worker_id(), 1))
    return fail(grpc::StatusCode::UNAVAILABLE, "selected worker disappeared");
  ScopeExit release_worker([&] { registry_->ChangeRunning(selected->worker_id(), -1); });
  metrics.Set("inference_worker_running_requests",
      selected->running_requests() + 1);
  auto downstream = std::make_shared<grpc::ClientContext>();
  downstream->set_deadline(context->deadline());
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!active_.emplace(request->request_id(), downstream).second)
      return fail(grpc::StatusCode::ALREADY_EXISTS, "request_id already active");
  }
  ScopeExit remove_request([&] {
    std::lock_guard<std::mutex> lock(mutex_);
    active_.erase(request->request_id());
  });
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
  GenerateChunk chunk;
  while (reader->Read(&chunk)) {
    if (context->IsCancelled()) {
      downstream->TryCancel();
      break;
    }
    if (chunk.request_id() != request->request_id()) {
      downstream->TryCancel();
      return fail(grpc::StatusCode::DATA_LOSS, "worker request_id mismatch");
    }
    if (!first_token && !chunk.text_delta().empty()) {
      first_token = true;
      metrics.Observe("inference_ttft_ms", ElapsedMs(start));
    }
    terminal = chunk.finished();
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
    it->second->TryCancel();
    reply->set_cancelled(true);
  }
  return grpc::Status::OK;
}

grpc::Status GatewayService::RegisterWorker(grpc::ServerContext*,
    const RegisterWorkerRequest* request, RegisterWorkerReply* reply) {
  reply->set_accepted(registry_->Register(request->worker()));
  if (!reply->accepted()) reply->set_error("worker_id, endpoint and model_id required");
  return grpc::Status::OK;
}

grpc::Status GatewayService::Heartbeat(grpc::ServerContext*,
    const HeartbeatRequest* request, HeartbeatReply* reply) {
  reply->set_accepted(registry_->Heartbeat(request->worker()));
  if (!reply->accepted()) reply->set_error("worker not registered or invalid heartbeat");
  return grpc::Status::OK;
}

}  // namespace sparkpush::inference
