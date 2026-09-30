#pragma once

#include <memory>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>

#include "gateway/scheduler.h"
#include "gateway/worker_registry.h"
#include "inference.grpc.pb.h"

namespace sparkpush::inference {

struct AdmissionOptions {
  size_t max_queue{64};
  std::chrono::milliseconds queue_timeout{30000};
  bool session_affinity{true};
};

class GatewayService final : public InferenceGateway::Service {
 public:
  GatewayService(WorkerRegistry* registry, const Scheduler* scheduler,
      AdmissionOptions options = {})
      : registry_(registry), scheduler_(scheduler), options_(options) {}
  grpc::Status Generate(grpc::ServerContext* context,
      const GenerateRequest* request,
      grpc::ServerWriter<GenerateChunk>* writer) override;
  grpc::Status Cancel(grpc::ServerContext*, const CancelRequest* request,
      CancelReply* reply) override;
  grpc::Status RegisterWorker(grpc::ServerContext*,
      const RegisterWorkerRequest* request,
      RegisterWorkerReply* reply) override;
  grpc::Status Heartbeat(grpc::ServerContext*, const HeartbeatRequest* request,
      HeartbeatReply* reply) override;

 private:
  WorkerRegistry* registry_;
  const Scheduler* scheduler_;
  const AdmissionOptions options_;
  struct RequestState {
    bool cancelled{false};
    std::shared_ptr<grpc::ClientContext> downstream;
  };
  std::mutex mutex_;
  std::condition_variable capacity_changed_;
  std::deque<std::string> waiting_;
  std::unordered_map<std::string, std::shared_ptr<RequestState>> active_;
  struct Affinity {
    std::string worker_id;
    std::chrono::steady_clock::time_point touched;
  };
  std::unordered_map<std::string, Affinity> affinity_;
};

}  // namespace sparkpush::inference
