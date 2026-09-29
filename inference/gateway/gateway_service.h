#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "gateway/scheduler.h"
#include "gateway/worker_registry.h"
#include "inference.grpc.pb.h"

namespace sparkpush::inference {

class GatewayService final : public InferenceGateway::Service {
 public:
  GatewayService(WorkerRegistry* registry, const Scheduler* scheduler)
      : registry_(registry), scheduler_(scheduler) {}
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
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<grpc::ClientContext>> active_;
};

}  // namespace sparkpush::inference
