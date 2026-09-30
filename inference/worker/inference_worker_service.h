#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "inference.grpc.pb.h"
#include "worker/generation_backend.h"
#include "worker/gpu_memory_probe.h"

namespace sparkpush::inference {

class InferenceWorkerService final : public InferenceWorker::Service {
 public:
  InferenceWorkerService(WorkerInfo info, std::unique_ptr<GenerationBackend> backend);
  grpc::Status Generate(grpc::ServerContext* context,
      const GenerateRequest* request,
      grpc::ServerWriter<GenerateChunk>* writer) override;
  grpc::Status Cancel(grpc::ServerContext*, const CancelRequest* request,
      CancelReply* reply) override;
  grpc::Status GetStatus(grpc::ServerContext*, const GetWorkerStatusRequest*,
      WorkerInfo* reply) override;

 private:
  WorkerInfo info_;
  std::unique_ptr<GenerationBackend> backend_;
  GpuMemoryProbe memory_probe_;
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<std::atomic<bool>>> active_;
};

}  // namespace sparkpush::inference
