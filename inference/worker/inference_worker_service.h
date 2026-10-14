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

// 一个进程服务一个模型。Generate 把 backend 的阻塞调用包成 gRPC 流。
//
// active_ 的值是 shared_ptr<atomic<bool>>，不是裸标志：
// Cancel RPC 和生成线程要同时看见同一次取消，锁只保护这张表。
// 生成进行中不能握着 mutex_，否则 Cancel 和 GetStatus 会被模型推理堵住。
// GetRuntimeStatus 和显存采样在锁外，见 .cpp。
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
