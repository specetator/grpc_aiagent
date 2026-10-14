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

// 准入参数。队列是全局 FIFO：只有队头能选 worker，后面的请求在锁上等待。
// 选中之后的 RPC 彼此独立，不再占着这把锁。
// session_affinity 让同一会话尽量回到上次的 worker，好复用那边的 KV cache。
struct AdmissionOptions {
  size_t max_queue{64};
  std::chrono::milliseconds queue_timeout{30000};
  bool session_affinity{true};
};

// 不加载模型。负责准入、选 worker，再把 worker 的流原样转给调用方。
//
// waiting_ 是还没选中 worker 的 request_id，只有队头有资格选。
// active_ 同时包含排队中和已经在跑的请求，Cancel 靠它找到状态。
// downstream 在选中 worker 之后才创建。排队阶段取消只置 cancelled，
// 没有下游可以 TryCancel。
// affinity_ 的 key 是 model + "\n" + session_id，十分钟有效，见 .cpp。
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
