#pragma once

#include "config.h"
#include "kafka_consumer.h"
#include "thread_pool.h"
#include "spark_push.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

namespace sparkpush {

// 推送处理器：Job 服务的核心组件
// 职责：从 Kafka 消费下行消息，通过 gRPC 推送到目标 Comet 节点
// 支持两种推送模式：
//   1. 传统模式（默认）：线程池 + 同步 gRPC unary 调用，带指数退避重试
//   2. 流模式（可选）：每个 Comet 一条 gRPC 客户端流（PushStream），持续推送
// 可靠性保障：重试 3 次后记录死信日志（[DLQ]），便于离线排查和补偿
class PushHandler {
 public:
  explicit PushHandler(const Config& cfg);
  ~PushHandler();
  bool Init();
  void Start();
  void Stop();

 private:
  void HandleMessage(const std::string& key, const std::string& value);
  void HandleBroadcastTask(const std::string& key, const std::string& value);
  void ProcessPushRequest(const PushToCometRequest& req);
  CometService::Stub* GetStub(const std::string& comet_id);
  void ParseCometTargets();
  
  Config cfg_;
  KafkaConsumer push_consumer_;
  KafkaConsumer broadcast_consumer_;
  std::unordered_map<std::string, std::string> comet_addrs_;
  std::unordered_map<std::string, std::unique_ptr<CometService::Stub>> comet_stubs_;
  std::mutex stub_mu_;
  ThreadPool rpc_pool_;
  
  // 流模式状态（每个Comet一个流）
  bool use_stream_;
  std::atomic<bool> stream_running_{false};
  
  struct StreamState {
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<CometService::Stub> stub;
    std::unique_ptr<grpc::ClientContext> ctx;
    std::unique_ptr<grpc::ClientWriter<PushToCometRequest>> writer;
    PushStreamReply response;  // gRPC响应（流结束时填充）
    std::thread writer_thread;
    std::queue<PushToCometRequest> send_queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
  };
  std::unordered_map<std::string, std::unique_ptr<StreamState>> streams_;

  // 流模式相关
  void InitStreams();
  void StreamWriterLoop(const std::string& comet_id);
  void SendToStream(const std::string& comet_id, PushToCometRequest req);
  bool ReconnectStream(const std::string& comet_id, StreamState* state);
};

}  // namespace sparkpush
