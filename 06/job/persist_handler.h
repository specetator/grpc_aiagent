#pragma once

#include "config.h"
#include "kafka_consumer.h"
#include "mysql_pool.h"
#include "thread_pool.h"
#include "spark_push.grpc.pb.h"

#include <memory>
#include <string>

namespace sparkpush {

// 异步持久化处理器：从 Kafka 消费消息并落盘到 MySQL
// 架构设计：
//   与 PushHandler 共享同一 Kafka topic（spark_push），但使用不同的消费组
//   推送消费组：spark_push_push_group    → 负责实时推送到 Comet
//   持久化消费组：spark_push_persist_group → 负责异步写入 MySQL
//   两个消费组独立消费，互不影响（Kafka 消费组机制保证）
// 可靠性：带指数退避重试 + 死信日志（[DLQ]）
class PersistHandler {
 public:
  explicit PersistHandler(const Config& cfg);
  bool Init();
  void Start();
  void Stop();

 private:
  void HandleMessage(const std::string& key, const std::string& value);
  void PersistMessage(const PushToCometRequest& req);

  Config cfg_;
  KafkaConsumer persist_consumer_;
  ThreadPool persist_pool_;
  std::unique_ptr<MySqlConnectionPool> mysql_pool_;
};

}  // namespace sparkpush
