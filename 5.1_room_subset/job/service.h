#pragma once

#include <grpcpp/grpcpp.h>

#include <mutex>
#include <string>
#include <unordered_map>

#include "config.h"
#include "kafka_consumer.h"
#include "spark_push.grpc.pb.h"
#include "thread_pool.h"

namespace sparkpush {

// JobRunner 负责从 Kafka 拉取任务并调用 Comet 进行推送的核心调度器。
// 主要功能：
// 1. 从 Kafka 消费推送任务消息
// 2. 解析消息并根据目标 Comet 节点进行分发
// 3. 通过 gRPC 调用 Comet 服务完成消息推送
class JobRunner {
   public:
    // 构造函数，传入配置对象
    explicit JobRunner(const Config& cfg);

    // 初始化 JobRunner，包括解析 Comet 目标、初始化 Kafka 消费者和线程池
    // 返回值：成功返回 true，失败返回 false
    bool Init();

    // 启动 JobRunner，开始从 Kafka 消费消息
    void Start();

    // 停止 JobRunner，停止 Kafka 消费者和 RPC 线程池
    void Stop();

   private:
    // 处理从 Kafka 接收到的消息
    // key: Kafka 消息的键（当前未使用）
    // value: Kafka 消息的值，包含序列化后的 PushToCometRequest
    void HandleMessage(const std::string& key, const std::string& value);

    // 处理推送请求，将消息分发到指定的或所有 Comet 节点
    // req: 推送请求对象，包含消息内容和目标 Comet 节点信息
    void ProcessPushRequest(const PushToCometRequest& req);

    // 获取指定 Comet 节点的 gRPC Stub 对象
    // comet_id: Comet 节点的唯一标识
    // 返回值：成功返回 Stub 指针，失败返回 nullptr
    CometService::Stub* GetStub(const std::string& comet_id);

    // 解析配置中的 Comet 目标列表
    // 将 comet_targets 配置字符串解析为 comet_addrs_ 映射表
    void ParseCometTargets();

    Config cfg_;  // 配置对象，包含 Kafka、Comet 等所有配置项
    KafkaConsumer consumer_;  // Kafka 消费者，用于接收推送任务消息
    std::unordered_map<std::string, std::string>
        comet_addrs_;  // Comet ID 到地址的映射表
    std::unordered_map<std::string, std::unique_ptr<CometService::Stub>>
        comet_stubs_;  // Comet ID 到 gRPC Stub 的映射表，用于缓存连接
    std::mutex stub_mu_;  // 保护 comet_stubs_ 的互斥锁，确保线程安全
    ThreadPool rpc_pool_;  // RPC 工作线程池，用于异步处理推送请求
};

}  // namespace sparkpush
