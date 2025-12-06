#pragma once

#include "config.h"
#include "kafka_consumer.h"
#include "thread_pool.h"
#include "spark_push.grpc.pb.h"

#include <grpcpp/grpcpp.h>

#include <mutex>
#include <string>
#include <unordered_map>

namespace sparkpush {

    // JobRunner 负责从 Kafka 拉取任务并调用 Comet 进行推送的核心调度器。
    class JobRunner {
    public:
        // 使用配置构造 JobRunner，初始化线程池等基础资源。
        explicit JobRunner(const Config& cfg);

        // 初始化 Kafka 消费者、解析 Comet 目标并启动线程池。
        bool Init();

        // 启动两个 Kafka 消费者，进入拉取消息状态。
        void Start();

        // 停止消费者与线程池，确保退出时资源释放。
        void Stop();

    private:
        // 普通推送消息的回调处理：解析并提交到 RPC 线程池。
        void HandleMessage(const std::string& key, const std::string& value);

        // 广播任务的回调处理：解析广播请求并下发到各 Comet。
        void HandleBroadcastTask(const std::string& key, const std::string& value);

        // 将解析好的 PushToCometRequest 发送到指定 Comet。
        void ProcessPushRequest(const PushToCometRequest& req);

        // 根据 comet_id 获取或创建对应的 gRPC Stub。
        CometService::Stub* GetStub(const std::string& comet_id);

        // 解析配置中的 comet_targets，填充 id -> address 映射。
        void ParseCometTargets();

        Config cfg_;
        KafkaConsumer consumer_;
        KafkaConsumer broadcast_consumer_;
        std::unordered_map<std::string, std::string> comet_addrs_;
        std::unordered_map<std::string, std::unique_ptr<CometService::Stub>> comet_stubs_;
        std::mutex stub_mu_;
        ThreadPool rpc_pool_;
    };

}  // namespace sparkpush



