#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace sparkpush {

// 封装 rdkafka 的简单消费者，负责订阅单个 topic 并将消息分发给回调。
class KafkaConsumer {
public:
    struct Options {
        // 是否开启自动提交；关闭时仅在业务回调成功后同步提交。
        bool enable_auto_commit{false};
        // 自动提交间隔（ms），仅在 enable_auto_commit 为真时生效
        int auto_commit_interval_ms{5000};
        // 拉取间隔超时；长轮询防止组再平衡异常
        int max_poll_interval_ms{300000};
        // 会话超时
        int session_timeout_ms{45000};
        // 起始位点策略：earliest/latest
        std::string auto_offset_reset{"earliest"};
        // 单条消息处理失败后的最大尝试次数（包含第一次）。
        int max_processing_attempts{3};
        // 业务处理重试的退避时间（ms）。
        int processing_retry_backoff_ms{100};
    };

    KafkaConsumer() = default;
    // 析构时会自动停止消费线程，释放资源。
    ~KafkaConsumer();

    // 初始化消费者，传入 broker、消费组、topic 及处理回调
    bool Init(const std::string& brokers,
              const std::string& group_id,
              const std::string& topic,
              std::function<bool(const std::string&, const std::string&)> callback,
              const Options& options);

    // 启动消费线程
    void Start();
    // 停止消费并回收资源
    void Stop();

private:
    // 主消费循环，阻塞拉取消息
    void Loop();
    // 根据消息错误码分发处理或记录错误
    void HandleMessage(RdKafka::Message* message);

    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    std::string topic_;
    std::function<bool(const std::string&, const std::string&)> callback_;
    Options options_;

    std::atomic<bool> running_{false};
    std::thread thread_;
};

}  // namespace sparkpush

