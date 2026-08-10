#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace sparkpush {

// 简单封装 rdkafka 生产者，提供初始化与发送接口。
class KafkaProducer {
public:
    KafkaProducer();
    // 析构时会尝试 flush，减少未发送消息。
    ~KafkaProducer();

    // 初始化生产者并绑定目标 topic
    bool Init(const std::string& brokers, const std::string& topic);
    // 发送单条消息，key 可选用于分区（异步，不逐条 flush）
    bool Send(const std::string& key, const std::string& value);
    // 发送并等待 librdkafka delivery report，用于持久化 topic 的 outbox 语义。
    bool SendAndWait(const std::string& key, const std::string& value,
                     int timeout_ms);

private:
    struct DeliveryState {
        std::mutex mutex;
        std::condition_variable cv;
        bool done{false};
        bool ok{false};
    };

    class DeliveryCallback;
    void PollLoop();

    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<RdKafka::Topic> topic_;
    std::string topic_name_;
    std::atomic<bool> poll_running_{false};
    std::thread poll_thread_;
    std::unique_ptr<DeliveryCallback> delivery_callback_;
    std::mutex delivery_mutex_;
    std::unordered_map<void*, std::shared_ptr<DeliveryState>> deliveries_;
};

}  // namespace sparkpush
