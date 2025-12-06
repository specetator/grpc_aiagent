#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>

namespace sparkpush {

// 简单封装 rdkafka 生产者，提供初始化与发送接口。
class KafkaProducer {
public:
    KafkaProducer() = default;
    // 析构时会尝试 flush，减少未发送消息。
    ~KafkaProducer();

    // 初始化生产者并绑定目标 topic
    bool Init(const std::string& brokers, const std::string& topic);
    // 发送单条消息，key 可选用于分区
    bool Send(const std::string& key, const std::string& value);

private:
    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<RdKafka::Topic> topic_;
    std::string topic_name_;
};

}  // namespace sparkpush


