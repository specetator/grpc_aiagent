#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>

namespace sparkpush {

// 简单 Kafka 生产者封装，负责初始化和发送消息。
class KafkaProducer {
public:
    KafkaProducer() = default;
    ~KafkaProducer();

    // 初始化生产者，绑定 broker 和目标 topic。
    bool Init(const std::string& brokers, const std::string& topic);

    // 发送一条消息，key 用于分区。
    bool Send(const std::string& key, const std::string& value);

private:
    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<RdKafka::Topic> topic_;
    std::string topic_name_;
};

}  // namespace sparkpush


