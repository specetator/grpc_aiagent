#include "kafka_consumer.h"

#include "logging.h"
#include "metrics.h"

#include <librdkafka/rdkafkacpp.h>
#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>
#include <nlohmann/json.hpp>

namespace sparkpush {
namespace {
// Kafka records may contain invalid UTF-8 or binary data; retain exact bytes.
std::string Hex(const std::string& bytes) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        result += digits[c >> 4];
        result += digits[c & 15];
    }
    return result;
}
}

// 析构时主动停止消费，确保后台线程退出。
KafkaConsumer::~KafkaConsumer() {
    Stop();
}

// 初始化消费者，配置集群、消费组与订阅主题，并注册消息回调。
bool KafkaConsumer::Init(const std::string& brokers,
                         const std::string& group_id,
                         const std::string& topic,
                         std::function<bool(const std::string&, const std::string&)> callback,
                         const Options& options) {
    if (thread_.joinable() || consumer_) return false;
    dead_letter_producer_.reset();
    failed_ = false;
    topic_ = topic;
    group_id_ = group_id;
    callback_ = std::move(callback);
    options_ = options;
    if (!options_.dead_letter_topic.empty()) {
        if (options_.enable_auto_commit || options_.dead_letter_timeout_ms <= 0 ||
            options_.dead_letter_topic == topic_) return false;
        dead_letter_producer_ = std::make_unique<KafkaProducer>();
        if (!dead_letter_producer_->Init(brokers, options_.dead_letter_topic))
            return false;
    }

    std::string errstr;
    // 创建全局配置
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    bool config_ok = true;
    auto set_config = [&](const std::string& key, const std::string& value) {
        if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
            config_ok = false;
            LOG_ERROR << "KafkaConsumer rejected config key=" << key;
        }
    };
    set_config("bootstrap.servers", brokers);
    set_config("group.id", group_id);
    set_config("enable.partition.eof", "false");
    set_config("auto.offset.reset", options_.auto_offset_reset);
    set_config("enable.auto.commit", options_.enable_auto_commit ? "true" : "false");
    if (dead_letter_producer_)
        set_config("enable.auto.offset.store", "false");
    set_config("auto.commit.interval.ms", std::to_string(options_.auto_commit_interval_ms));
    set_config("max.poll.interval.ms", std::to_string(options_.max_poll_interval_ms));
    set_config("session.timeout.ms", std::to_string(options_.session_timeout_ms));
    if (!config_ok) {
        delete conf;
        return false;
    }

    consumer_.reset(RdKafka::KafkaConsumer::create(conf, errstr));
    delete conf;
    if (!consumer_) {
        LOG_ERROR << "KafkaConsumer create failed: " << errstr;
        return false;
    }

    // 订阅目标 topic
    std::vector<std::string> topics = {topic_};
    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        LOG_ERROR << "KafkaConsumer subscribe failed: " << RdKafka::err2str(err);
        return false;
    }
    return true;
}

// 启动消费线程，避免重复启动。
void KafkaConsumer::Start() {
    if (running_ || thread_.joinable() || failed_ || !consumer_) {
        return;
    }
    running_ = true;
    thread_ = std::thread(&KafkaConsumer::Loop, this);
}

// 停止消费并释放底层 consumer 资源。
void KafkaConsumer::Stop() {
    running_ = false;
    if (thread_.joinable()) {
        thread_.join();
    }
    if (consumer_) {
        consumer_->close();
        consumer_.reset();
    }
}

// 阻塞拉取消息的主循环，使用超时便于及时退出。
void KafkaConsumer::Loop() {
    // 简单的阻塞拉取循环，使用 1 秒超时以便及时感知停止信号
    while (running_) {
        std::unique_ptr<RdKafka::Message> msg(consumer_->consume(1000));
        if (!msg) {
            continue;
        }
        HandleMessage(msg.get());
    }
    if (failed_) {
        // Release assignment promptly so a replacement can replay the record.
        consumer_->close();
        consumer_.reset();
    }
}

// 按错误码分类处理 Kafka 消息，确保业务回调后再提交位点。
void KafkaConsumer::HandleMessage(RdKafka::Message* message) {
    switch (message->err()) {
        case RdKafka::ERR_NO_ERROR: {
            const auto timestamp = message->timestamp();
            if (timestamp.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE &&
                timestamp.timestamp > 0) {
                const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                MetricsRegistry::Instance().Set(
                    "spark_push_kafka_lag_ms",
                    std::max<int64_t>(0, now_ms - timestamp.timestamp));
            }
            std::string key;
            if (message->key()) {
                key = *message->key();
            }
            std::string value;
            if (message->len())
                value.assign(static_cast<const char*>(message->payload()), message->len());
            bool handled = !callback_;
            const int attempts = std::max(1, options_.max_processing_attempts);
            for (int attempt = 1; callback_ && attempt <= attempts; ++attempt) {
                try {
                    handled = callback_(key, value);
                } catch (...) {
                    // Exception text can contain message bodies/credentials.
                    LOG_ERROR << "Kafka business callback threw topic=" << topic_;
                    handled = false;
                }
                if (handled) {
                    break;
                }
                if (attempt < attempts) {
                    LOG_WARN << "Kafka business callback failed, retrying topic="
                             << message->topic_name()
                             << " partition=" << message->partition()
                             << " offset=" << message->offset()
                             << " attempt=" << attempt << "/" << attempts;
                    std::this_thread::sleep_for(std::chrono::milliseconds(
                        std::max(0, options_.processing_retry_backoff_ms) * attempt));
                }
            }

            if (!handled) {
                LOG_ERROR << "Kafka processing exhausted topic="
                          << message->topic_name()
                          << " partition=" << message->partition()
                          << " offset=" << message->offset();
                if (dead_letter_producer_) {
                    try {
                        const auto id = nlohmann::json::array(
                            {group_id_, topic_, message->partition(), message->offset()}).dump();
                        nlohmann::json failure = {
                            {"schema", "sparkpush.dead_letter.v1"}, {"id", id},
                            {"consumer_group", group_id_}, {"topic", topic_},
                            {"partition", message->partition()}, {"offset", message->offset()},
                            {"timestamp_ms", timestamp.timestamp},
                            {"key_is_null", message->key() == nullptr},
                            {"payload_is_null", message->payload() == nullptr},
                            {"key_hex", Hex(key)}, {"payload_hex", Hex(value)},
                            {"attempts", attempts}, {"reason", "processing_exhausted"}};
                        if (options_.failure_recovery) {
                            auto recovery = options_.failure_recovery(key, value);
                            if (!recovery.topic.empty()) {
                                failure["recovery"] = {{"topic", recovery.topic},
                                    {"key_hex", Hex(recovery.key)},
                                    {"payload_hex", Hex(recovery.payload)}};
                            }
                        }
                        handled = dead_letter_producer_->SendAndWait(
                            id, failure.dump(), options_.dead_letter_timeout_ms);
                    } catch (...) {
                        handled = false;
                    }
                    if (!handled) {
                        failed_ = true;
                        running_ = false;
                        MetricsRegistry::Instance().Set(
                            "spark_push_kafka_consumer_failed_" + topic_, 1);
                        LOG_ERROR << "Kafka consumer halted: DLQ unconfirmed topic=" << topic_;
                        return;
                    }
                    MetricsRegistry::Instance().Increment(
                        "spark_push_kafka_dead_letters_total_" + topic_);
                }
            }

            // Durable mode requires business success or confirmed DLQ delivery.
            // Other consumers retain their existing best-effort failure policy.
            if (!options_.enable_auto_commit && consumer_) {
                RdKafka::ErrorCode commit_err = consumer_->commitSync(message);
                if (commit_err != RdKafka::ERR_NO_ERROR) {
                    LOG_ERROR << "Kafka offset commit failed: "
                              << RdKafka::err2str(commit_err);
                    if (dead_letter_producer_) {
                        failed_ = true;
                        running_ = false;
                        MetricsRegistry::Instance().Set(
                            "spark_push_kafka_consumer_failed_" + topic_, 1);
                    }
                }
            }
            break;
        }
        case RdKafka::ERR__TIMED_OUT:
            // 正常超时，无需日志
            break;
        default:
            LOG_ERROR << "Kafka consume error: " << message->errstr();
            break;
    }
}

}  // namespace sparkpush
