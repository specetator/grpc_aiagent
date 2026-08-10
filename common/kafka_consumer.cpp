#include "kafka_consumer.h"

#include "logging.h"
#include "metrics.h"

#include <librdkafka/rdkafkacpp.h>
#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>

namespace sparkpush {

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
    topic_ = topic;
    callback_ = std::move(callback);
    options_ = options;

    std::string errstr;
    // 创建全局配置
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    conf->set("bootstrap.servers", brokers, errstr);
    conf->set("group.id", group_id, errstr);
    conf->set("enable.partition.eof", "false", errstr);
    conf->set("auto.offset.reset", options_.auto_offset_reset, errstr);
    conf->set("enable.auto.commit",
              options_.enable_auto_commit ? "true" : "false",
              errstr);
    conf->set("auto.commit.interval.ms",
              std::to_string(options_.auto_commit_interval_ms),
              errstr);
    conf->set("max.poll.interval.ms",
              std::to_string(options_.max_poll_interval_ms),
              errstr);
    conf->set("session.timeout.ms",
              std::to_string(options_.session_timeout_ms),
              errstr);

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
    if (running_) {
        return;
    }
    running_ = true;
    thread_ = std::thread(&KafkaConsumer::Loop, this);
}

// 停止消费并释放底层 consumer 资源。
void KafkaConsumer::Stop() {
    if (!running_) {
        return;
    }
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
            std::string value(static_cast<const char*>(message->payload()),
                              message->len());
            bool handled = !callback_;
            const int attempts = std::max(1, options_.max_processing_attempts);
            for (int attempt = 1; callback_ && attempt <= attempts; ++attempt) {
                handled = callback_(key, value);
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
                // 当前项目没有独立 DLQ topic，至少保留不含正文的审计日志，
                // 避免毒消息永久阻塞整个分区。
                LOG_ERROR << "[DLQ][KafkaConsumer] processing exhausted topic="
                          << message->topic_name()
                          << " partition=" << message->partition()
                          << " offset=" << message->offset();
            }

            // 回调完成（成功或已记录死信）后再同步提交，避免任务刚入线程池就前移位点。
            if (!options_.enable_auto_commit && consumer_) {
                RdKafka::ErrorCode commit_err = consumer_->commitSync(message);
                if (commit_err != RdKafka::ERR_NO_ERROR) {
                    LOG_ERROR << "Kafka offset commit failed: "
                              << RdKafka::err2str(commit_err);
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
