#include "kafka_consumer.h"

#include "logging.h"

#include <librdkafka/rdkafkacpp.h>
#include <vector>

namespace sparkpush {

KafkaConsumer::~KafkaConsumer() {
    Stop();
}

bool KafkaConsumer::Init(const std::string& brokers,
                         const std::string& group_id,
                         const std::string& topic,
                         std::function<void(const std::string&, const std::string&)> callback,
                         const Options& options) {
    topic_ = topic;
    callback_ = std::move(callback);
    options_ = options;

    std::string errstr;
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

    std::vector<std::string> topics = {topic_};
    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        LOG_ERROR << "KafkaConsumer subscribe failed: " << RdKafka::err2str(err);
        return false;
    }
    return true;
}

void KafkaConsumer::Start() {
    if (running_) {
        return;
    }
    running_ = true;
    thread_ = std::thread(&KafkaConsumer::Loop, this);
}

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

void KafkaConsumer::Loop() {
    while (running_) {
        std::unique_ptr<RdKafka::Message> msg(consumer_->consume(1000));
        if (!msg) {
            continue;
        }
        HandleMessage(msg.get());
    }
}

void KafkaConsumer::HandleMessage(RdKafka::Message* message) {
    switch (message->err()) {
        case RdKafka::ERR_NO_ERROR: {
            std::string key;
            if (message->key()) {
                key = *message->key();
            }
            std::string value(static_cast<const char*>(message->payload()),
                              message->len());
            if (callback_) {
                callback_(key, value);
            }
            if (!options_.enable_auto_commit && consumer_) {
                consumer_->commitAsync(message);
            }
            break;
        }
        case RdKafka::ERR__TIMED_OUT:
            break;
        default:
            LOG_ERROR << "Kafka consume error: " << message->errstr();
            break;
    }
}

}  // namespace sparkpush


