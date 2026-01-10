#include "kafka_producer.h"

#include "logging.h"

#include <librdkafka/rdkafkacpp.h>

namespace sparkpush {

// 析构时尝试 flush，降低进程退出前的消息丢失概率。
KafkaProducer::~KafkaProducer() {
    if (producer_) {
        // 离开前尽量刷盘，减少消息丢失
        producer_->flush(3000);
    }
}

// 初始化生产者：配置可靠性参数、创建全局 conf，并绑定目标 topic。
bool KafkaProducer::Init(const std::string& brokers, const std::string& topic) {
    std::string errstr;
    topic_name_ = topic;

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    auto set_conf = [&](const std::string& key, const std::string& value) {
        if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
            LOG_ERROR << "KafkaProducer set " << key << " failed: " << errstr;
            return false;
        }
        return true;
    };

    // 兼容老版本 librdkafka 的配置键名差异（compression / keepalive）
    auto set_conf_any = [&](std::initializer_list<std::pair<std::string, std::string>> kvs) {
        for (const auto& kv : kvs) {
            if (set_conf(kv.first, kv.second)) {
                return true;
            }
        }
        return false;
    };
    // 同一键尝试多个候选值，取第一组成功的配置
    auto set_conf_with_values = [&](const std::string& key,
                                    std::initializer_list<std::string> values) {
        for (const auto& v : values) {
            if (set_conf(key, v)) {
                return true;
            }
        }
        return false;
    };

    // 基础 broker 和可靠性、性能参数
    if (!set_conf("bootstrap.servers", brokers)) {
        delete conf;
        return false;
    }
    set_conf("acks", "all");
    set_conf("enable.idempotence", "true");
    // compression.type 在新版本可用，老版本需使用 compression.codec
    // 如果 zstd 未编译进 librdkafka，则依次回退 lz4 -> snappy -> gzip -> none
    bool compression_ok =
        // set_conf_any({{"compression.type", "zstd"}, {"compression.codec", "zstd"}}) ||
        set_conf_any({{"compression.type", "lz4"}, {"compression.codec", "lz4"}}) ||
        set_conf_any({{"compression.type", "snappy"}, {"compression.codec", "snappy"}}) ||
        set_conf_any({{"compression.type", "gzip"}, {"compression.codec", "gzip"}});
    if (!compression_ok) {
        // 仍失败则显式关闭压缩，避免启动时刷屏错误
        set_conf_with_values("compression.type", {"none"});
        set_conf_with_values("compression.codec", {"none"});
        LOG_WARN << "KafkaProducer compression algorithms unavailable, fallback to none";
    }
    set_conf("linger.ms", "5");
    set_conf("batch.num.messages", "1000");
    set_conf("retries", "5");
    set_conf("retry.backoff.ms", "500");
    // socket.keepalive.enable 为新键，部分老版本使用 socket.keepalive
    set_conf_any({{"socket.keepalive.enable", "true"}, {"socket.keepalive", "true"}});
    set_conf("message.timeout.ms", "60000");
    set_conf("queue.buffering.max.messages", "200000");
    if (!topic_name_.empty()) {
        set_conf("client.id", "spark_push_producer_" + topic_name_);
    }

    producer_.reset(RdKafka::Producer::create(conf, errstr));
    delete conf;
    if (!producer_) {
        LOG_ERROR << "KafkaProducer set brokers failed: " << errstr;
        return false;
    }

    // 绑定 topic
    RdKafka::Conf* topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    topic_.reset(
        RdKafka::Topic::create(producer_.get(), topic_name_, topic_conf, errstr));
    delete topic_conf;
    if (!topic_) {
        LOG_ERROR << "KafkaProducer create topic failed: " << errstr;
        return false;
    }

    LOG_INFO << "KafkaProducer initialized topic=" << topic_name_;
    return true;
}

// 发送单条消息，失败时输出错误日志并返回 false。
bool KafkaProducer::Send(const std::string& key, const std::string& value) {
    if (!producer_ || !topic_) {
        return false;
    }

    RdKafka::ErrorCode err = producer_->produce(
        topic_.get(),
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(value.data()),
        value.size(),
        key.empty() ? nullptr : &key,
        nullptr);
    if (err != RdKafka::ERR_NO_ERROR) {
        LOG_ERROR << "Kafka produce failed: " << RdKafka::err2str(err);
        return false;
    }

    // poll 触发内部回调与发送进度
    producer_->poll(0);
    return true;
}

}  // namespace sparkpush


