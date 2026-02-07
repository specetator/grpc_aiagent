#include "kafka_producer.h"

#include "logging.h"

#include <librdkafka/rdkafkacpp.h>

namespace sparkpush {

KafkaProducer::~KafkaProducer() {
  if (producer_) {
    producer_->flush(3000);
  }
}

bool KafkaProducer::Init(const std::string& brokers, const std::string& topic) {
  std::string errstr;
  topic_name_ = topic;
  RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  auto set_conf = [&](const std::string& key, const std::string& value) {
    if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
      LogError("KafkaProducer set " + key + " failed: " + errstr);
      return false;
    }
    return true;
  };
  if (!set_conf("bootstrap.servers", brokers)) {
    delete conf;
    return false;
  }
  // Kafka 低延迟配置（牺牲吞吐量换取延迟）：
  // 在 IM 场景下，消息实时性优先于批量效率
  set_conf("acks", "1");                    // 只等 leader 确认（不等 ISR 全部同步）
  set_conf("enable.idempotence", "false");  // 关闭幂等（减少额外的协议往返）
  set_conf("compression.type", "none");     // 不压缩（消息体小，压缩收益不大）
  set_conf("linger.ms", "0");               // 不等待批量聚合，立即发送
  set_conf("batch.num.messages", "1");      // 每条消息独立发送
  set_conf("retries", "5");                 // 发送失败最多重试 5 次
  set_conf("retry.backoff.ms", "500");
  set_conf("socket.keepalive", "true");     // TCP keepalive 保活
  set_conf("message.timeout.ms", "60000");  // 消息超时 60 秒
  set_conf("queue.buffering.max.messages", "200000");
  if (!topic_name_.empty()) {
    set_conf("client.id", "spark_push_producer_" + topic_name_);
  }
  producer_.reset(RdKafka::Producer::create(conf, errstr));
  delete conf;
  if (!producer_) {
    LogError("KafkaProducer set brokers failed: " + errstr);
    return false;
  }
  RdKafka::Conf* topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  topic_.reset(RdKafka::Topic::create(producer_.get(), topic_name_, topic_conf, errstr));
  delete topic_conf;
  if (!topic_) {
    LogError("KafkaProducer create topic failed: " + errstr);
    return false;
  }
  LogInfo("KafkaProducer initialized topic=" + topic_name_);
  return true;
}

bool KafkaProducer::Send(const std::string& key, const std::string& value) {
  if (!producer_ || !topic_) return false;
  RdKafka::ErrorCode err = producer_->produce(
      topic_.get(),
      RdKafka::Topic::PARTITION_UA,
      RdKafka::Producer::RK_MSG_COPY,
      const_cast<char*>(value.data()),
      value.size(),
      key.empty() ? nullptr : &key,
      nullptr);
  if (err != RdKafka::ERR_NO_ERROR) {
    LogError("Kafka produce failed: " + RdKafka::err2str(err));
    return false;
  }
  producer_->poll(0);
  return true;
}

}  // namespace sparkpush


