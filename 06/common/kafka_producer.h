#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <memory>
#include <string>

namespace sparkpush {

class KafkaProducer {
 public:
  KafkaProducer() = default;
  ~KafkaProducer();

  bool Init(const std::string& brokers, const std::string& topic);
  bool Send(const std::string& key, const std::string& value);

 private:
  std::unique_ptr<RdKafka::Producer> producer_;
  std::unique_ptr<RdKafka::Topic> topic_;
  std::string topic_name_;
};

}  // namespace sparkpush


