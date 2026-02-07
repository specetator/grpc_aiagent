#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <thread>

namespace sparkpush {

class KafkaConsumer {
 public:
  struct Options {
    bool enable_auto_commit{false};
    int auto_commit_interval_ms{5000};
    int max_poll_interval_ms{300000};
    int session_timeout_ms{45000};
    std::string auto_offset_reset{"earliest"};
  };

  KafkaConsumer() = default;
  ~KafkaConsumer();

  bool Init(const std::string& brokers,
            const std::string& group_id,
            const std::string& topic,
            std::function<void(const std::string&, const std::string&)> callback,
            const Options& options);

  void Start();
  void Stop();

 private:
  void Loop();
  void HandleMessage(RdKafka::Message* message);

  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
  std::string topic_;
  std::function<void(const std::string&, const std::string&)> callback_;
  Options options_;

  std::atomic<bool> running_{false};
  std::thread thread_;
};

}  // namespace sparkpush


