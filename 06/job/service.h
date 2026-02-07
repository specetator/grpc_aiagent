#pragma once

#include "config.h"
#include "push_handler.h"
#include "persist_handler.h"

#include <memory>

namespace sparkpush {

// Job服务主入口：组合推送和持久化两个处理器
// 两个处理器使用独立的Kafka消费组，互不影响
class JobRunner {
 public:
  explicit JobRunner(const Config& cfg);
  bool Init();
  void Start();
  void Stop();

 private:
  Config cfg_;
  std::unique_ptr<PushHandler> push_handler_;
  std::unique_ptr<PersistHandler> persist_handler_;
};

}  // namespace sparkpush
