#include "service.h"

#include "logging.h"

namespace sparkpush {

JobRunner::JobRunner(const Config& cfg) : cfg_(cfg) {}

bool JobRunner::Init() {
  LogInfo("[JobRunner] Initializing with separate push and persist handlers...");

  // 初始化推送处理器
  push_handler_ = std::make_unique<PushHandler>(cfg_);
  if (!push_handler_->Init()) {
    LogError("[JobRunner] PushHandler init failed");
    return false;
  }

  // 初始化持久化处理器
  persist_handler_ = std::make_unique<PersistHandler>(cfg_);
  if (!persist_handler_->Init()) {
    LogError("[JobRunner] PersistHandler init failed");
    return false;
  }

  LogInfo("[JobRunner] Initialized successfully");
  return true;
}

void JobRunner::Start() {
  LogInfo("[JobRunner] Starting handlers...");
  push_handler_->Start();
  persist_handler_->Start();
  LogInfo("[JobRunner] All handlers started");
}

void JobRunner::Stop() {
  LogInfo("[JobRunner] Stopping handlers...");
  if (push_handler_) {
    push_handler_->Stop();
  }
  if (persist_handler_) {
    persist_handler_->Stop();
  }
  LogInfo("[JobRunner] All handlers stopped");
}

}  // namespace sparkpush
