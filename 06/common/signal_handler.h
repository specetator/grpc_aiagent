#pragma once

#include <atomic>
#include <csignal>
#include <functional>

namespace sparkpush {

// 全局优雅关闭标志，收到 SIGTERM/SIGINT 后置 true
inline std::atomic<bool>& ShutdownRequested() {
  static std::atomic<bool> flag{false};
  return flag;
}

// 注册 SIGTERM / SIGINT 信号处理函数
// 可选传入自定义回调，会在信号到达时调用（需信号安全）
inline void InstallSignalHandler(std::function<void()> on_signal = nullptr) {
  static std::function<void()> s_callback;
  s_callback = std::move(on_signal);

  auto handler = [](int sig) {
    (void)sig;
    ShutdownRequested().store(true, std::memory_order_relaxed);
    if (s_callback) {
      s_callback();
    }
  };

  std::signal(SIGTERM, handler);
  std::signal(SIGINT, handler);
}

}  // namespace sparkpush
