#pragma once

#include <atomic>
#include <string>
#include <thread>

namespace sparkpush {

// 仅绑定本机的轻量 metrics HTTP server。
// Logic 已有 Muduo HTTP 服务，Job/Comet 使用该小 server 暴露同一套指标。
class MetricsHttpServer {
 public:
  MetricsHttpServer() = default;
  ~MetricsHttpServer();

  MetricsHttpServer(const MetricsHttpServer&) = delete;
  MetricsHttpServer& operator=(const MetricsHttpServer&) = delete;

  bool Start(int port);
  void Stop();

 private:
  void Loop();
  void HandleClient(int fd);

  int port_{0};
  std::atomic<int> listen_fd_{-1};
  std::atomic<bool> running_{false};
  std::thread thread_;
};

}  // namespace sparkpush
