#pragma once

#include <atomic>
#include <string>
#include <thread>

namespace sparkpush {

// 仅绑定本机的轻量 metrics HTTP server。
// Logic 已有 Muduo HTTP 服务，Job/Comet 使用该小 server 暴露同一套指标。
//
// 为何三个服务不共用一种方案：Logic 本来就有 muduo EventLoop 在跑，
// 指标可以直接挂在业务 HTTP 端口（9101）上；Job 完全没有 EventLoop，
// Comet 的 loop 也不适合再混入管理流量。这个独立小 server 让三者
// 能用同一套指标名与探针语义（/metrics 与 /ready）。
//
// 默认端口：Logic 9101（复用业务）、Job 9202、Comet 9203。
class MetricsHttpServer {
 public:
  MetricsHttpServer() = default;
  ~MetricsHttpServer();

  // 禁拷贝：对象持有 listen fd 与线程，拷贝会造成双重 close 与双重 join。
  MetricsHttpServer(const MetricsHttpServer&) = delete;
  MetricsHttpServer& operator=(const MetricsHttpServer&) = delete;

  // port <= 0 视为「未启用」并返回 true；重复 Start 返回 false。
  bool Start(int port);
  // 幂等；会 shutdown listen fd 以唤醒阻塞在 poll 里的线程。
  void Stop();

 private:
  void Loop();
  void HandleClient(int fd);

  int port_{0};
  // 两个标志都用 atomic：Stop() 可能在任意线程被调（包括析构），
  // 而 Loop() 在自己的线程里读。listen_fd_ 用 atomic<int> 而不是
  // 普通 int + mutex：Loop 需要无锁地频繁读它。
  std::atomic<int> listen_fd_{-1};
  std::atomic<bool> running_{false};
  std::thread thread_;   // 声明在最后：保证先于其他成员被销毁
};

}  // namespace sparkpush
