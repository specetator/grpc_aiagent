#include "metrics_http_server.h"

#include "logging.h"
#include "metrics.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

namespace sparkpush {

// 极简的 /metrics 与 /ready HTTP 服务。
//
// 为何不用 muduo 而直接写 socket：本服务只需要处理两种固定路径的
// 短连接请求，且必须在业务服务之前/之后都能独立启停。不依赖
// muduo EventLoop 意味着它能在任何进程（包括没有 loop 的 Job）里跑。

MetricsHttpServer::~MetricsHttpServer() { Stop(); }

bool MetricsHttpServer::Start(int port) {
  // port <= 0 时返回 true（而不是 false）：这是有意的。配置里没开
  // metrics 端口是合法情况（比如单测），不该让调用方当成启动失败；
  // 而 already running 返回 false 才是真的错误。
  if (port <= 0 || running_) return port <= 0;
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;
  int reuse = 1;
  // SO_REUSEADDR：服务频繁重启时避开 TIME_WAIT 导致的 bind 失败。
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<uint16_t>(port));
  // 硬编码只绑 127.0.0.1：指标里含连接数、延迟、业务计数，属于
  // 内部信息，不应暴露到对外网卡。需要远程采集时应该走 SSH 隧道
  // 或 Prometheus 本机 agent，而不是把端口开出去。
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(fd, 16) != 0) {
    ::close(fd);   // 失败路径必须关 fd，否则重启时泄漏描述符
    return false;
  }
  port_ = port;
  listen_fd_.store(fd);
  running_ = true;
  thread_ = std::thread(&MetricsHttpServer::Loop, this);
  LOG_INFO << "Metrics HTTP server listening on 127.0.0.1:" << port_;
  return true;
}

// 停机。exchange 保证幂等：析构与显式 Stop 重复调时只生效一次。
//
// 关 fd 前先 shutdown(SHUT_RDWR)：Loop 可能正阻塞在 poll 里，
// 单纯 close 不会唤醒它（poll 仍持有旧 fd 的引用），shutdown 才会
// 让它立即返回错误从而退出循环。这是避免 join 卡死的关键。
void MetricsHttpServer::Stop() {
  if (!running_.exchange(false)) return;
  const int fd = listen_fd_.exchange(-1);
  if (fd >= 0) {
    ::shutdown(fd, SHUT_RDWR);
    ::close(fd);
  }
  if (thread_.joinable()) thread_.join();
}

// 接受循环。用 poll 而不是阻塞 accept，就是为了能被 Stop 唤醒。
//
// 200ms 超时是「停机响应延迟」与「空转开销」的折中：即使 shutdown
// 没及时唤醒 poll，最多 200ms 后也会因 running_ 变 false 而退出。
//
// 单线程串行处理：指标采集频率极低（几秒一次），不值得引入并发；
// 串行也避开了 HandleClient 里对 MetricsRegistry 的额外争用。
void MetricsHttpServer::Loop() {
  while (running_) {
    const int listen_fd = listen_fd_.load();
    if (listen_fd < 0) break;
    pollfd descriptor{listen_fd, POLLIN, 0};
    const int ready = ::poll(&descriptor, 1, 200);
    if (!running_) break;
    if (ready <= 0 || !(descriptor.revents & POLLIN)) continue;
    const int client = ::accept(listen_fd, nullptr, nullptr);
    if (client >= 0) HandleClient(client);
  }
}

// 处理一个短连接请求，只识别 GET /metrics 与 GET /ready。
//
// 用 strstr 而不是完整 HTTP 解析：这是内部探针端点，只需区分路径。
// 注意 "GET /ready " 带了尾空格：避免 /readyxxx 这类路径被误匹配。
//
// /ready 的语义：读 spark_push_service_ready 这个 gauge，不就绪时返回
// 503（而不是 200 + 错误正文）。这样 curl -f 与容器健康检查能直接
// 用退出码判断，sparkctl.sh health 就依赖这个行为。
//
// 超时保护：收发各 1s。探针卡住不应该拖死整个 accept 循环。
void MetricsHttpServer::HandleClient(int fd) {
  timeval timeout{1, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  // 4 KiB 固定缓冲，只读一次：探针请求头很短，不需要处理分段。
  // 留一个字节给 '\0'，因为下面用 strstr 需要 C 字符串。
  char request[4096] = {0};
  const ssize_t received = ::recv(fd, request, sizeof(request) - 1, 0);
  const bool metrics = received > 0 &&
                       std::strstr(request, "GET /metrics") != nullptr;
  const bool readiness = received > 0 && std::strstr(request, "GET /ready ") != nullptr;
  const bool ready = MetricsRegistry::Instance().GaugeValue("spark_push_service_ready") == 1;
  const std::string body = readiness ? (ready ? "ready\n" : "not ready\n") : metrics
                               ? MetricsRegistry::Instance().RenderPrometheus()
                               : "not found\n";
  const std::string status = readiness ? (ready ? "200 OK" : "503 Service Unavailable") : metrics ? "200 OK" : "404 Not Found";
  // Content-Type 带 version=0.0.4：Prometheus 文本格式的版本声明，
  // 缺了它部分采集器会拒绝解析。Connection: close 配合单次请求短连接。
  const std::string response =
      "HTTP/1.1 " + status + "\r\nContent-Type: text/plain; version=0.0.4\r\n"
      "Content-Length: " + std::to_string(body.size()) +
      "\r\nConnection: close\r\n\r\n" + body;
  // 循环 send：单次 send 可能只写出部分字节（内核缓冲满），
  // 必须写完否则客户端会因 Content-Length 不匹配而挂住。
  // MSG_NOSIGNAL：对端已关时返回错误而不是发 SIGPIPE 杀掉整个进程。
  size_t sent = 0;
  while (sent < response.size()) {
    const ssize_t bytes = ::send(fd, response.data() + sent, response.size() - sent, MSG_NOSIGNAL);
    if (bytes <= 0) break;
    sent += bytes;
  }
  ::shutdown(fd, SHUT_RDWR);
  ::close(fd);
}

}  // namespace sparkpush
