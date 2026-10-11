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

MetricsHttpServer::~MetricsHttpServer() { Stop(); }

bool MetricsHttpServer::Start(int port) {
  if (port <= 0 || running_) return port <= 0;
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return false;
  int reuse = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(static_cast<uint16_t>(port));
  ::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
      ::listen(fd, 16) != 0) {
    ::close(fd);
    return false;
  }
  port_ = port;
  listen_fd_.store(fd);
  running_ = true;
  thread_ = std::thread(&MetricsHttpServer::Loop, this);
  LOG_INFO << "Metrics HTTP server listening on 127.0.0.1:" << port_;
  return true;
}

void MetricsHttpServer::Stop() {
  if (!running_.exchange(false)) return;
  const int fd = listen_fd_.exchange(-1);
  if (fd >= 0) {
    ::shutdown(fd, SHUT_RDWR);
    ::close(fd);
  }
  if (thread_.joinable()) thread_.join();
}

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

void MetricsHttpServer::HandleClient(int fd) {
  timeval timeout{1, 0};
  ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
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
  const std::string response =
      "HTTP/1.1 " + status + "\r\nContent-Type: text/plain; version=0.0.4\r\n"
      "Content-Length: " + std::to_string(body.size()) +
      "\r\nConnection: close\r\n\r\n" + body;
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
