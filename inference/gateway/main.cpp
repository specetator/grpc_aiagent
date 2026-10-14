#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <memory>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "gateway/gateway_service.h"
#include "metrics.h"
#include "metrics_http_server.h"

namespace {
std::atomic<bool> running{true};
void Stop(int) { running = false; }
}  // namespace

// 推理网关进程。不读模型，只做注册、心跳超时和请求转发。
// 默认绑 127.0.0.1，信道没有 TLS。这是给本机 Bridge 用的，不要直接绑到 0.0.0.0。
int main(int argc, char** argv) {
  std::string listen = "127.0.0.1:9300";
  int metrics_port = 9301;
  bool gateway_managed_only = false;
  sparkpush::inference::AdmissionOptions admission;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--listen" && i + 1 < argc) listen = argv[++i];
    else if (arg == "--metrics-port" && i + 1 < argc) metrics_port = std::stoi(argv[++i]);
    else if (arg == "--max-queue" && i + 1 < argc) admission.max_queue = std::stoul(argv[++i]);
    else if (arg == "--queue-timeout-ms" && i + 1 < argc)
      admission.queue_timeout = std::chrono::milliseconds(std::stoi(argv[++i]));
    else if (arg == "--no-session-affinity") admission.session_affinity = false;
    else if (arg == "--gateway-managed-only") gateway_managed_only = true;
    else { std::cerr << "usage: inference_gateway [--listen host:port] [--metrics-port port] [--max-queue N] [--queue-timeout-ms N] [--no-session-affinity] [--gateway-managed-only]\n"; return 2; }
  }
  // 心跳超时写死 5 秒，和 worker 大约每秒一次的心跳配在一起：
  // 单次 RPC 失败不会立刻把节点摘掉，连丢几次才会标成不健康。
  sparkpush::inference::WorkerRegistry registry(std::chrono::seconds(5), gateway_managed_only);
  sparkpush::MetricsRegistry::Instance().Set("inference_gateway_managed_only", gateway_managed_only ? 1 : 0);
  sparkpush::inference::LeastLoadedScheduler scheduler;
  // 队列长度 0 和超过 4096 都拒绝启动。0 会被后面的「队列已满」判断当成永远满，
  // 所有请求直接 RESOURCE_EXHAUSTED。超时必须是正数。
  // stoi/stoul 格式不对会抛异常，进程直接起不来，不带着半截参数跑。
  if (!admission.max_queue || admission.max_queue > 4096 || admission.queue_timeout.count() <= 0)
    return 2;
  sparkpush::inference::GatewayService service(&registry, &scheduler, admission);
  grpc::EnableDefaultHealthCheckService(true);
  grpc::ServerBuilder builder;
  int bound_port = 0;
  builder.AddListeningPort(listen, grpc::InsecureServerCredentials(), &bound_port);
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  if (!server || bound_port == 0) { std::cerr << "failed to listen on " << listen << '\n'; return 1; }
  sparkpush::MetricsHttpServer metrics;
  // metrics_port <= 0 时不启 metrics HTTP，进程照样跑。负端口不会报错。
  if (metrics_port > 0 && !metrics.Start(metrics_port)) {
    std::cerr << "failed to listen on metrics port " << metrics_port << '\n'; return 1;
  }
  std::signal(SIGINT, Stop);
  std::signal(SIGTERM, Stop);
  std::cerr << "inference gateway listening on " << listen << '\n';
  // 每 200ms 把注册表里的在途数、等待数、最旧心跳年龄刷进指标。
  // 过期 worker 仍在表里（healthy=false），年龄照样计入。表空时年龄保持 0。
  while (running) {
    const auto workers = registry.ListWorkers();
    int64_t running_count = 0, waiting_count = 0, oldest_age = 0;
    const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    for (const auto& worker : workers) {
      running_count += worker.running_requests();
      waiting_count += worker.waiting_requests();
      oldest_age = std::max<int64_t>(oldest_age, now_ms - worker.timestamp_ms());
    }
    auto& registry_metrics = sparkpush::MetricsRegistry::Instance();
    registry_metrics.Set("inference_worker_running_requests", running_count);
    registry_metrics.Set("inference_worker_waiting_requests", waiting_count);
    registry_metrics.Set("inference_worker_heartbeat_age_ms", oldest_age);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  server->Shutdown();
  metrics.Stop();
}
