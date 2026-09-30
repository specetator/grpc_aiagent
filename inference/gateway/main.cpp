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

int main(int argc, char** argv) {
  std::string listen = "127.0.0.1:9300";
  int metrics_port = 9301;
  sparkpush::inference::AdmissionOptions admission;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--listen" && i + 1 < argc) listen = argv[++i];
    else if (arg == "--metrics-port" && i + 1 < argc) metrics_port = std::stoi(argv[++i]);
    else if (arg == "--max-queue" && i + 1 < argc) admission.max_queue = std::stoul(argv[++i]);
    else if (arg == "--queue-timeout-ms" && i + 1 < argc)
      admission.queue_timeout = std::chrono::milliseconds(std::stoi(argv[++i]));
    else if (arg == "--no-session-affinity") admission.session_affinity = false;
    else { std::cerr << "usage: inference_gateway [--listen host:port] [--metrics-port port] [--max-queue N] [--queue-timeout-ms N] [--no-session-affinity]\n"; return 2; }
  }
  sparkpush::inference::WorkerRegistry registry;
  sparkpush::inference::LeastLoadedScheduler scheduler;
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
  if (metrics_port > 0 && !metrics.Start(metrics_port)) {
    std::cerr << "failed to listen on metrics port " << metrics_port << '\n'; return 1;
  }
  std::signal(SIGINT, Stop);
  std::signal(SIGTERM, Stop);
  std::cerr << "inference gateway listening on " << listen << '\n';
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
