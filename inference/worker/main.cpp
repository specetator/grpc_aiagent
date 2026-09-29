#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>

#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "worker/inference_worker_service.h"
#include "worker/llama_cpp_backend.h"

namespace {
std::atomic<bool> running{true};
void Stop(int) { running = false; }
int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}
}  // namespace

int main(int argc, char** argv) {
  std::string id = "worker-0", listen = "127.0.0.1:9400";
  std::string model = "mock-model", gateway = "127.0.0.1:9300";
  std::string backend = "mock", model_endpoint;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--worker-id" && i + 1 < argc) id = argv[++i];
    else if (arg == "--listen" && i + 1 < argc) listen = argv[++i];
    else if (arg == "--model" && i + 1 < argc) model = argv[++i];
    else if (arg == "--gateway" && i + 1 < argc) gateway = argv[++i];
    else if (arg == "--backend" && i + 1 < argc) backend = argv[++i];
    else if (arg == "--model-endpoint" && i + 1 < argc) model_endpoint = argv[++i];
    else { std::cerr << "usage: inference_worker [--worker-id id] [--listen host:port] [--model id] [--gateway host:port] [--backend mock|llamacpp] [--model-endpoint http://host:port]\n"; return 2; }
  }
  if ((backend != "mock" && backend != "llamacpp") ||
      (backend == "llamacpp" && model_endpoint.empty())) {
    std::cerr << "invalid worker backend or missing model endpoint\n";
    return 2;
  }
  sparkpush::inference::WorkerInfo info;
  info.set_worker_id(id);
  info.set_endpoint(listen);
  info.set_model_id(model);
  info.set_device_type(backend == "mock" ? "cpu-mock" : "llamacpp");
  info.set_healthy(true);
  std::unique_ptr<sparkpush::inference::GenerationBackend> generation;
  if (backend == "mock")
    generation = std::make_unique<sparkpush::inference::MockGenerationBackend>();
  else
    generation = std::make_unique<sparkpush::inference::LlamaCppBackend>(model_endpoint);
  sparkpush::inference::InferenceWorkerService service(info, std::move(generation));
  grpc::EnableDefaultHealthCheckService(true);
  grpc::ServerBuilder builder;
  int bound_port = 0;
  builder.AddListeningPort(listen, grpc::InsecureServerCredentials(), &bound_port);
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  if (!server || bound_port == 0) { std::cerr << "failed to listen on " << listen << '\n'; return 1; }
  auto stub = sparkpush::inference::InferenceGateway::NewStub(
      grpc::CreateChannel(gateway, grpc::InsecureChannelCredentials()));
  std::signal(SIGINT, Stop);
  std::signal(SIGTERM, Stop);
  std::cerr << "inference worker " << id << " listening on " << listen << '\n';
  bool registered = false;
  while (running) {
    grpc::ServerContext status_context;
    sparkpush::inference::GetWorkerStatusRequest status_request;
    service.GetStatus(&status_context, &status_request, &info);
    info.set_timestamp_ms(NowMs());
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
    if (!registered) {
      sparkpush::inference::RegisterWorkerRequest request;
      *request.mutable_worker() = info;
      sparkpush::inference::RegisterWorkerReply reply;
      registered = stub->RegisterWorker(&context, request, &reply).ok() && reply.accepted();
    } else {
      sparkpush::inference::HeartbeatRequest request;
      *request.mutable_worker() = info;
      sparkpush::inference::HeartbeatReply reply;
      registered = stub->Heartbeat(&context, request, &reply).ok() && reply.accepted();
    }
    for (int i = 0; i < 10 && running; ++i)
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  server->Shutdown();
}
