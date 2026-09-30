#include <chrono>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>
#include <atomic>
#include "metrics.h"

#include <grpcpp/grpcpp.h>

#include "gateway/gateway_service.h"
#include "worker/inference_worker_service.h"
#include "grpc_inference_client.h"

using namespace sparkpush::inference;

namespace {
void Check(bool ok, const char* message) {
  if (!ok) { std::cerr << "FAIL: " << message << '\n'; std::exit(1); }
}
WorkerInfo MakeWorker(const std::string& id, const std::string& endpoint = "127.0.0.1:1") {
  WorkerInfo info;
  info.set_worker_id(id);
  info.set_endpoint(endpoint);
  info.set_model_id("mock-model");
  info.set_healthy(true);
  info.set_total_memory_bytes(100);
  info.set_free_memory_bytes(100);
  return info;
}
GenerateRequest MakeRequest(const std::string& id) {
  GenerateRequest request;
  request.set_request_id(id);
  request.set_session_id("test");
  request.set_model("mock-model");
  request.set_prompt("hello");
  return request;
}
void WireTemperaturePresence() {
  GenerateRequest request;
  Check(request.sampling_temperature_case() != GenerateRequest::kTemperature,
        "unset temperature presence");
  // Field 7, fixed32 zero: existing proto3 optional clients encode the same bytes.
  const std::string legacy_zero("\x3d\x00\x00\x00\x00", 5);
  Check(request.ParseFromString(legacy_zero) &&
        request.sampling_temperature_case() == GenerateRequest::kTemperature &&
        request.temperature() == 0 && request.SerializeAsString() == legacy_zero,
        "legacy explicit zero temperature wire compatibility");
  request.clear_temperature();
  Check(request.sampling_temperature_case() != GenerateRequest::kTemperature,
        "cleared temperature presence");
}
void RegistryAndScheduler() {
  WorkerRegistry registry(std::chrono::milliseconds(25));
  auto a = MakeWorker("a"), b = MakeWorker("b");
  Check(registry.Register(a), "register");
  a.set_running_requests(3);
  Check(registry.Register(a), "duplicate register updates");
  Check(registry.ListWorkers()[0].running_requests() == 3, "updated load");
  Check(registry.Register(b), "second register");
  Check(registry.Heartbeat(a), "heartbeat");
  Check(registry.FindWorkersForModel("other").empty(), "model filtering");
  LeastLoadedScheduler scheduler;
  auto chosen = scheduler.SelectWorker(MakeRequest("scheduler"), registry.ListWorkers());
  Check(chosen && chosen->worker_id() == "b", "least loaded");
  b.set_running_requests(3);
  Check(registry.Heartbeat(b), "heartbeat b");
  chosen = scheduler.SelectWorker(MakeRequest("tie"), registry.ListWorkers());
  Check(chosen && chosen->worker_id() == "a", "deterministic tie");
  a.set_max_concurrent_requests(3);
  Check(registry.Heartbeat(a), "capacity heartbeat");
  chosen = scheduler.SelectWorker(MakeRequest("capacity"), registry.ListWorkers());
  Check(chosen && chosen->worker_id() == "b", "full worker filtered");
  std::this_thread::sleep_for(std::chrono::milliseconds(35));
  Check(registry.FindWorkersForModel("mock-model").empty(), "heartbeat expiry");
  Check(!registry.ListWorkers()[0].healthy(), "expired unhealthy");
  Check(registry.Remove("a"), "remove");
}
class BlockingBackend final : public GenerationBackend {
 public:
  std::atomic<bool> entered{false}, release{false};
  bool Generate(const GenerateRequest& request, const std::atomic<bool>& cancelled,
      const std::function<bool(const GenerateChunk&)>& emit) override {
    entered = true;
    while (!release && !cancelled) std::this_thread::sleep_for(std::chrono::milliseconds(2));
    if (cancelled) return false;
    GenerateChunk end;
    end.set_request_id(request.request_id());
    end.set_finished(true); end.set_finish_reason("stop");
    return emit(end);
  }
};
void AdmissionAndCapacity() {
  WorkerRegistry registry;
  LeastLoadedScheduler scheduler;
  AdmissionOptions options;
  options.max_queue = 1;
  options.queue_timeout = std::chrono::milliseconds(100);
  GatewayService gateway(&registry, &scheduler, options);
  grpc::ServerBuilder gb;
  int gp = 0, wp = 0;
  gb.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &gp);
  gb.RegisterService(&gateway);
  auto gs = gb.BuildAndStart();
  auto backend = std::make_unique<BlockingBackend>();
  auto* blocking = backend.get();
  auto info = MakeWorker("bounded"); info.set_max_concurrent_requests(1);
  InferenceWorkerService worker(info, std::move(backend));
  grpc::ServerBuilder wb;
  wb.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &wp);
  wb.RegisterService(&worker);
  auto ws = wb.BuildAndStart();
  Check(gs && ws, "bounded servers start");
  info.set_endpoint("127.0.0.1:" + std::to_string(wp));
  registry.Register(info);
  auto stub = InferenceGateway::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(gp), grpc::InsecureChannelCredentials()));
  auto generate = [&](const std::string& id) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    auto stream = stub->Generate(&context, MakeRequest(id));
    GenerateChunk chunk; while (stream->Read(&chunk)) {}
    return stream->Finish();
  };
  grpc::Status owner_status, queued_status;
  std::thread owner([&] { owner_status = generate("owner"); });
  for (int i = 0; i < 500 && !blocking->entered; ++i)
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  Check(blocking->entered, "capacity owner entered");
  std::thread queued([&] { queued_status = generate("queued"); });
  bool waiting = false;
  for (int i = 0; i < 100 && !waiting; ++i) {
    waiting = sparkpush::MetricsRegistry::Instance().RenderPrometheus().find(
        "inference_requests_waiting 1\n") != std::string::npos;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  Check(waiting, "request queued");
  Check(generate("overflow").error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED,
        "bounded queue rejects overflow");
  grpc::ClientContext cancel_context;
  CancelRequest cancel; cancel.set_request_id("queued"); CancelReply reply;
  Check(stub->Cancel(&cancel_context, cancel, &reply).ok() && reply.cancelled(), "queued cancel");
  queued.join();
  Check(queued_status.error_code() == grpc::StatusCode::CANCELLED, "queued cancellation status");
  Check(generate("timeout").error_code() == grpc::StatusCode::DEADLINE_EXCEEDED,
        "queue deadline enforced");
  auto direct = InferenceWorker::NewStub(grpc::CreateChannel(info.endpoint(), grpc::InsecureChannelCredentials()));
  grpc::ClientContext direct_context;
  auto stream = direct->Generate(&direct_context, MakeRequest("direct-overflow"));
  GenerateChunk chunk; while (stream->Read(&chunk)) {}
  Check(stream->Finish().error_code() == grpc::StatusCode::RESOURCE_EXHAUSTED, "direct worker cap");
  blocking->release = true; owner.join();
  Check(owner_status.ok() && registry.ListWorkers()[0].running_requests() == 0,
        "reserved capacity released");
  const auto metrics = sparkpush::MetricsRegistry::Instance().RenderPrometheus();
  Check(metrics.find("inference_queue_wait_ms_bucket{le=\"+Inf\"}") != std::string::npos,
        "Prometheus histogram buckets exported");
  ws->Shutdown(); gs->Shutdown();
}
void StreamAndCancel() {
  WorkerRegistry registry(std::chrono::seconds(5));
  LeastLoadedScheduler scheduler;
  GatewayService gateway(&registry, &scheduler);
  grpc::ServerBuilder gateway_builder;
  int gateway_port = 0;
  gateway_builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &gateway_port);
  gateway_builder.RegisterService(&gateway);
  auto gateway_server = gateway_builder.BuildAndStart();
  Check(gateway_server && gateway_port > 0, "gateway start");
  auto gateway_stub = InferenceGateway::NewStub(grpc::CreateChannel(
      "127.0.0.1:" + std::to_string(gateway_port), grpc::InsecureChannelCredentials()));
  {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    auto reader = gateway_stub->Generate(&context, MakeRequest("none"));
    GenerateChunk chunk;
    Check(!reader->Read(&chunk), "no worker no chunks");
    Check(reader->Finish().error_code() == grpc::StatusCode::UNAVAILABLE, "no worker unavailable");
  }
  WorkerInfo info = MakeWorker("worker-test");
  InferenceWorkerService worker(info, std::make_unique<MockGenerationBackend>());
  grpc::ServerBuilder worker_builder;
  int worker_port = 0;
  worker_builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &worker_port);
  worker_builder.RegisterService(&worker);
  auto worker_server = worker_builder.BuildAndStart();
  Check(worker_server && worker_port > 0, "worker start");
  info.set_endpoint("127.0.0.1:" + std::to_string(worker_port));
  Check(registry.Register(info), "register live worker");
  {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    auto reader = gateway_stub->Generate(&context, MakeRequest("stream"));
    GenerateChunk chunk;
    int count = 0;
    bool terminal = false;
    while (reader->Read(&chunk)) {
      Check(chunk.request_id() == "stream", "request id propagated");
      Check(chunk.sequence() == static_cast<uint64_t>(count++), "ordered sequence");
      terminal = chunk.finished();
    }
    Check(reader->Finish().ok() && count == 7 && terminal, "stream terminal");
    Check(registry.ListWorkers()[0].running_requests() == 0, "running count decremented");
  }
  {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    auto reader = gateway_stub->Generate(&context, MakeRequest("cancel"));
    GenerateChunk chunk;
    Check(reader->Read(&chunk), "first chunk before cancel");
    grpc::ClientContext cancel_context;
    CancelRequest cancel_request;
    cancel_request.set_request_id("cancel");
    CancelReply cancel_reply;
    Check(gateway_stub->Cancel(&cancel_context, cancel_request, &cancel_reply).ok() &&
          cancel_reply.cancelled(), "cancel rpc");
    int extra = 0;
    while (reader->Read(&chunk)) ++extra;
    Check(extra == 0, "no chunks after cancel");
    Check(!reader->Finish().ok(), "cancel status");
    Check(registry.ListWorkers()[0].running_requests() == 0, "cancel count decremented");
    WorkerInfo status;
    GetWorkerStatusRequest status_request;
    for (int attempt = 0; attempt < 30; ++attempt) {
      grpc::ServerContext status_context;
      worker.GetStatus(&status_context, &status_request, &status);
      if (status.running_requests() == 0) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    Check(status.running_requests() == 0, "worker stopped after cancel");
  }
  {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));
    auto reader = gateway_stub->Generate(&context, MakeRequest("disconnect"));
    GenerateChunk chunk;
    Check(reader->Read(&chunk), "first chunk before upstream disconnect");
    context.TryCancel();
    Check(!reader->Read(&chunk), "disconnect stops stream");
    Check(reader->Finish().error_code() == grpc::StatusCode::CANCELLED,
          "disconnect cancelled status");
  }
  {
    sparkpush::HermesBridgeConfig config;
    config.inference_gateway = "127.0.0.1:" + std::to_string(gateway_port);
    config.inference_model = "mock-model";
    config.request_timeout_ms = 3000;
    std::atomic<bool> running{true};
    sparkpush::GrpcInferenceClient client(config, &running);
    sparkpush::HermesChatOptions options;
    options.images = nlohmann::json::array();
    options.request_id = "bridge-client";
    options.session_id = "test";
    sparkpush::HermesChatResult result;
    std::string error, deltas;
    const nlohmann::json messages = nlohmann::json::array({
        {{"role", "user"}, {"content", "hello"}}});
    const bool bridge_ok = client.ChatStream(messages, options,
        [&](const std::string& delta) { deltas += delta; },
        &result, &error);
    if (!bridge_ok) std::cerr << "bridge client error: " << error << '\n';
    Check(bridge_ok, "bridge gRPC client stream");
    Check(result.text == "Hello, this is a stream." &&
          deltas == result.text && result.stream_chunk_count == 6,
          "bridge gRPC client text and deltas");
  }
  worker_server->Shutdown();
  {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(250));
    auto reader = gateway_stub->Generate(&context, MakeRequest("down"));
    GenerateChunk chunk;
    Check(!reader->Read(&chunk), "down worker no chunk");
    Check(!reader->Finish().ok(), "down worker failed");
    Check(registry.ListWorkers()[0].running_requests() == 0, "failure count decremented");
  }
  gateway_server->Shutdown();
}
}  // namespace

int main() {
  WireTemperaturePresence();
  RegistryAndScheduler();
  StreamAndCancel();
  AdmissionAndCapacity();
  std::cout << "inference tests passed\n";
}
