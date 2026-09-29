#include <chrono>
#include <atomic>
#include <iostream>
#include <string>
#include <thread>

#include <grpcpp/grpcpp.h>

#include "inference.grpc.pb.h"

int main(int argc, char** argv) {
  std::string gateway = "127.0.0.1:9300", model = "mock-model", prompt = "hello";
  int cancel_after_chunks = 0;
  int max_tokens = 0;
  int cancel_after_ms = 0;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--gateway" && i + 1 < argc) gateway = argv[++i];
    else if (arg == "--model" && i + 1 < argc) model = argv[++i];
    else if (arg == "--prompt" && i + 1 < argc) prompt = argv[++i];
    else if (arg == "--cancel-after-chunks" && i + 1 < argc)
      cancel_after_chunks = std::stoi(argv[++i]);
    else if (arg == "--max-tokens" && i + 1 < argc)
      max_tokens = std::stoi(argv[++i]);
    else if (arg == "--cancel-after-ms" && i + 1 < argc)
      cancel_after_ms = std::stoi(argv[++i]);
    else { std::cerr << "usage: inference_cli [--gateway host:port] [--model id] [--prompt text] [--max-tokens N] [--cancel-after-chunks N] [--cancel-after-ms N]\n"; return 2; }
  }
  if (max_tokens < 0 || cancel_after_chunks < 0 || cancel_after_ms < 0) return 2;
  auto stub = sparkpush::inference::InferenceGateway::NewStub(
      grpc::CreateChannel(gateway, grpc::InsecureChannelCredentials()));
  sparkpush::inference::GenerateRequest request;
  request.set_request_id("cli-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  request.set_session_id("cli");
  request.set_model(model);
  request.set_prompt(prompt);
  request.set_max_tokens(max_tokens);
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  std::atomic<bool> timer_done{false};
  std::thread cancel_timer;
  if (cancel_after_ms > 0) {
    cancel_timer = std::thread([&] {
      for (int elapsed = 0; elapsed < cancel_after_ms && !timer_done.load(); elapsed += 10)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (!timer_done.load()) context.TryCancel();
    });
  }
  auto reader = stub->Generate(&context, request);
  sparkpush::inference::GenerateChunk chunk;
  int chunks = 0;
  while (reader->Read(&chunk)) {
    if (!chunk.text_delta().empty()) std::cout << '[' << chunk.sequence() << "] " << chunk.text_delta() << '\n';
    if (chunk.finished()) std::cout << "finished: " << chunk.finish_reason() << '\n';
    if (!chunk.text_delta().empty() && cancel_after_chunks > 0 &&
        ++chunks >= cancel_after_chunks) {
      context.TryCancel();
      break;
    }
  }
  auto status = reader->Finish();
  timer_done.store(true);
  if (cancel_timer.joinable()) cancel_timer.join();
  if (!status.ok()) { std::cerr << status.error_message() << '\n'; return 1; }
}
