#include <chrono>
#include <atomic>
#include <iostream>
#include <string>
#include <thread>
#include <nlohmann/json.hpp>

#include <grpcpp/grpcpp.h>

#include "inference.grpc.pb.h"

int main(int argc, char** argv) {
  std::string gateway = "127.0.0.1:9300", model = "mock-model", prompt = "hello";
  int cancel_after_chunks = 0;
  int max_tokens = 0;
  int cancel_after_ms = 0;
  int timeout_ms = 120000, seed = 42;
  bool json_output = false, cache_prompt = true, ignore_eos = false;
  std::string session = "cli";
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--gateway" && i + 1 < argc) gateway = argv[++i];
    else if (arg == "--model" && i + 1 < argc) model = argv[++i];
    else if (arg == "--prompt" && i + 1 < argc) prompt = argv[++i];
    else if (arg == "--cancel-after-chunks" && i + 1 < argc)
      cancel_after_chunks = std::stoi(argv[++i]);
    else if (arg == "--max-tokens" && i + 1 < argc)
      max_tokens = std::stoi(argv[++i]);
    else if (arg == "--json") json_output = true;
    else if (arg == "--session" && i + 1 < argc) session = argv[++i];
    else if (arg == "--seed" && i + 1 < argc) seed = std::stoi(argv[++i]);
    else if (arg == "--timeout-ms" && i + 1 < argc) timeout_ms = std::stoi(argv[++i]);
    else if (arg == "--no-cache-prompt") cache_prompt = false;
    else if (arg == "--ignore-eos") ignore_eos = true;
    else if (arg == "--cancel-after-ms" && i + 1 < argc)
      cancel_after_ms = std::stoi(argv[++i]);
    else { std::cerr << "usage: inference_cli [--gateway host:port] [--model id] [--prompt text] [--max-tokens N] [--cancel-after-chunks N] [--cancel-after-ms N] [--json] [--session id] [--seed N] [--timeout-ms N] [--no-cache-prompt] [--ignore-eos]\n"; return 2; }
  }
  if (max_tokens < 0 || cancel_after_chunks < 0 || cancel_after_ms < 0 || timeout_ms <= 0) return 2;
  auto stub = sparkpush::inference::InferenceGateway::NewStub(
      grpc::CreateChannel(gateway, grpc::InsecureChannelCredentials()));
  sparkpush::inference::GenerateRequest request;
  request.set_request_id("cli-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  request.set_session_id(session);
  request.set_model(model);
  request.set_prompt(prompt);
  request.set_max_tokens(max_tokens);
  request.set_temperature(0);
  (*request.mutable_metadata())["seed"] = std::to_string(seed);
  (*request.mutable_metadata())["cache_prompt"] = cache_prompt ? "true" : "false";
  (*request.mutable_metadata())["ignore_eos"] = ignore_eos ? "true" : "false";
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));
  std::atomic<bool> timer_done{false};
  std::thread cancel_timer;
  if (cancel_after_ms > 0) {
    cancel_timer = std::thread([&] {
      for (int elapsed = 0; elapsed < cancel_after_ms && !timer_done.load(); elapsed += 10)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (!timer_done.load()) context.TryCancel();
    });
  }
  const auto start = std::chrono::steady_clock::now();
  auto elapsed = [&] { return std::chrono::duration<double, std::milli>(
      std::chrono::steady_clock::now() - start).count(); };
  double ttft = -1;
  nlohmann::json result{{"request_id", request.request_id()}, {"session_id", session},
      {"text", ""}, {"completion_tokens", 0}, {"prompt_tokens", 0}};
  auto reader = stub->Generate(&context, request);
  sparkpush::inference::GenerateChunk chunk;
  int chunks = 0;
  while (reader->Read(&chunk)) {
    if (!chunk.text_delta().empty()) {
      if (ttft < 0) ttft = elapsed();
      result["text"] = result["text"].get<std::string>() + chunk.text_delta();
      if (!json_output) std::cout << '[' << chunk.sequence() << "] " << chunk.text_delta() << '\n';
    }
    if (chunk.finished()) {
      result["finish_reason"] = chunk.finish_reason();
      result["prompt_tokens"] = chunk.prompt_tokens();
      result["completion_tokens"] = chunk.completion_tokens();
      result["cached_prompt_tokens"] = chunk.cached_prompt_tokens();
      result["runtime_prefill_ms"] = chunk.runtime_prefill_ms();
      result["runtime_decode_ms"] = chunk.runtime_decode_ms();
      if (!json_output) std::cout << "finished: " << chunk.finish_reason() << '\n';
    }
    if (!chunk.text_delta().empty() && cancel_after_chunks > 0 &&
        ++chunks >= cancel_after_chunks) {
      context.TryCancel();
      break;
    }
  }
  auto status = reader->Finish();
  result["ttft_ms"] = ttft;
  result["e2e_ms"] = elapsed();
  result["ok"] = status.ok() && result.contains("finish_reason");
  result["grpc_code"] = status.error_code();
  if (json_output) std::cout << result.dump() << '\n';
  timer_done.store(true);
  if (cancel_timer.joinable()) cancel_timer.join();
  if (!status.ok()) { std::cerr << status.error_message() << '\n'; return 1; }
}
