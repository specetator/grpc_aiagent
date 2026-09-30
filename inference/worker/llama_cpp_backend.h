#pragma once

#include <string>

#include "worker/generation_backend.h"

namespace sparkpush::inference {

// Adapts a separately running llama-server. Model execution and batching stay
// in llama.cpp; Spark Push owns only request routing and the gRPC stream.
class LlamaCppBackend final : public GenerationBackend {
 public:
  explicit LlamaCppBackend(std::string endpoint, std::string runtime = "llamacpp")
      : endpoint_(std::move(endpoint)), runtime_(std::move(runtime)) {}
  bool Generate(const GenerateRequest& request,
      const std::atomic<bool>& cancelled,
      const std::function<bool(const GenerateChunk&)>& emit) override;
  void GetRuntimeStatus(WorkerInfo* info) override;

 private:
  std::string endpoint_;
  std::string runtime_;
};

}  // namespace sparkpush::inference
