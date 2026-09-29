#pragma once

#include <string>

#include "worker/generation_backend.h"

namespace sparkpush::inference {

// Adapts a separately running llama-server. Model execution and batching stay
// in llama.cpp; Spark Push owns only request routing and the gRPC stream.
class LlamaCppBackend final : public GenerationBackend {
 public:
  explicit LlamaCppBackend(std::string endpoint) : endpoint_(std::move(endpoint)) {}
  bool Generate(const GenerateRequest& request,
      const std::atomic<bool>& cancelled,
      const std::function<bool(const GenerateChunk&)>& emit) override;

 private:
  std::string endpoint_;
};

}  // namespace sparkpush::inference
