#pragma once

#include <string>

#include "worker/generation_backend.h"

namespace sparkpush::inference {

// Adapts a separately running llama-server. Model execution and batching stay
// in llama.cpp; Spark Push owns only request routing and the gRPC stream.
//
// 译：适配一个单独跑着的 llama-server。模型执行和批处理留在 llama.cpp，
// Spark Push 只负责请求路由和 gRPC 流。本类不加载权重。
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
