#pragma once

#include <atomic>
#include <functional>

#include "inference.pb.h"

namespace sparkpush::inference {

class GenerationBackend {
 public:
  virtual ~GenerationBackend() = default;
  // Returns false when generation was cancelled or the stream write failed.
  virtual bool Generate(const GenerateRequest& request,
      const std::atomic<bool>& cancelled,
      const std::function<bool(const GenerateChunk&)>& emit) = 0;
};

class MockGenerationBackend final : public GenerationBackend {
 public:
  bool Generate(const GenerateRequest& request,
      const std::atomic<bool>& cancelled,
      const std::function<bool(const GenerateChunk&)>& emit) override;
};

}  // namespace sparkpush::inference
