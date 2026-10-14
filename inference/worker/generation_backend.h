#pragma once

#include <atomic>
#include <functional>

#include "inference.pb.h"

namespace sparkpush::inference {

class GenerationBackend {
 public:
  virtual ~GenerationBackend() = default;
  // Returns false when generation was cancelled or the stream write failed.
  // 译：取消或写出失败时返回 false。其余失败应抛异常，
  // worker 把 false 映射成 CANCELLED，把异常映射成 INTERNAL。不要混用。
  virtual bool Generate(const GenerateRequest& request,
      const std::atomic<bool>& cancelled,
      const std::function<bool(const GenerateChunk&)>& emit) = 0;
  // Runtime probes execute outside the worker's active-request mutex.
  // 译：运行时探针在 worker 的活跃请求锁之外执行。
  // 实现里不要假设自己持有那把锁，也不要在里面做会卡住生成的长调用。
  virtual void GetRuntimeStatus(WorkerInfo*) {}
};

class MockGenerationBackend final : public GenerationBackend {
 public:
  bool Generate(const GenerateRequest& request,
      const std::atomic<bool>& cancelled,
      const std::function<bool(const GenerateChunk&)>& emit) override;
};

}  // namespace sparkpush::inference
