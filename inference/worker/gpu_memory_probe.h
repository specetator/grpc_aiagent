#pragma once
#include <mutex>
#include "inference.pb.h"

namespace sparkpush::inference {
// Optional HIP/CUDA telemetry, loaded dynamically; no SDK link dependency.
// CUDA prefers NVML matched by PCI identity; Driver API is a labelled fallback.
// Missing/failed probes leave memory unknown (zero), not stale capacity.
//
// 译：可选的 HIP/CUDA 遥测，运行时 dlopen，不链接 SDK。
// CUDA 优先用按 PCI 对上的 NVML；Driver API 是标明来源的退路。
// 探针缺失或失败时显存保持未知（两个字段都是 0），不沿用上一次的容量。
// Sample 每次先把字段写成 0 再填，所以失败不会留下构造前调用方塞进来的旧数。
class GpuMemoryProbe {
 public:
  explicit GpuMemoryProbe(const WorkerInfo& info);
  ~GpuMemoryProbe();
  void Sample(WorkerInfo* info);
 private:
  void* library_{nullptr};
  int (*set_device_)(int){nullptr};
  int (*memory_info_)(size_t*, size_t*){nullptr};
  int (*cuda_get_current_)(void**){nullptr};
  int (*cuda_set_current_)(void*){nullptr};
  int (*cuda_release_)(int){nullptr};
  void* cuda_context_{nullptr};
  int cuda_device_{-1};
  struct NvmlMemory { unsigned long long total, free, used; };
  void* nvml_library_{nullptr};
  void* nvml_device_{nullptr};
  int (*nvml_memory_)(void*, NvmlMemory*){nullptr};
  int (*nvml_shutdown_)(){nullptr};
  bool nvml_initialized_{false};
  int device_index_{0};
  std::mutex mutex_;
};
}
