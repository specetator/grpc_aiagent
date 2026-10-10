#pragma once
#include <mutex>
#include "inference.pb.h"

namespace sparkpush::inference {
// Optional HIP/CUDA telemetry, loaded dynamically; no SDK link dependency.
// CUDA prefers NVML matched by PCI identity; Driver API is a labelled fallback.
// Missing/failed probes leave memory unknown (zero), not stale capacity.
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
