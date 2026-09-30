#pragma once
#include <mutex>
#include "inference.pb.h"

namespace sparkpush::inference {
// Optional HIP memory telemetry. CPU builds do not link to ROCm and remain
// runnable without its SDK. Missing/failed probes leave memory unknown (zero).
class GpuMemoryProbe {
 public:
  explicit GpuMemoryProbe(const WorkerInfo& info);
  ~GpuMemoryProbe();
  void Sample(WorkerInfo* info);
 private:
  void* library_{nullptr};
  int (*set_device_)(int){nullptr};
  int (*memory_info_)(size_t*, size_t*){nullptr};
  int device_index_{0};
  std::mutex mutex_;
};
}
