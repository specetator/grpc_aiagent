#include "worker/gpu_memory_probe.h"
#include <dlfcn.h>

namespace sparkpush::inference {
GpuMemoryProbe::GpuMemoryProbe(const WorkerInfo& info) : device_index_(info.device_index()) {
  if (info.device_type() != "rocm") return;
  library_ = dlopen("/opt/rocm/lib/libamdhip64.so", RTLD_NOW | RTLD_LOCAL);
  if (!library_) return;
  set_device_ = reinterpret_cast<int (*)(int)>(dlsym(library_, "hipSetDevice"));
  memory_info_ = reinterpret_cast<int (*)(size_t*, size_t*)>(dlsym(library_, "hipMemGetInfo"));
}
GpuMemoryProbe::~GpuMemoryProbe() { if (library_) dlclose(library_); }
void GpuMemoryProbe::Sample(WorkerInfo* info) {
  if (!set_device_ || !memory_info_) return;
  std::lock_guard<std::mutex> lock(mutex_);
  size_t free = 0, total = 0;
  if (set_device_(device_index_) != 0 || memory_info_(&free, &total) != 0) return;
  info->set_free_memory_bytes(free);
  info->set_total_memory_bytes(total);
}
}
