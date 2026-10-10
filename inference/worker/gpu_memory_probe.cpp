#include "worker/gpu_memory_probe.h"
#include <dlfcn.h>

namespace sparkpush::inference {
GpuMemoryProbe::GpuMemoryProbe(const WorkerInfo& info) : device_index_(info.device_index()) {
  if (device_index_ < 0) return;
  if (info.device_type() == "rocm") {
    library_ = dlopen("/opt/rocm/lib/libamdhip64.so", RTLD_NOW | RTLD_LOCAL);
    if (!library_) library_ = dlopen("libamdhip64.so", RTLD_NOW | RTLD_LOCAL);
    if (!library_) return;
    set_device_ = reinterpret_cast<int (*)(int)>(dlsym(library_, "hipSetDevice"));
    memory_info_ = reinterpret_cast<int (*)(size_t*, size_t*)>(dlsym(library_, "hipMemGetInfo"));
  } else if (info.device_type() == "cuda") {
    // Driver API keeps serving binaries buildable without CUDA headers/toolkit.
    library_ = dlopen("libcuda.so.1", RTLD_NOW | RTLD_LOCAL);
    if (!library_) return;
    auto init = reinterpret_cast<int (*)(unsigned)>(dlsym(library_, "cuInit"));
    auto get_device = reinterpret_cast<int (*)(int*, int)>(dlsym(library_, "cuDeviceGet"));
    auto retain = reinterpret_cast<int (*)(void**, int)>(dlsym(library_, "cuDevicePrimaryCtxRetain"));
    cuda_release_ = reinterpret_cast<int (*)(int)>(dlsym(library_, "cuDevicePrimaryCtxRelease_v2"));
    if (!cuda_release_)
      cuda_release_ = reinterpret_cast<int (*)(int)>(dlsym(library_, "cuDevicePrimaryCtxRelease"));
    cuda_get_current_ = reinterpret_cast<int (*)(void**)>(dlsym(library_, "cuCtxGetCurrent"));
    cuda_set_current_ = reinterpret_cast<int (*)(void*)>(dlsym(library_, "cuCtxSetCurrent"));
    memory_info_ = reinterpret_cast<int (*)(size_t*, size_t*)>(dlsym(library_, "cuMemGetInfo_v2"));
    if (!init || !get_device || !retain || !cuda_release_ || !cuda_get_current_ ||
        !cuda_set_current_ || !memory_info_ || init(0) != 0 ||
        get_device(&cuda_device_, device_index_) != 0 ||
        retain(&cuda_context_, cuda_device_) != 0) cuda_context_ = nullptr;
    // WSL cuMemGetInfo can omit other processes' allocations. Prefer NVML's
    // device-wide counters; match PCI identity, never assume a visible CUDA
    // ordinal equals an NVML physical index (CUDA_VISIBLE_DEVICES may reorder).
    auto pci_id = reinterpret_cast<int (*)(char*, int, int)>(dlsym(library_, "cuDeviceGetPCIBusId"));
    char pci[32] = {};
    if (cuda_device_ >= 0 && pci_id && pci_id(pci, sizeof(pci), cuda_device_) == 0) {
      nvml_library_ = dlopen("libnvidia-ml.so.1", RTLD_NOW | RTLD_LOCAL);
      if (nvml_library_) {
        auto nvml_init = reinterpret_cast<int (*)()>(dlsym(nvml_library_, "nvmlInit_v2"));
        auto by_pci = reinterpret_cast<int (*)(const char*, void**)>(
            dlsym(nvml_library_, "nvmlDeviceGetHandleByPciBusId_v2"));
        nvml_memory_ = reinterpret_cast<int (*)(void*, NvmlMemory*)>(
            dlsym(nvml_library_, "nvmlDeviceGetMemoryInfo"));
        nvml_shutdown_ = reinterpret_cast<int (*)()>(dlsym(nvml_library_, "nvmlShutdown"));
        if (nvml_init && by_pci && nvml_memory_ && nvml_shutdown_ && nvml_init() == 0) {
          nvml_initialized_ = true;
          if (by_pci(pci, &nvml_device_) != 0) nvml_device_ = nullptr;
        }
      }
    }
  }
}
GpuMemoryProbe::~GpuMemoryProbe() {
  if (cuda_context_) cuda_release_(cuda_device_);
  if (nvml_initialized_) nvml_shutdown_();
  if (nvml_library_) dlclose(nvml_library_);
  if (library_) dlclose(library_);
}
void GpuMemoryProbe::Sample(WorkerInfo* info) {
  std::lock_guard<std::mutex> lock(mutex_);
  info->set_free_memory_bytes(0);
  info->set_total_memory_bytes(0);
  info->set_memory_probe_source("unavailable");
  if (nvml_device_ && nvml_memory_) {
    NvmlMemory memory{};
    if (nvml_memory_(nvml_device_, &memory) == 0 && memory.total > 0 && memory.free <= memory.total) {
      info->set_free_memory_bytes(memory.free);
      info->set_total_memory_bytes(memory.total);
      info->set_memory_probe_source("nvml");
      return;
    }
  }
  size_t free = 0, total = 0;
  if (cuda_context_) {
    void* previous = nullptr;
    if (cuda_get_current_(&previous) != 0) return;
    const int set_result = cuda_set_current_(cuda_context_);
    const int sample_result = set_result == 0 ? memory_info_(&free, &total) : -1;
    const int restore_result = cuda_set_current_(previous);
    if (set_result != 0 || sample_result != 0 || restore_result != 0) return;
  } else {
    if (!set_device_ || !memory_info_ || set_device_(device_index_) != 0 ||
        memory_info_(&free, &total) != 0) return;
  }
  if (total == 0 || free > total) return;
  info->set_free_memory_bytes(free);
  info->set_total_memory_bytes(total);
  info->set_memory_probe_source(cuda_context_ ? "cuda_driver" : "hip");
}
}
