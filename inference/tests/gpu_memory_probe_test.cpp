#include "worker/gpu_memory_probe.h"
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iostream>

// 三件事：NVML 按 PCI 命中时用 6 GiB；NVML 失败时退回 Driver API 的 8 GiB；
// 采样结束后调用方原来的 CUDA context（这里的 sentinel）必须还在。
// 其他失败把显存写成 0、来源写成 unavailable，不能留下采样前塞进去的 123/456。
int main() {
  using namespace sparkpush::inference;
  void* lib = dlopen("libcuda.so.1", RTLD_NOW | RTLD_LOCAL);
  if (!lib) return 1;
  auto get = reinterpret_cast<int (*)(void**)>(dlsym(lib, "cuCtxGetCurrent"));
  auto set = reinterpret_cast<int (*)(void*)>(dlsym(lib, "cuCtxSetCurrent"));
  int sentinel;
  set(&sentinel);
  WorkerInfo info;
  info.set_device_type("cuda"); info.set_device_index(0);
  {
    GpuMemoryProbe probe(info);
    info.set_free_memory_bytes(123); info.set_total_memory_bytes(456);
    probe.Sample(&info);
    const char* fail = std::getenv("SPARK_TEST_CUDA_FAIL");
    const bool fallback = fail && std::strcmp(fail,"nvml") == 0;
    const bool failed = fail && !fallback;
    if (info.free_memory_bytes() != (failed ? 0 : uint64_t{fallback ? 8U : 6U} << 30) ||
        info.total_memory_bytes() != (failed ? 0 : uint64_t{12} << 30)) return 2;
    if (info.memory_probe_source() != (failed ? "unavailable" : fallback ? "cuda_driver" : "nvml")) return 5;
    void* current = nullptr; get(&current);
    if (current != &sentinel) return 3;
  }
  info.set_device_type("cpu");
  GpuMemoryProbe cpu(info); cpu.Sample(&info);
  if (info.total_memory_bytes() != 0 || info.free_memory_bytes() != 0) return 4;
  dlclose(lib);
  std::cout << "GPU probe: NVML PCI match, driver fallback and context restoration OK\n";
}
