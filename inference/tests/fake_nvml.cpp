// NVML 替身。只认 fake_cuda_driver 报出的 PCI 0000:01:00.0，别的总线号返回失败，
// 用来锁住「按 PCI 找卡，不按 CUDA 序号猜」这条路径。
// SPARK_TEST_CUDA_FAIL=nvml 或 sample 时本函数失败，探针应退回 Driver API。
// 空闲显存是 6 GiB，和 Driver API 的 8 GiB 不同，调用方靠这个数判断读的是哪一层。
#include <cstdlib>
#include <cstring>
struct Memory { unsigned long long total, free, used; };
extern "C" {
int nvmlInit_v2() { return 0; }
int nvmlShutdown() { return 0; }
int nvmlDeviceGetHandleByPciBusId_v2(const char* pci, void** device) {
  if (std::strcmp(pci,"0000:01:00.0") != 0) return 1;
  *device = reinterpret_cast<void*>(1); return 0;
}
int nvmlDeviceGetMemoryInfo(void*, Memory* memory) {
  const char* fail = std::getenv("SPARK_TEST_CUDA_FAIL");
  if (fail && (std::strcmp(fail,"sample") == 0 || std::strcmp(fail,"nvml") == 0)) return 1;
  memory->total = 12ULL << 30; memory->free = 6ULL << 30; memory->used = 6ULL << 30;
  return 0;
}
}
