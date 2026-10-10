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
