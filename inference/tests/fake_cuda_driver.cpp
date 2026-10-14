// Deterministic Driver API fixture. Never used by production workers.
// 译：确定性的 Driver API 替身，生产 worker 不会加载它。
// 测试用 LD_PRELOAD / 同名 so 把它当成 libcuda.so.1。
// PCI 固定报 0000:01:00.0，必须和 fake_nvml 接受的那条总线号一致，否则 NVML 对不上。
// SPARK_TEST_CUDA_FAIL=init 让 cuInit 失败；=sample 让 cuMemGetInfo 失败。
// 显存固定 8 GiB 空闲 / 12 GiB 总量，和 NVML 的 6 GiB 空闲区分开，测试靠这个判断走了哪条探针。
#include <cstdlib>
#include <cstddef>
#include <cstring>
namespace {
int primary;
thread_local void* current = nullptr;
bool fail(const char* stage) {
  const char* value = std::getenv("SPARK_TEST_CUDA_FAIL");
  return value && std::strcmp(value, stage) == 0;
}
}
extern "C" {
int cuInit(unsigned) { return fail("init") ? 1 : 0; }
int cuDeviceGet(int* device, int index) { *device = index; return index == 0 ? 0 : 1; }
int cuDeviceGetPCIBusId(char* pci, int length, int) {
  if (length < 13) return 1;
  std::strcpy(pci, "0000:01:00.0"); return 0;
}
int cuDevicePrimaryCtxRetain(void** ctx, int) { *ctx = &primary; return 0; }
int cuDevicePrimaryCtxRelease_v2(int) { return 0; }
int cuCtxGetCurrent(void** ctx) { *ctx = current; return 0; }
int cuCtxSetCurrent(void* ctx) { current = ctx; return 0; }
int cuMemGetInfo_v2(size_t* free, size_t* total) {
  if (current != &primary || fail("sample")) return 1;
  *free = size_t{8} << 30; *total = size_t{12} << 30; return 0;
}
}
