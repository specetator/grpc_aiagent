// Deterministic Driver API fixture. Never used by production workers.
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
