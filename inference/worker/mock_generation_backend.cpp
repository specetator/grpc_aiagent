#include "worker/generation_backend.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace sparkpush::inference {
namespace {
int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}
}  // namespace

bool MockGenerationBackend::Generate(const GenerateRequest& request,
    const std::atomic<bool>& cancelled,
    const std::function<bool(const GenerateChunk&)>& emit) {
  static constexpr const char* kTokens[] = {
      "Hello", ", ", "this ", "is ", "a ", "stream."};
  // max_tokens==0 用全部 6 个词。小于 6 时少发几个，终态 finish_reason 写成 length。
  // 推理网关不因 length 失败，它只转发。Hermes 的 SSE 解析才会把非 stop 当失败。
  const size_t limit = request.max_tokens() == 0 ? 6 :
      std::min<size_t>(6, request.max_tokens());
  for (size_t index = 0; index < limit; ++index) {
    // Each synchronous gRPC call occupies its own server worker thread only.
    // Polling in short slices also bounds explicit Cancel latency.
    // 译：同步 gRPC 调用只占它自己的那条服务线程。
    // 按 10ms 切片轮询，是为了把显式 Cancel 的生效时间压在大约一个切片里。
    for (int step = 0; step < 3; ++step) {
      if (cancelled.load()) return false;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (cancelled.load()) return false;
    GenerateChunk chunk;
    chunk.set_request_id(request.request_id());
    chunk.set_sequence(index);
    chunk.set_text_delta(kTokens[index]);
    chunk.set_timestamp_ms(NowMs());
    if (!emit(chunk)) return false;
  }
  // 取消时直接返回 false，不发终态块。worker 把 false 映射成 CANCELLED。
  // 若这里仍发 finished=true，上游会把一次取消记成正常结束。
  if (cancelled.load()) return false;
  GenerateChunk terminal;
  terminal.set_request_id(request.request_id());
  terminal.set_sequence(limit);
  terminal.set_finished(true);
  terminal.set_finish_reason(limit < 6 ? "length" : "stop");
  terminal.set_timestamp_ms(NowMs());
  return emit(terminal);
}

}  // namespace sparkpush::inference
