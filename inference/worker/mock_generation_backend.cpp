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
  const size_t limit = request.max_tokens() == 0 ? 6 :
      std::min<size_t>(6, request.max_tokens());
  for (size_t index = 0; index < limit; ++index) {
    // Each synchronous gRPC call occupies its own server worker thread only.
    // Polling in short slices also bounds explicit Cancel latency.
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
