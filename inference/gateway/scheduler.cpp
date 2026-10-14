#include "gateway/scheduler.h"

#include <algorithm>

namespace sparkpush::inference {

std::optional<WorkerInfo> LeastLoadedScheduler::SelectWorker(
    const GenerateRequest& request, const std::vector<WorkerInfo>& workers) const {
  std::optional<WorkerInfo> best;
  double best_score = 0;
  for (const auto& worker : workers) {
    if (!worker.healthy() || worker.model_id() != request.model()) continue;
    // max_concurrent_requests 为 0 表示不设上限。非 0 且已满则跳过。
    if (worker.max_concurrent_requests() &&
        worker.running_requests() >= worker.max_concurrent_requests()) continue;
    // 探不到显存时压力记 0，而不是记成「已经占满」。
    // 否则探针失败的机器会被永远排到最后，甚至在只有它健康时也选不中。
    const double pressure = worker.total_memory_bytes() == 0 ? 0.0 :
        1.0 - std::min(1.0, static_cast<double>(worker.free_memory_bytes()) /
                            worker.total_memory_bytes());
    const double score = worker.running_requests() * weights_.running +
        worker.waiting_requests() * weights_.waiting +
        pressure * weights_.memory_pressure;
    if (!best || score < best_score ||
        (score == best_score && worker.worker_id() < best->worker_id())) {
      best = worker;
      best_score = score;
    }
  }
  return best;
}

}  // namespace sparkpush::inference
