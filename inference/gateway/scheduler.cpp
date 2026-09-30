#include "gateway/scheduler.h"

#include <algorithm>

namespace sparkpush::inference {

std::optional<WorkerInfo> LeastLoadedScheduler::SelectWorker(
    const GenerateRequest& request, const std::vector<WorkerInfo>& workers) const {
  std::optional<WorkerInfo> best;
  double best_score = 0;
  for (const auto& worker : workers) {
    if (!worker.healthy() || worker.model_id() != request.model()) continue;
    if (worker.max_concurrent_requests() &&
        worker.running_requests() >= worker.max_concurrent_requests()) continue;
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
