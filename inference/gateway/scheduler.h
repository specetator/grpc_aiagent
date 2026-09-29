#pragma once

#include <optional>
#include <vector>

#include "inference.pb.h"

namespace sparkpush::inference {

class Scheduler {
 public:
  virtual ~Scheduler() = default;
  virtual std::optional<WorkerInfo> SelectWorker(
      const GenerateRequest& request,
      const std::vector<WorkerInfo>& workers) const = 0;
};

struct SchedulerWeights {
  double running{1.0};
  double waiting{2.0};
  double memory_pressure{1.0};
};

class LeastLoadedScheduler final : public Scheduler {
 public:
  explicit LeastLoadedScheduler(SchedulerWeights weights = {}) : weights_(weights) {}
  std::optional<WorkerInfo> SelectWorker(
      const GenerateRequest& request,
      const std::vector<WorkerInfo>& workers) const override;

 private:
  SchedulerWeights weights_;
};

}  // namespace sparkpush::inference
