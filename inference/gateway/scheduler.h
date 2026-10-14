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

// 分数越低越优先。waiting 默认权重高于 running：
// 一个队列里已经有人等的 worker，比「正在跑但没人排队」的更不该再接。
// 平分时按 worker_id 字典序，保证同一份输入每次选出同一个节点。
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
