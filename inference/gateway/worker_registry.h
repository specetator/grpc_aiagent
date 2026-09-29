#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "inference.pb.h"

namespace sparkpush::inference {

class WorkerRegistry {
 public:
  explicit WorkerRegistry(std::chrono::milliseconds timeout = std::chrono::seconds(5))
      : timeout_(timeout) {}
  bool Register(const WorkerInfo& worker);
  bool Heartbeat(const WorkerInfo& worker);
  bool Remove(const std::string& worker_id);
  std::vector<WorkerInfo> ListWorkers() const;
  std::vector<WorkerInfo> FindWorkersForModel(const std::string& model) const;
  bool ChangeRunning(const std::string& worker_id, int delta);

 private:
  struct Entry {
    WorkerInfo info;
    std::chrono::steady_clock::time_point last_heartbeat;
    int active_gateway_requests{0};
  };
  WorkerInfo Snapshot(const Entry& entry,
                      std::chrono::steady_clock::time_point now) const;
  const std::chrono::milliseconds timeout_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, Entry> workers_;
};

}  // namespace sparkpush::inference
