#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "inference.pb.h"

namespace sparkpush::inference {

// Worker 注册表。心跳超过 timeout 的节点在快照里被标成不健康，
// 调度器不会再把新请求分给它。用 steady_clock 算超时，不被 NTP 回拨影响。
//
// gateway_managed_only 为真时，running_requests 只信本网关自己的
// ChangeRunning 计数，不采信心跳里上报的负载。心跳最多晚一秒，
// 会在请求已经结束之后仍显示「满载」。
class WorkerRegistry {
 public:
  explicit WorkerRegistry(std::chrono::milliseconds timeout = std::chrono::seconds(5),
                          bool gateway_managed_only = false)
      : timeout_(timeout), gateway_managed_only_(gateway_managed_only) {}
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
  const bool gateway_managed_only_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, Entry> workers_;
};

}  // namespace sparkpush::inference
