#include "gateway/worker_registry.h"

#include <algorithm>

namespace sparkpush::inference {
namespace {
int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}
}  // namespace

bool WorkerRegistry::Register(const WorkerInfo& worker) {
  if (worker.worker_id().empty() || worker.endpoint().empty() ||
      worker.model_id().empty()) return false;
  std::lock_guard<std::mutex> lock(mutex_);
  // 重复注册只刷新 info 和心跳，不把 active_gateway_requests 清零。
  // 清零会让网关以为这个 worker 突然空了，把正在跑的请求再塞一批进来。
  auto& entry = workers_[worker.worker_id()];
  entry.info = worker;
  entry.info.set_timestamp_ms(NowMs());
  entry.last_heartbeat = std::chrono::steady_clock::now();
  return true;
}

bool WorkerRegistry::Heartbeat(const WorkerInfo& worker) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = workers_.find(worker.worker_id());
  if (it == workers_.end() || worker.endpoint().empty() ||
      worker.model_id().empty()) return false;
  it->second.info = worker;
  it->second.info.set_timestamp_ms(NowMs());
  it->second.last_heartbeat = std::chrono::steady_clock::now();
  return true;
}

bool WorkerRegistry::Remove(const std::string& worker_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  return workers_.erase(worker_id) != 0;
}

WorkerInfo WorkerRegistry::Snapshot(
    const Entry& entry, std::chrono::steady_clock::time_point now) const {
  WorkerInfo info = entry.info;
  if (now - entry.last_heartbeat > timeout_) info.set_healthy(false);
  // In an exclusively gateway-managed fleet, local RPC leases are the live
  // admission truth. Heartbeat load can remain full up to one second after all
  // those RPCs complete. Default mode still accounts for reported/direct load.
  // 译：只由本网关管辖时，本地 RPC 租约才是准入的实时真相。
  // 心跳里的负载在全部 RPC 结束后最多还能满一秒。
  // 默认模式取「心跳上报」和「本网关计数」的较大值，避免漏算直接打到 worker 的请求。
  info.set_running_requests(gateway_managed_only_ ? entry.active_gateway_requests :
      std::max<uint32_t>(info.running_requests(), entry.active_gateway_requests));
  return info;
}

std::vector<WorkerInfo> WorkerRegistry::ListWorkers() const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto now = std::chrono::steady_clock::now();
  std::vector<WorkerInfo> result;
  result.reserve(workers_.size());
  for (const auto& item : workers_) result.push_back(Snapshot(item.second, now));
  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
    return a.worker_id() < b.worker_id();
  });
  return result;
}

std::vector<WorkerInfo> WorkerRegistry::FindWorkersForModel(
    const std::string& model) const {
  auto workers = ListWorkers();
  workers.erase(std::remove_if(workers.begin(), workers.end(),
      [&](const WorkerInfo& info) {
        return !info.healthy() || info.model_id() != model;
      }), workers.end());
  return workers;
}

bool WorkerRegistry::ChangeRunning(const std::string& worker_id, int delta) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = workers_.find(worker_id);
  // 减到负数直接拒绝，计数保持原值。调用方拿到 false 不能假设已经减过。
  if (it == workers_.end() || it->second.active_gateway_requests + delta < 0)
    return false;
  it->second.active_gateway_requests += delta;
  return true;
}

}  // namespace sparkpush::inference
