#include "rate_limiter.h"

#include <algorithm>

namespace sparkpush {

SceneRateLimiter::SceneRateLimiter(const RateLimitConfig& config)
    : config_(config) {}

// scene 字符串 -> 配额档位。
// 采用「白名单 + 保守默认」：只识别 single 和 broadcast，其余（包括
// group、chatroom 以及任何未知值）全部归到 group 档。这样新增
// scene 时不会因为没改这里而变成「无限流」。
SceneRateLimiter::Rule SceneRateLimiter::RuleFor(
    const std::string& scene) const {
  if (scene == "single") {
    return {config_.single_rate_per_sec, config_.single_burst};
  }
  if (scene == "broadcast") {
    return {config_.broadcast_rate_per_sec, config_.broadcast_burst};
  }
  return {config_.group_rate_per_sec, config_.group_burst};
}

// 惰性令牌桶（lazy refill）：不开后台线程定时加令牌，而是在每次
// Allow 时按「距上次访问的时长」一次性补齐。
//
// 相比定时补币的优势：没有空转开销，空闲的桶不消耗任何 CPU；
// 且不需要为每个桶维护定时器。
bool SceneRateLimiter::Allow(const std::string& scene,
                             const std::string& key) {
  const Rule rule = RuleFor(scene);
  // 配额 <= 0 视为「该场景已关闭」，全部拒绝。
  // 注意这是 fail-closed（失败则拒绝）而不是 fail-open：
  // 配置写错时宁可误拒也不能变成无限流。
  if (rule.rate_per_sec <= 0.0 || rule.burst <= 0.0) return false;

  const auto now = std::chrono::steady_clock::now();
  // 桶键带 scene 前缀，避免不同场景的同名 key（比如单聊的 user_id
  // 与房间的 room_id 可能数值重叠）共用一个桶而互相抢配额。
  const std::string bucket_key = scene + ":" + key;
  std::lock_guard<std::mutex> lock(mutex_);
  auto& bucket = buckets_[bucket_key];
  // 首次访问：time_point 默认构造为 epoch（count()==0），用它作为
  // 「未初始化」哨兵，直接给满桶。这样新会话能立即发 burst 条，
  // 而不是从零开始慢慢攒。
  if (bucket.last_refill.time_since_epoch().count() == 0) {
    bucket.last_refill = now;
    bucket.tokens = rule.burst;
  }
  // 按经过时长补令牌，并用 min 封顶到 burst。
  // 封顶是必需的：不封顶的话一个空闲很久的桶会攒下巨量令牌，
  // 回头一瞬间放出远超设计峰值的流量，burst 就失去意义了。
  const double elapsed = std::chrono::duration<double>(now - bucket.last_refill).count();
  bucket.tokens = std::min(rule.burst, bucket.tokens + elapsed * rule.rate_per_sec);
  bucket.last_refill = now;
  // 不足一个令牌则拒绝，且**不预支**（不把 tokens 扣成负数）。
  // 预支会让后续请求额外等待，造成隐式排队，与「快速失败」的目标相反。
  if (bucket.tokens < 1.0) return false;
  bucket.tokens -= 1.0;
  return true;
}

}  // namespace sparkpush
