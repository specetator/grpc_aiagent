#include "rate_limiter.h"

#include <algorithm>

namespace sparkpush {

SceneRateLimiter::SceneRateLimiter(const RateLimitConfig& config)
    : config_(config) {}

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

bool SceneRateLimiter::Allow(const std::string& scene,
                             const std::string& key) {
  const Rule rule = RuleFor(scene);
  if (rule.rate_per_sec <= 0.0 || rule.burst <= 0.0) return false;

  const auto now = std::chrono::steady_clock::now();
  const std::string bucket_key = scene + ":" + key;
  std::lock_guard<std::mutex> lock(mutex_);
  auto& bucket = buckets_[bucket_key];
  if (bucket.last_refill.time_since_epoch().count() == 0) {
    bucket.last_refill = now;
    bucket.tokens = rule.burst;
  }
  const double elapsed = std::chrono::duration<double>(now - bucket.last_refill).count();
  bucket.tokens = std::min(rule.burst, bucket.tokens + elapsed * rule.rate_per_sec);
  bucket.last_refill = now;
  if (bucket.tokens < 1.0) return false;
  bucket.tokens -= 1.0;
  return true;
}

}  // namespace sparkpush
