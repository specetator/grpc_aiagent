#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>

namespace sparkpush {

struct RateLimitConfig {
  double single_rate_per_sec{300.0};
  double single_burst{60.0};
  double group_rate_per_sec{120.0};
  double group_burst{30.0};
  double broadcast_rate_per_sec{20.0};
  double broadcast_burst{5.0};
};

// 按 scene + 业务 key 维护令牌桶。
// single 以 sender 为 key，group 以 room 为 key，broadcast 以 scope 为 key，
// 达到上限时由上层保留 Kafka 有界队列/重试语义。
class SceneRateLimiter {
 public:
  explicit SceneRateLimiter(const RateLimitConfig& config);

  bool Allow(const std::string& scene, const std::string& key);

 private:
  struct Bucket {
    double tokens{0.0};
    std::chrono::steady_clock::time_point last_refill;
  };

  struct Rule {
    double rate_per_sec{0.0};
    double burst{0.0};
  };

  Rule RuleFor(const std::string& scene) const;

  RateLimitConfig config_;
  mutable std::mutex mutex_;
  std::unordered_map<std::string, Bucket> buckets_;
};

}  // namespace sparkpush
