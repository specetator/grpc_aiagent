#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>

namespace sparkpush {

// 三个场景的令牌桶参数（速率 + 桶容量）。
//
// 默认值的量级关系是有意的：single(300/s) > group(120/s) > broadcast(20/s)。
// 单聊是主业务，配额最高；广播影响面最大且容易被滥用，配额最低。
// burst 是「瞬时峰值容量」，比 rate 小很多，因此只能吸收短暂突发，
// 不能靠积攒令牌来支撑持续高频。
//
// 任何一项 <= 0 都被视为「该场景关闭」，Allow 直接返回 false（见 .cpp）。
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
//
// 桶键实际存为 scene + ":" + key，因此不同场景的同名 key 不会互相干扰。
//
// 当前实现的已知边界：buckets_ 只增不减（没有淘汰机制），长期运行且
// key 基数很大时会持续占内存。在教学/单机场景下可接受；上生产需加
// LRU 或定期清理。
class SceneRateLimiter {
 public:
  explicit SceneRateLimiter(const RateLimitConfig& config);

  // 尝试取一个令牌。true = 放行，false = 应回 429。
  // 线程安全（内部全局 mutex），但不做阻塞等待：限流必须快速失败，
  // 否则堆积的等待线程本身就会拖垮服务。
  bool Allow(const std::string& scene, const std::string& key);

 private:
  // 单个桶的状态。用 steady_clock 而不是 system_clock：
  // 后者会被 NTP 校时跳变，一次向后跳就能让令牌瞬间满桶或长期为负。
  struct Bucket {
    double tokens{0.0};
    std::chrono::steady_clock::time_point last_refill;
  };

  struct Rule {
    double rate_per_sec{0.0};
    double burst{0.0};
  };

  // scene -> 规则映射。未知 scene 归到 group 档（保守默认）。
  Rule RuleFor(const std::string& scene) const;

  RateLimitConfig config_;
  // 保护 buckets_。声明为 mutable 是为了将来能在 const 方法里做只读
  // 统计（当前 Allow 本身非 const，因此这个修饰并非必需）。
  mutable std::mutex mutex_;
  std::unordered_map<std::string, Bucket> buckets_;
};

}  // namespace sparkpush
