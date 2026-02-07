#pragma once

#include <hiredis/hiredis.h>

#include <condition_variable>
#include <chrono>
#include <deque>
#include <mutex>
#include <queue>
#include <string>

namespace sparkpush {

// Redis 连接池配置
// 设计要点：
//   - 支持自动扩容（enable_auto_grow）：空闲连接不足时动态创建，上限 max_pool_size
//   - 空闲回收（idle_timeout_ms）：超时未使用的连接自动释放，避免资源泄漏
//   - 热路径优化：Acquire() 跳过 PING 验证（减少一次网络往返），连接失效在使用时发现
struct RedisConfig {
  std::string host;
  int port{6379};
  std::string password;
  int db{0};
  int pool_size{4};
  int min_pool_size{0};
  int max_pool_size{0};
  int connect_timeout_ms{2000};
  int rw_timeout_ms{2000};
  int idle_timeout_ms{60000};
  bool enable_auto_grow{true};
};

class RedisConnectionPool;

// RAII 连接守卫：析构时自动归还连接到池（类似 std::unique_lock）
class RedisConnGuard {
 public:
  RedisConnGuard(RedisConnectionPool& pool, redisContext* ctx);
  ~RedisConnGuard();  // 自动调用 pool_.Release(ctx_)

  redisContext* get() const { return ctx_; }

 private:
  RedisConnectionPool& pool_;
  redisContext* ctx_;
};

class RedisConnectionPool {
 public:
  RedisConnectionPool() = default;
  ~RedisConnectionPool();

  bool Init(const RedisConfig& cfg);

  RedisConnGuard Acquire(int timeout_ms = -1);

 private:
  redisContext* CreateContextUnlocked();
  redisContext* PopIdleUnlocked();
  void CleanupIdleUnlocked();
  bool Validate(redisContext** ctx);
  void Release(redisContext* ctx);

  friend class RedisConnGuard;

  std::mutex mutex_;
  std::condition_variable cv_;
  struct ConnEntry {
    redisContext* ctx{nullptr};
    std::chrono::steady_clock::time_point last_used;
  };
  std::deque<ConnEntry> idle_;
  int total_conns_{0};
  bool stopping_{false};
  RedisConfig cfg_;
};

}  // namespace sparkpush


