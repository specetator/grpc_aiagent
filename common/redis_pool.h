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
    int health_check_interval_ms{30000};
    bool enable_auto_grow{true};
};

class RedisConnectionPool;

// RAII 连接守卫，析构时自动归还连接
class RedisConnGuard {
public:
    // 持有连接的生命周期守卫。
    RedisConnGuard(RedisConnectionPool& pool, redisContext* ctx);
    // 析构时自动归还连接。
    ~RedisConnGuard();

    redisContext* get() const { return ctx_; }

private:
    RedisConnectionPool& pool_;
    redisContext* ctx_;
};

// 线程安全的 Redis 连接池，支持自动扩容与空闲回收
class RedisConnectionPool {
public:
    RedisConnectionPool() = default;
    ~RedisConnectionPool();

    // 初始化连接池，并按配置预热连接。
    bool Init(const RedisConfig& cfg);

    // 获取连接，支持阻塞等待。
    RedisConnGuard Acquire(int timeout_ms = -1);

private:
    redisContext* CreateContextUnlocked();
    bool PopIdleUnlocked(
        redisContext** ctx,
        std::chrono::steady_clock::time_point* last_used);
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
