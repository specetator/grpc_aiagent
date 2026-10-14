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
//
// 与 MySqlConfig 结构几乎一致（min/pool/max 三档、空闲回收、自动扩容），
// 这是有意的：两个池的 Acquire/Release/Cleanup 策略保持同构，
// 读懂一个就能推定另一个。
//
// 为何 Redis 也需要池：hiredis 的 redisContext **不是线程安全**的，
// 多线程共用一个 context 会造成协议流错乱（上一个命令的回复被
// 当成下一个命令的结果）。而 Logic 的 gRPC handler 是多线程并发的，
// 因此每次操作必须独占一个 context。
struct RedisConfig {
    std::string host;
    int port{6379};
    std::string password;
    int db{0};
    int pool_size{4};
    int min_pool_size{0};
    int max_pool_size{0};
    int connect_timeout_ms{2000};
    // 读写超时。对 Redis 尤其重要：热路径上的序号分配（AllocateSessionMsgSeq）
    // 是同步等待的，没有超时的话 Redis 卡住会直接拖死整个消息链路。
    int rw_timeout_ms{2000};
    int idle_timeout_ms{60000};
    // 健康检查间隔。Validate 里据此判断是否需要主动探测，
    // 避开每次借出都发一次 PING 的开销。
    int health_check_interval_ms{30000};
    bool enable_auto_grow{true};
};

class RedisConnectionPool;

// RAII 连接守卫，析构时自动归还连接
//
// 注意 pool_ 是**引用**而不是指针（与 MySqlConnGuard 不同）：
// 因此它不可能表达「空 guard」。Acquire 失败时返回的是
// pool_ 有效但 ctx_ == nullptr 的守卫，调用方必须检查 get()。
class RedisConnGuard {
public:
    // 持有连接的生命周期守卫。
    RedisConnGuard(RedisConnectionPool& pool, redisContext* ctx);
    // 禁拷贝：拷贝会导致同一 context 被归还两次，进而被两个
    // 线程同时使用——对 Redis 而言这会直接造成协议流错乱。
    RedisConnGuard(const RedisConnGuard&) = delete;
    RedisConnGuard& operator=(const RedisConnGuard&) = delete;
    // 移动后把源的 ctx_ 置空，转移归还责任。
    RedisConnGuard(RedisConnGuard&& other) noexcept : pool_(other.pool_), ctx_(other.ctx_) {
        other.ctx_ = nullptr;
    }
    // 析构时自动归还连接。
    ~RedisConnGuard();

    // 可能为 nullptr（Acquire 超时/池已停），用前必须判空。
    redisContext* get() const { return ctx_; }

private:
    RedisConnectionPool& pool_;
    redisContext* ctx_;
};

// 线程安全的 Redis 连接池，支持自动扩容与空闲回收
//
// RedisStore 的所有方法都遵循同一模式：
//   auto guard = pool_->Acquire();
//   redisContext* ctx = guard.get();
//   if (!ctx) return false;
// 因此任何 Redis 异常都表现为方法返回 false，而不是抛异常或崩溃。
class RedisConnectionPool {
public:
    RedisConnectionPool() = default;
    ~RedisConnectionPool();

    // 初始化连接池，并按配置预热连接。
    bool Init(const RedisConfig& cfg);

    // 获取连接，支持阻塞等待。默认 3s，超时返回 ctx_ 为空的守卫。
    RedisConnGuard Acquire(int timeout_ms = 3000);
    void Stop();

private:
    // *Unlocked 后缀同样表示「调用时不得持 mutex_」（建连会阻塞）。
    redisContext* CreateContextUnlocked();
    // 同时回填 last_used：调用方需要它来判断是否超过
    // health_check_interval_ms 而需要探测。
    bool PopIdleUnlocked(
        redisContext** ctx,
        std::chrono::steady_clock::time_point* last_used);
    void CleanupIdleUnlocked();
    // 双重指针：探测失败时需要把 ctx 置空并告知调用方丢弃。
    bool Validate(redisContext** ctx);
    void Release(redisContext* ctx);

    friend class RedisConnGuard;

    std::mutex mutex_;
    std::condition_variable cv_;
    // steady_clock 而不是 system_clock：理由同 MySqlConnectionPool，
    // 避开 NTP 校时跳变把所有连接瞬间判为过期。
    struct ConnEntry {
        redisContext* ctx{nullptr};
        std::chrono::steady_clock::time_point last_used;
    };
    std::deque<ConnEntry> idle_;
    int total_conns_{0};   // 已借出 + 空闲，用于卡 max_pool_size
    bool stopping_{false};
    bool inited_{false};
    RedisConfig cfg_;
};

}  // namespace sparkpush
