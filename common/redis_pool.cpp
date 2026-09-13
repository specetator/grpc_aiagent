#include "redis_pool.h"

#include "logging.h"
#include "metrics.h"

#include <algorithm>

namespace sparkpush {

// 构造守卫，记录池与连接，析构时自动归还。
RedisConnGuard::RedisConnGuard(RedisConnectionPool& pool, redisContext* ctx)
    : pool_(pool), ctx_(ctx) {}

// 析构时将连接放回池中。
RedisConnGuard::~RedisConnGuard() {
    if (ctx_) {
        pool_.Release(ctx_);
    }
}

RedisConnectionPool::~RedisConnectionPool() {
    std::unique_lock<std::mutex> lock(mutex_);
    stopping_ = true;
    cv_.notify_all();
    while (!idle_.empty()) {
        redisFree(idle_.front().ctx);
        idle_.pop_front();
    }
    total_conns_ = 0;
}

// 创建 redisContext，包含鉴权、选择 DB 以及超时设置。
redisContext* RedisConnectionPool::CreateContextUnlocked() {
    struct timeval tv;
    tv.tv_sec = cfg_.connect_timeout_ms / 1000;
    tv.tv_usec = (cfg_.connect_timeout_ms % 1000) * 1000;
    redisContext* ctx =
        redisConnectWithTimeout(cfg_.host.c_str(), cfg_.port, tv);
    if (!ctx || ctx->err) {
        if (ctx) {
            LOG_ERROR << "redisConnect failed: " << ctx->errstr;
            redisFree(ctx);
        } else {
            LOG_ERROR << "redisConnect failed: null context";
        }
        return nullptr;
    }

    // 鉴权
    if (!cfg_.password.empty()) {
        redisReply* reply =
            (redisReply*)redisCommand(ctx, "AUTH %s", cfg_.password.c_str());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG_ERROR << "redis AUTH failed";
            if (reply) {
                freeReplyObject(reply);
            }
            redisFree(ctx);
            return nullptr;
        }
        freeReplyObject(reply);
    }

    // 选择 DB
    if (cfg_.db != 0) {
        redisReply* reply =
            (redisReply*)redisCommand(ctx, "SELECT %d", cfg_.db);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LOG_ERROR << "redis SELECT failed";
            if (reply) {
                freeReplyObject(reply);
            }
            redisFree(ctx);
            return nullptr;
        }
        freeReplyObject(reply);
    }

    // 读写超时
    struct timeval rw;
    rw.tv_sec = cfg_.rw_timeout_ms / 1000;
    rw.tv_usec = (cfg_.rw_timeout_ms % 1000) * 1000;
    redisSetTimeout(ctx, rw);
    return ctx;
}

// 初始化连接池，预创建最小数量的连接。
bool RedisConnectionPool::Init(const RedisConfig& cfg) {
    std::unique_lock<std::mutex> lock(mutex_);
    cfg_ = cfg;
    if (cfg_.min_pool_size <= 0) {
        cfg_.min_pool_size = cfg_.pool_size > 0 ? cfg_.pool_size : 1;
    }
    if (cfg_.max_pool_size <= 0) {
        cfg_.max_pool_size = std::max(cfg_.min_pool_size, cfg_.pool_size);
    }
    idle_.clear();
    total_conns_ = 0;

    for (int i = 0; i < cfg_.min_pool_size; ++i) {
        redisContext* ctx = CreateContextUnlocked();
        if (!ctx) {
            while (!idle_.empty()) {
                redisFree(idle_.front().ctx);
                idle_.pop_front();
            }
            total_conns_ = 0;
            return false;
        }
        idle_.push_back({ctx, std::chrono::steady_clock::now()});
        ++total_conns_;
        MetricsRegistry::Instance().Increment(
            "spark_push_redis_pool_connections_created_total");
    }
    stopping_ = false;
    return true;
}

// 获取连接，支持超时等待与自动扩容。
RedisConnGuard RedisConnectionPool::Acquire(int timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    redisContext* ctx = nullptr;
    std::chrono::steady_clock::time_point last_used{};
    bool needs_validation = false;
    auto wait_timeout = std::chrono::milliseconds(timeout_ms);

    while (!stopping_) {
        if (PopIdleUnlocked(&ctx, &last_used)) {
            needs_validation = cfg_.health_check_interval_ms > 0 &&
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - last_used).count() >=
                    cfg_.health_check_interval_ms;
            break;
        }

        if (cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size) {
            ctx = CreateContextUnlocked();
            if (ctx) {
                ++total_conns_;
                needs_validation = false;
                MetricsRegistry::Instance().Increment(
                    "spark_push_redis_pool_connections_created_total");
                break;
            }
        }

        if (timeout_ms < 0) {
            cv_.wait(lock);
        } else if (timeout_ms == 0) {
            MetricsRegistry::Instance().Increment(
                "spark_push_redis_pool_acquire_timeout_total");
            return RedisConnGuard(*this, nullptr);
        } else {
            if (cv_.wait_for(lock, wait_timeout) == std::cv_status::timeout) {
                MetricsRegistry::Instance().Increment(
                    "spark_push_redis_pool_acquire_timeout_total");
                return RedisConnGuard(*this, nullptr);
            }
        }
    }

    while (true) {
        if (!ctx || stopping_) {
            return RedisConnGuard(*this, nullptr);
        }
        lock.unlock();
        bool valid = true;
        if (needs_validation) {
            MetricsRegistry::Instance().Increment(
                "spark_push_redis_pool_health_check_total");
            valid = Validate(&ctx);
            MetricsRegistry::Instance().Increment(
                valid ? "spark_push_redis_pool_health_check_success_total"
                      : "spark_push_redis_pool_health_check_failed_total");
        }
        if (valid) {
            return RedisConnGuard(*this, ctx);
        }
        {
            std::lock_guard<std::mutex> relock(mutex_);
            if (total_conns_ > 0) {
                --total_conns_;
            }
        }
        lock.lock();
        needs_validation = false;
        if (PopIdleUnlocked(&ctx, &last_used)) {
            needs_validation = cfg_.health_check_interval_ms > 0 &&
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - last_used).count() >=
                    cfg_.health_check_interval_ms;
        }
        if (!ctx && cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size) {
            ctx = CreateContextUnlocked();
            if (ctx) {
                ++total_conns_;
                MetricsRegistry::Instance().Increment(
                    "spark_push_redis_pool_connections_created_total");
            }
        }
    }
}

// 弹出一个空闲连接，清理过期连接后再返回。
bool RedisConnectionPool::PopIdleUnlocked(
    redisContext** ctx,
    std::chrono::steady_clock::time_point* last_used) {
    if (!ctx || !last_used) return false;
    *ctx = nullptr;
    CleanupIdleUnlocked();
    if (idle_.empty()) {
        return false;
    }
    *ctx = idle_.front().ctx;
    *last_used = idle_.front().last_used;
    idle_.pop_front();
    return *ctx != nullptr;
}

// 清理超过空闲时间的连接，保留最小池大小。
void RedisConnectionPool::CleanupIdleUnlocked() {
    if (cfg_.idle_timeout_ms <= 0) {
        return;
    }

    auto now = std::chrono::steady_clock::now();
    while (!idle_.empty()) {
        auto& entry = idle_.front();
        auto idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           now - entry.last_used)
                           .count();
        if (idle_ms < cfg_.idle_timeout_ms ||
            total_conns_ <= cfg_.min_pool_size) {
            break;
        }
        redisFree(entry.ctx);
        idle_.pop_front();
        --total_conns_;
        MetricsRegistry::Instance().Increment(
            "spark_push_redis_pool_idle_closed_total");
    }
}

// 校验连接是否可用，失败则释放并置空。
bool RedisConnectionPool::Validate(redisContext** ctx) {
    if (!ctx || !(*ctx)) {
        return false;
    }
    redisReply* reply = (redisReply*)redisCommand(*ctx, "PING");
    if (!reply) {
        redisFree(*ctx);
        *ctx = nullptr;
        return false;
    }
    bool ok = reply->type == REDIS_REPLY_STATUS &&
              reply->str &&
              std::string(reply->str) == "PONG";
    freeReplyObject(reply);
    if (!ok) {
        redisFree(*ctx);
        *ctx = nullptr;
    }
    return ok;
}

// 归还连接到空闲队列；停止阶段直接释放。
void RedisConnectionPool::Release(redisContext* ctx) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (stopping_) {
        if (ctx) {
            redisFree(ctx);
            if (total_conns_ > 0) {
                --total_conns_;
            }
        }
        return;
    }
    // 业务命令已发现 I/O 错误时，不把断链连接放回池中反复失败。
    if (!ctx || ctx->err != 0) {
        if (ctx) redisFree(ctx);
        if (total_conns_ > 0) --total_conns_;
        MetricsRegistry::Instance().Increment(
            "spark_push_redis_pool_connections_discarded_total");
        cv_.notify_one();
        return;
    }
    idle_.push_back({ctx, std::chrono::steady_clock::now()});
    cv_.notify_one();
}

}  // namespace sparkpush
