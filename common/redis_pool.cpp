#include "redis_pool.h"

#include "logging.h"

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
            LogError(std::string("redisConnect failed: ") + ctx->errstr);
            redisFree(ctx);
        } else {
            LogError("redisConnect failed: null context");
        }
        return nullptr;
    }

    // 鉴权
    if (!cfg_.password.empty()) {
        redisReply* reply =
            (redisReply*)redisCommand(ctx, "AUTH %s", cfg_.password.c_str());
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            LogError("redis AUTH failed");
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
            LogError("redis SELECT failed");
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
    }
    stopping_ = false;
    return true;
}

// 获取连接，支持超时等待与自动扩容。
RedisConnGuard RedisConnectionPool::Acquire(int timeout_ms) {
    std::unique_lock<std::mutex> lock(mutex_);
    redisContext* ctx = nullptr;
    auto wait_timeout = std::chrono::milliseconds(timeout_ms);

    while (!stopping_) {
        ctx = PopIdleUnlocked();
        if (ctx) {
            break;
        }

        if (cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size) {
            ctx = CreateContextUnlocked();
            if (ctx) {
                ++total_conns_;
                break;
            }
        }

        if (timeout_ms < 0) {
            cv_.wait(lock);
        } else if (timeout_ms == 0) {
            return RedisConnGuard(*this, nullptr);
        } else {
            if (cv_.wait_for(lock, wait_timeout) == std::cv_status::timeout) {
                return RedisConnGuard(*this, nullptr);
            }
        }
    }

    while (true) {
        if (!ctx || stopping_) {
            return RedisConnGuard(*this, nullptr);
        }
        lock.unlock();
        if (Validate(&ctx)) {
            return RedisConnGuard(*this, ctx);
        }
        {
            std::lock_guard<std::mutex> relock(mutex_);
            if (total_conns_ > 0) {
                --total_conns_;
            }
        }
        lock.lock();
        ctx = PopIdleUnlocked();
        if (!ctx && cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size) {
            ctx = CreateContextUnlocked();
            if (ctx) {
                ++total_conns_;
            }
        }
    }
}

// 弹出一个空闲连接，清理过期连接后再返回。
redisContext* RedisConnectionPool::PopIdleUnlocked() {
    CleanupIdleUnlocked();
    if (idle_.empty()) {
        return nullptr;
    }
    redisContext* ctx = idle_.front().ctx;
    idle_.pop_front();
    return ctx;
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
    idle_.push_back({ctx, std::chrono::steady_clock::now()});
    cv_.notify_one();
}

}  // namespace sparkpush
