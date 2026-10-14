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
//
// 顺序有讲究：必须先设 rw 超时再发 AUTH/SELECT。
// 若不先设，AUTH 本身就可能无限阻塞（Redis 卡住时），
// 建连函数就再也回不来了。
//
// 命名里的 Unlocked 同样表示「调用时不得持 mutex_」：
// 建连与 AUTH 都是阻塞网络操作，持锁会把整个池卡死。
redisContext* RedisConnectionPool::CreateContextUnlocked() {
    struct timeval tv;
    tv.tv_sec = cfg_.connect_timeout_ms / 1000;
    tv.tv_usec = (cfg_.connect_timeout_ms % 1000) * 1000;
    redisContext* ctx =
        redisConnectWithTimeout(cfg_.host.c_str(), cfg_.port, tv);
    // hiredis 的一个坑：连接失败时它**仍会返回非空 ctx**，错误放在
    // ctx->err 里。因此必须同时检查两者，只看 nullptr 会漏掉大部分失败。
    if (!ctx || ctx->err) {
        if (ctx) {
            LOG_ERROR << "redisConnect failed: " << ctx->errstr;
            redisFree(ctx);
        } else {
            LOG_ERROR << "redisConnect failed: null context";
        }
        return nullptr;
    }

    struct timeval rw;
    rw.tv_sec = cfg_.rw_timeout_ms / 1000;
    rw.tv_usec = (cfg_.rw_timeout_ms % 1000) * 1000;
    if (redisSetTimeout(ctx, rw) != REDIS_OK) { redisFree(ctx); return nullptr; }
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
    return ctx;
}

// 初始化连接池，预创建最小数量的连接。
bool RedisConnectionPool::Init(const RedisConfig& cfg) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (inited_) return !stopping_;
    cfg_ = cfg;
    if (cfg_.min_pool_size <= 0) {
        cfg_.min_pool_size = cfg_.pool_size > 0 ? cfg_.pool_size : 1;
    }
    if (cfg_.max_pool_size <= 0) {
        cfg_.max_pool_size = std::max(cfg_.min_pool_size, cfg_.pool_size);
    }
    cfg_.max_pool_size = std::max(cfg_.max_pool_size, cfg_.min_pool_size);
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
    inited_ = true;
    return true;
}

// 获取连接，支持超时等待与自动扩容。
RedisConnGuard RedisConnectionPool::Acquire(int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max(0, timeout_ms));
    std::unique_lock<std::mutex> lock(mutex_);
    while (inited_ && !stopping_) {
        redisContext* context = nullptr;
        std::chrono::steady_clock::time_point last_used{};
        bool validate = false;
        if (PopIdleUnlocked(&context, &last_used)) {
            validate = cfg_.health_check_interval_ms > 0 &&
                std::chrono::steady_clock::now() - last_used >= std::chrono::milliseconds(cfg_.health_check_interval_ms);
        } else if (total_conns_ < cfg_.min_pool_size ||
                   (cfg_.enable_auto_grow && total_conns_ < cfg_.max_pool_size)) {
            ++total_conns_;
            lock.unlock();
            context = CreateContextUnlocked();
            lock.lock();
            if (!context) --total_conns_;
            else MetricsRegistry::Instance().Increment("spark_push_redis_pool_connections_created_total");
        }
        if (context) {
            lock.unlock();
            if (validate) MetricsRegistry::Instance().Increment("spark_push_redis_pool_health_check_total");
            const bool valid = !validate || Validate(&context);
            if (validate) MetricsRegistry::Instance().Increment(valid
                ? "spark_push_redis_pool_health_check_success_total"
                : "spark_push_redis_pool_health_check_failed_total");
            lock.lock();
            if (valid && !stopping_) return RedisConnGuard(*this, context);
            if (context) redisFree(context);
            --total_conns_;
            cv_.notify_all();
        }
        if (stopping_ || timeout_ms == 0 || (timeout_ms > 0 && std::chrono::steady_clock::now() >= deadline)) break;
        auto wake = std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
        if (timeout_ms > 0) wake = std::min(wake, deadline);
        cv_.wait_until(lock, wake);
    }
    MetricsRegistry::Instance().Increment("spark_push_redis_pool_acquire_timeout_total");
    return RedisConnGuard(*this, nullptr);
}

void RedisConnectionPool::Stop() {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_ = true;
    cv_.notify_all();
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
// 用 PING 探测连接是否仍可用。
//
// 为何需要：Redis 服务端会在 timeout 后关连接，TCP 半开时客户端
// 并不知情。不探测就会把一个死 context 交给 RedisStore，而 hiredis
// 在已断开的 context 上发命令会持续返回错误。
//
// 失败时必须 redisFree 并把 *ctx 置空：调用方（Acquire）靠
// 「ctx 为空」判断该连接已不可用，若不置空它会去 free 一个野指针。
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
//
// 关键分支：ctx->err != 0 时**不放回池**而是直接销毁。
// 一个已出现 I/O 错误的 context 在 hiredis 里是永久失效的（后续
// 命令都会立即返回错误）。若放回池，每次借出都会失败，
// 表现为「Redis 明明恢复了但服务一直报错」，直到池被逐渐洗干。
// 因此这里主动丢弃并 notify，让等待者去建新连接。
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
