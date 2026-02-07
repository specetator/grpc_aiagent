#include "redis_pool.h"

#include "logging.h"

#include <algorithm>

namespace sparkpush {

RedisConnGuard::RedisConnGuard(RedisConnectionPool& pool, redisContext* ctx)
    : pool_(pool), ctx_(ctx) {}

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
  if (!cfg_.password.empty()) {
    redisReply* reply =
        (redisReply*)redisCommand(ctx, "AUTH %s", cfg_.password.c_str());
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
      LogError("redis AUTH failed");
      if (reply) freeReplyObject(reply);
      redisFree(ctx);
      return nullptr;
    }
    freeReplyObject(reply);
  }
  if (cfg_.db != 0) {
    redisReply* reply =
        (redisReply*)redisCommand(ctx, "SELECT %d", cfg_.db);
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
      LogError("redis SELECT failed");
      if (reply) freeReplyObject(reply);
      redisFree(ctx);
      return nullptr;
    }
    freeReplyObject(reply);
  }
  struct timeval rw;
  rw.tv_sec = cfg_.rw_timeout_ms / 1000;
  rw.tv_usec = (cfg_.rw_timeout_ms % 1000) * 1000;
  redisSetTimeout(ctx, rw);
  return ctx;
}

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

RedisConnGuard RedisConnectionPool::Acquire(int timeout_ms) {
  std::unique_lock<std::mutex> lock(mutex_);
  redisContext* ctx = nullptr;
  auto wait_timeout = std::chrono::milliseconds(timeout_ms);

  while (!stopping_) {
    ctx = PopIdleUnlocked();
    if (ctx) break;
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

  // 热路径优化：跳过PING验证直接返回连接
  // 如果连接失效会在实际使用时发现并处理
  if (!ctx || stopping_) {
    return RedisConnGuard(*this, nullptr);
  }
  return RedisConnGuard(*this, ctx);
}

redisContext* RedisConnectionPool::PopIdleUnlocked() {
  CleanupIdleUnlocked();
  if (idle_.empty()) return nullptr;
  redisContext* ctx = idle_.front().ctx;
  idle_.pop_front();
  return ctx;
}

void RedisConnectionPool::CleanupIdleUnlocked() {
  if (cfg_.idle_timeout_ms <= 0) return;
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

bool RedisConnectionPool::Validate(redisContext** ctx) {
  if (!ctx || !(*ctx)) return false;
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

void RedisConnectionPool::Release(redisContext* ctx) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (stopping_) {
    if (ctx) {
      redisFree(ctx);
      if (total_conns_ > 0) --total_conns_;
    }
    return;
  }
  idle_.push_back({ctx, std::chrono::steady_clock::now()});
  cv_.notify_one();
}

}  // namespace sparkpush
