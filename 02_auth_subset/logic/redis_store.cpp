#include "redis_store.h"

#include "logging.h"

namespace sparkpush {

// 写入 token -> user_id 映射
bool RedisStore::SetToken(const std::string& token, int64_t user_id,
                          int ttl_seconds) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    redisReply* reply =
        (redisReply*)redisCommand(ctx, "SETEX token:%s %d %lld", token.c_str(),
                                  ttl_seconds, static_cast<long long>(user_id));
    if (!reply) {
        LOG_ERROR << "Redis SETEX token failed";
        return false;
    }
    LOG_INFO << "SetToken " << token
             << ", reply type: " << std::to_string(reply->type);
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

// 通过 token 获取 user_id
bool RedisStore::GetUserIdByToken(const std::string& token, int64_t* user_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    LOG_INFO << "GetUserIdByToken " << token;

    redisReply* reply =
        (redisReply*)redisCommand(ctx, "GET token:%s", token.c_str());
    if (!reply) {
        LOG_ERROR << "Redis GET token failed";
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *user_id = std::stoll(reply->str);
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

// 记录用户路由 comet
bool RedisStore::AddRoute(int64_t user_id, const std::string& comet_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SADD route:user:%lld %s", static_cast<long long>(user_id),
        comet_id.c_str());
    if (!reply) {
        LOG_ERROR << "Redis SADD route failed";
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

// 移除用户路由
bool RedisStore::RemoveRoute(int64_t user_id, const std::string& comet_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SREM route:user:%lld %s", static_cast<long long>(user_id),
        comet_id.c_str());
    if (!reply) {
        LOG_ERROR << "Redis SREM route failed";
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

}  // namespace sparkpush
