// ============================================================================
// Redis 存储实现
// 
// 使用 hiredis 库操作 Redis，通过连接池管理连接
// ============================================================================
#include "redis_store.h"

#include "logging.h"

namespace sparkpush {

// 存储 token 到 Redis
// 使用 SETEX 命令：原子操作，同时设置键值和过期时间
// Key 格式：token:<token_string>
// Value：user_id（数字）
bool RedisStore::SetToken(const std::string& token, int64_t user_id,
                          int ttl_seconds) {
    if (!pool_) return false;
    
    // 从连接池获取 Redis 连接
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    // 执行 SETEX 命令：设置键值并指定过期时间（秒）
    redisReply* reply =
        (redisReply*)redisCommand(ctx, "SETEX token:%s %d %lld", token.c_str(),
                                  ttl_seconds, static_cast<long long>(user_id));
    if (!reply) {
        LOG_ERROR << "Redis SETEX token failed";
        return false;
    }
    
    LOG_INFO << "SetToken " << token
             << ", reply type: " << std::to_string(reply->type);
    
    // 检查回复类型，非错误即为成功
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

// 通过 token 查询 user_id
// 使用 GET 命令读取 token 对应的 user_id
// 如果 token 不存在或已过期，GET 返回 nil
bool RedisStore::GetUserIdByToken(const std::string& token, int64_t* user_id) {
    if (!pool_) return false;
    
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    
    LOG_INFO << "GetUserIdByToken " << token;

    // 执行 GET 命令
    redisReply* reply =
        (redisReply*)redisCommand(ctx, "GET token:%s", token.c_str());
    if (!reply) {
        LOG_ERROR << "Redis GET token failed";
        return false;
    }
    
    // 检查回复类型：REDIS_REPLY_STRING 表示找到了值
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *user_id = std::stoll(reply->str);  // 将字符串转为数字
        ok = true;
    }
    
    freeReplyObject(reply);
    return ok;
}

// 添加用户路由信息
// 使用 SADD 命令将 comet_id 添加到用户的路由集合
// Key 格式：route:user:<user_id>
// Value：Set(comet_id1, comet_id2, ...)
// 用途：消息推送时查询用户在哪些 comet 节点上有连接
bool RedisStore::AddRoute(int64_t user_id, const std::string& comet_id) {
    if (!pool_) return false;
    
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    
    // 执行 SADD 命令：向集合添加元素，自动去重
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

// 移除用户路由信息
// 使用 SREM 命令从用户的路由集合中删除指定的 comet_id
// 用途：用户断开连接时清理路由信息
bool RedisStore::RemoveRoute(int64_t user_id, const std::string& comet_id) {
    if (!pool_) return false;
    
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    
    // 执行 SREM 命令：从集合删除元素
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
