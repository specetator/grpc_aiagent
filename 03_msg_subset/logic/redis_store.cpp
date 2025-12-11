// ============================================================================
// Redis 存储实现
// 
// 使用 hiredis 库操作 Redis，通过连接池管理连接
// ============================================================================
#include "redis_store.h"

#include "logging.h"

namespace sparkpush {

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

bool RedisStore::GetUserRoutes(int64_t user_id,
                               std::vector<std::string>* comets) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SMEMBERS route:user:%lld", static_cast<long long>(user_id));
    if (!reply) {
        LOG_ERROR << "Redis SMEMBERS route failed";
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY) {
        ok = true;
        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* e = reply->element[i];
            if (e->type == REDIS_REPLY_STRING && e->str) {
                comets->push_back(e->str);
            }
        }
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::SetSessionLastSeq(const std::string& session_id,
                                   int64_t last_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SET session:last_seq:%s %lld", session_id.c_str(),
        static_cast<long long>(last_seq));
    if (!reply) {
        LOG_ERROR << "Redis SET session:last_seq failed";
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::GetSessionLastSeq(const std::string& session_id,
                                   int64_t* last_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "GET session:last_seq:%s", session_id.c_str());
    if (!reply) {
        LOG_ERROR << "Redis GET session:last_seq failed";
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *last_seq = std::stoll(reply->str);
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::SetUserReadSeq(int64_t user_id, const std::string& session_id,
                                int64_t read_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    
    // Key 格式：read:{session_id}:{user_id}
    // 例如：read:s_1_2:2 表示用户2在会话s_1_2中的已读位置
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SET read:%s:%lld %lld",
        session_id.c_str(), static_cast<long long>(user_id),
        static_cast<long long>(read_seq));
    if (!reply) {
        LOG_ERROR << "Redis SET read seq failed for session=" << session_id
                  << " user=" << user_id;
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::GetUserReadSeq(int64_t user_id, const std::string& session_id,
                                int64_t* read_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    
    // Key 格式：read:{session_id}:{user_id}
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "GET read:%s:%lld",
        session_id.c_str(), static_cast<long long>(user_id));
    if (!reply) {
        LOG_ERROR << "Redis GET read seq failed for session=" << session_id
                  << " user=" << user_id;
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *read_seq = std::stoll(reply->str);
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::AddRoomComet(int64_t room_id, const std::string& comet_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SADD room:comets:%lld %s", static_cast<long long>(room_id),
        comet_id.c_str());
    if (!reply) {
        LOG_ERROR << "Redis SADD room:comets failed";
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::RemoveRoomComet(int64_t room_id, const std::string& comet_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SREM room:comets:%lld %s", static_cast<long long>(room_id),
        comet_id.c_str());
    if (!reply) {
        LOG_ERROR << "Redis SREM room:comets failed";
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::GetRoomComets(int64_t room_id,
                               std::vector<std::string>* comets) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SMEMBERS room:comets:%lld", static_cast<long long>(room_id));
    if (!reply) {
        LOG_ERROR << "Redis SMEMBERS room:comets failed";
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY) {
        ok = true;
        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* e = reply->element[i];
            if (e->type == REDIS_REPLY_STRING && e->str) {
                comets->push_back(e->str);
            }
        }
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::SetRoomOnlineCount(int64_t room_id, int64_t count) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SET room:online_count:%lld %lld", static_cast<long long>(room_id),
        static_cast<long long>(count));
    if (!reply) {
        LOG_ERROR << "Redis SET room:online_count failed";
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::GetRoomOnlineCount(int64_t room_id, int64_t* count) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "GET room:online_count:%lld", static_cast<long long>(room_id));
    if (!reply) {
        LOG_ERROR << "Redis GET room:online_count failed";
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        *count = std::stoll(reply->str);
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::IncrRoomOnlineCount(int64_t room_id, int64_t delta,
                                     int64_t* new_value) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "INCRBY room:online_count:%lld %lld",
        static_cast<long long>(room_id), static_cast<long long>(delta));
    if (!reply) {
        LOG_ERROR << "Redis INCRBY room:online_count failed";
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        if (new_value) {
            *new_value = reply->integer;
        }
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::IncrRoomCometCount(int64_t room_id,
                                    const std::string& comet_id, int64_t delta,
                                    int64_t* new_value) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key =
        "room:comet_count:" + std::to_string(room_id) + ":" + comet_id;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "INCRBY %s %lld", key.c_str(), static_cast<long long>(delta));
    if (!reply) {
        LOG_ERROR << "Redis INCRBY room:comet_count failed";
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        if (new_value) {
            *new_value = reply->integer;
        }
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::NextSingleMsgId(int64_t user1, int64_t user2, int64_t* seq) {
    if (!seq) return false;
    if (user1 <= 0 || user2 <= 0) return false;
    if (user1 > user2) std::swap(user1, user2);
    if (!pool_) return false;

    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    std::string key = "msgid:" + std::to_string(user1) + ":" + std::to_string(user2);
    redisReply* reply =
        (redisReply*)redisCommand(ctx, "INCR %s", key.c_str());
    if (!reply) {
        LOG_ERROR << "Redis INCR single msg_id failed, key: " << key;
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        *seq = reply->integer;
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::NextGroupMsgId(int64_t group_id, int64_t* seq) {
    if (!seq) return false;
    if (group_id <= 0) return false;
    if (!pool_) return false;

    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    std::string key = "msgid_group:" + std::to_string(group_id);
    redisReply* reply =
        (redisReply*)redisCommand(ctx, "INCR %s", key.c_str());
    if (!reply) {
        LOG_ERROR << "Redis INCR group msg_id failed, key: " << key;
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        *seq = reply->integer;
    }
    freeReplyObject(reply);
    return ok;
}

}  // namespace sparkpush
