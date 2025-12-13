// ============================================================================
// Redis 存储实现
//
// 使用 hiredis 库操作 Redis，通过连接池管理连接
// ============================================================================
#include "redis_store.h"

#include <unordered_set>

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

bool RedisStore::UpsertUserConnection(int64_t user_id,
                                      const std::string& comet_id,
                                      const std::string& conn_id,
                                      int64_t ttl_ms) {
    if (!pool_ || comet_id.empty() || conn_id.empty()) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    std::string key = "user_connections:" + std::to_string(user_id);
    std::string field = comet_id + ":" + conn_id;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HSET %s %s {}",
                                                  key.c_str(), field.c_str());
    if (!reply) {
        LOG_ERROR << "Redis HSET user_connections failed";
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    if (!ok) return false;

    if (ttl_ms > 0) {
        reply = (redisReply*)redisCommand(ctx, "PEXPIRE %s %lld", key.c_str(),
                                          static_cast<long long>(ttl_ms));
        if (reply) freeReplyObject(reply);
    }
    return true;
}

bool RedisStore::RemoveUserConnection(int64_t user_id,
                                      const std::string& comet_id,
                                      const std::string& conn_id) {
    if (!pool_ || comet_id.empty() || conn_id.empty()) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key = "user_connections:" + std::to_string(user_id);
    std::string field = comet_id + ":" + conn_id;
    redisReply* reply = (redisReply*)redisCommand(ctx, "HDEL %s %s",
                                                  key.c_str(), field.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::GetUserConnectionComets(int64_t user_id,
                                         std::vector<std::string>* comets) {
    if (!pool_ || !comets) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key = "user_connections:" + std::to_string(user_id);
    redisReply* reply = (redisReply*)redisCommand(ctx, "HKEYS %s", key.c_str());
    if (!reply) {
        LOG_ERROR << "Redis HKEYS user_connections failed";
        return false;
    }
    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY) {
        ok = true;
        std::unordered_set<std::string> uniq;
        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* e = reply->element[i];
            if (e->type == REDIS_REPLY_STRING && e->str) {
                std::string field = e->str;
                auto pos = field.find(':');
                if (pos != std::string::npos) {
                    uniq.insert(field.substr(0, pos));
                }
            }
        }
        comets->assign(uniq.begin(), uniq.end());
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
        ctx, "SET read:%s:%lld %lld", session_id.c_str(),
        static_cast<long long>(user_id), static_cast<long long>(read_seq));
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
    redisReply* reply =
        (redisReply*)redisCommand(ctx, "GET read:%s:%lld", session_id.c_str(),
                                  static_cast<long long>(user_id));
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

bool RedisStore::ClearUnreadCount(int64_t user_id,
                                  const std::string& session_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key =
        "unread_count:" + std::to_string(user_id) + ":" + session_id;
    redisReply* reply = (redisReply*)redisCommand(ctx, "DEL %s", key.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::IncrUnreadCount(int64_t user_id, const std::string& session_id,
                                 int64_t delta, int64_t* new_value) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key =
        "unread_count:" + std::to_string(user_id) + ":" + session_id;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "INCRBY %s %lld", key.c_str(), static_cast<long long>(delta));
    if (!reply) return false;
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        if (new_value) *new_value = reply->integer;
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::GetUnreadCount(int64_t user_id, const std::string& session_id,
                                int64_t* value) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key =
        "unread_count:" + std::to_string(user_id) + ":" + session_id;
    redisReply* reply = (redisReply*)redisCommand(ctx, "GET %s", key.c_str());
    if (!reply) return false;
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        try {
            if (value) *value = std::stoll(reply->str);
            ok = true;
        } catch (...) {
        }
    } else if (reply->type == REDIS_REPLY_NIL) {
        if (value) *value = 0;
        ok = true;
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::AddUserSession(int64_t user_id,
                                const std::string& session_id) {
    if (!pool_ || session_id.empty()) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key = "user_sessions:" + std::to_string(user_id);
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SADD %s %s", key.c_str(), session_id.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::ListUserSessions(int64_t user_id,
                                  std::vector<std::string>* sessions) {
    if (!pool_ || !sessions) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key = "user_sessions:" + std::to_string(user_id);
    redisReply* reply =
        (redisReply*)redisCommand(ctx, "SMEMBERS %s", key.c_str());
    if (!reply) return false;
    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY) {
        ok = true;
        sessions->clear();
        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* e = reply->element[i];
            if (e->type == REDIS_REPLY_STRING && e->str) {
                sessions->emplace_back(e->str);
            }
        }
    }
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::SetUserSessionMeta(int64_t user_id,
                                    const std::string& session_id,
                                    const std::string& last_msg_id,
                                    int64_t last_msg_seq,
                                    const std::string& last_msg_type,
                                    int64_t last_time_ms,
                                    const std::string& last_preview) {
    if (!pool_ || session_id.empty()) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key =
        "user_session_meta:" + std::to_string(user_id) + ":" + session_id;
    redisReply* reply = (redisReply*)redisCommand(
        ctx,
        "HMSET %s last_msg_id %s last_msg_seq %lld last_msg_type %s "
        "last_time_ms %lld last_preview %s",
        key.c_str(), last_msg_id.c_str(), static_cast<long long>(last_msg_seq),
        last_msg_type.c_str(), static_cast<long long>(last_time_ms),
        last_preview.c_str());
    if (!reply) return false;
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

bool RedisStore::GetUserSessionMeta(
    int64_t user_id, const std::string& session_id, std::string* last_msg_id,
    int64_t* last_msg_seq, std::string* last_msg_type, int64_t* last_time_ms,
    std::string* last_preview) {
    if (!pool_ || session_id.empty()) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    std::string key =
        "user_session_meta:" + std::to_string(user_id) + ":" + session_id;
    redisReply* reply =
        (redisReply*)redisCommand(ctx,
                                  "HMGET %s last_msg_id last_msg_seq "
                                  "last_msg_type last_time_ms last_preview",
                                  key.c_str());
    if (!reply) return false;
    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY && reply->elements == 5) {
        ok = true;
        if (last_msg_id && reply->element[0]->type == REDIS_REPLY_STRING) {
            *last_msg_id = reply->element[0]->str;
        }
        if (last_msg_seq && reply->element[1]->type == REDIS_REPLY_STRING) {
            try {
                *last_msg_seq = std::stoll(reply->element[1]->str);
            } catch (...) {
            }
        }
        if (last_msg_type && reply->element[2]->type == REDIS_REPLY_STRING) {
            *last_msg_type = reply->element[2]->str;
        }
        if (last_time_ms && reply->element[3]->type == REDIS_REPLY_STRING) {
            try {
                *last_time_ms = std::stoll(reply->element[3]->str);
            } catch (...) {
            }
        }
        if (last_preview && reply->element[4]->type == REDIS_REPLY_STRING) {
            *last_preview = reply->element[4]->str;
        }
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

    std::string key =
        "msgid:" + std::to_string(user1) + ":" + std::to_string(user2); //范例： msgid:1:2
    redisReply* reply = (redisReply*)redisCommand(ctx, "INCR %s", key.c_str());
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
    redisReply* reply = (redisReply*)redisCommand(ctx, "INCR %s", key.c_str());
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
