#include "redis_store.h"

#include <algorithm>
#include <unordered_set>

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
    LOG_INFO << "SetToken succeeded for user_id=" << std::to_string(user_id)
             << ", reply type=" << std::to_string(reply->type);
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    if (ok) {
        // 反向索引让注销/封禁可以 O(tokens_of_user) 撤销全部会话。
        // 旧版本只写 token:<token>，RevokeUserTokens 仍会 SCAN 兜底。
        redisReply* index_reply = static_cast<redisReply*>(redisCommand(
            ctx, "SADD user:tokens:%lld %s", static_cast<long long>(user_id),
            token.c_str()));
        if (!index_reply || index_reply->type == REDIS_REPLY_ERROR) {
            LOG_WARN << "Redis token reverse index failed for user_id="
                     << std::to_string(user_id);
        }
        if (index_reply) freeReplyObject(index_reply);
        const int index_ttl = std::max(ttl_seconds + 3600, 86400);
        redisReply* expire_reply = static_cast<redisReply*>(redisCommand(
            ctx, "EXPIRE user:tokens:%lld %d", static_cast<long long>(user_id),
            index_ttl));
        if (expire_reply) freeReplyObject(expire_reply);
    }
    return ok;
}

// 通过 token 获取 user_id
bool RedisStore::GetUserIdByToken(const std::string& token, int64_t* user_id) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
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

bool RedisStore::RevokeUserTokens(int64_t user_id, int* revoked_count) {
    if (revoked_count) *revoked_count = 0;
    if (!pool_ || user_id <= 0) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    std::unordered_set<std::string> token_keys;
    redisReply* indexed = static_cast<redisReply*>(redisCommand(
        ctx, "SMEMBERS user:tokens:%lld", static_cast<long long>(user_id)));
    if (!indexed) return false;
    if (indexed->type != REDIS_REPLY_ARRAY) {
        freeReplyObject(indexed);
        return false;
    }
    for (size_t i = 0; i < indexed->elements; ++i) {
        redisReply* item = indexed->element[i];
        if (item && item->type == REDIS_REPLY_STRING && item->str) {
            token_keys.insert("token:" + std::string(item->str));
        }
    }
    freeReplyObject(indexed);

    // 兼容部署升级前创建的 token：没有 user:tokens 反向索引时扫描 token
    // key 并比对 value。管理操作是低频路径，宁可完整撤销也不留下旧会话。
    std::string cursor = "0";
    do {
        redisReply* scan = static_cast<redisReply*>(redisCommand(
            ctx, "SCAN %s MATCH token:* COUNT 256", cursor.c_str()));
        if (!scan || scan->type != REDIS_REPLY_ARRAY || scan->elements != 2 ||
            !scan->element[0] || !scan->element[1] ||
            scan->element[0]->type != REDIS_REPLY_STRING ||
            scan->element[1]->type != REDIS_REPLY_ARRAY) {
            if (scan) freeReplyObject(scan);
            return false;
        }
        cursor = scan->element[0]->str ? scan->element[0]->str : "0";
        redisReply* keys = scan->element[1];
        for (size_t i = 0; i < keys->elements; ++i) {
            redisReply* key = keys->element[i];
            if (!key || key->type != REDIS_REPLY_STRING || !key->str) continue;
            redisReply* value =
                static_cast<redisReply*>(redisCommand(ctx, "GET %s", key->str));
            if (value && value->type == REDIS_REPLY_STRING && value->str) {
                try {
                    if (std::stoll(value->str) == user_id) {
                        token_keys.insert(key->str);
                    }
                } catch (...) {
                    LOG_WARN << "Ignore malformed token owner value";
                }
            }
            if (value) freeReplyObject(value);
        }
        freeReplyObject(scan);
    } while (cursor != "0");

    int deleted = 0;
    for (const auto& key : token_keys) {
        redisReply* result =
            static_cast<redisReply*>(redisCommand(ctx, "DEL %s", key.c_str()));
        if (!result) return false;
        if (result->type == REDIS_REPLY_INTEGER && result->integer > 0) {
            ++deleted;
        }
        const bool error = result->type == REDIS_REPLY_ERROR;
        freeReplyObject(result);
        if (error) return false;
    }

    redisReply* cleanup = static_cast<redisReply*>(redisCommand(
        ctx, "DEL user:tokens:%lld route:user:%lld",
        static_cast<long long>(user_id), static_cast<long long>(user_id)));
    if (!cleanup) return false;
    const bool cleanup_ok = cleanup->type != REDIS_REPLY_ERROR;
    freeReplyObject(cleanup);
    if (!cleanup_ok) return false;
    InvalidateRouteCache(user_id);
    if (revoked_count) *revoked_count = deleted;
    return true;
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
    if (ok) InvalidateRouteCache(user_id);
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
    if (ok) InvalidateRouteCache(user_id);
    return ok;
}

void RedisStore::InvalidateRouteCache(int64_t user_id) {
    std::unique_lock<std::shared_mutex> lock(route_cache_mutex_);
    route_cache_.erase(user_id);
}

// 获取用户所有 comet 路由（本地缓存 + Redis）
bool RedisStore::GetUserRoutes(int64_t user_id,
                               std::vector<std::string>* comets) {
    if (!comets) return false;
    auto now = std::chrono::steady_clock::now();
    {
        std::shared_lock<std::shared_mutex> lock(route_cache_mutex_);
        auto it = route_cache_.find(user_id);
        if (it != route_cache_.end() && it->second.expire_time > now) {
            *comets = it->second.comets;
            return true;
        }
    }
    std::vector<std::string> redis_comets;
    if (!GetUserRoutesFromRedis(user_id, &redis_comets)) {
        return false;
    }
    {
        std::unique_lock<std::shared_mutex> lock(route_cache_mutex_);
        RouteCacheEntry entry;
        entry.comets = redis_comets;
        entry.expire_time =
            now + std::chrono::milliseconds(route_cache_ttl_ms_);
        route_cache_[user_id] = std::move(entry);
    }
    *comets = std::move(redis_comets);
    return true;
}

bool RedisStore::GetUserRoutesFromRedis(int64_t user_id,
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

namespace {

const char* kAllocateSeqScript = R"lua(
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local floor = tonumber(ARGV[1])
if current < floor then
  redis.call('SET', KEYS[1], floor)
end

if KEYS[3] ~= '' then
  local existing = redis.call('GET', KEYS[3])
  if existing then
    return {tonumber(existing), 0}
  end
end

local next_seq = redis.call('INCR', KEYS[1])
local last_seq = tonumber(redis.call('GET', KEYS[2]) or '0')
if next_seq > last_seq then
  redis.call('SET', KEYS[2], next_seq)
end
if KEYS[3] ~= '' then
  redis.call('SET', KEYS[3], next_seq, 'EX', tonumber(ARGV[2]))
end
return {next_seq, 1}
)lua";

}  // namespace

bool RedisStore::AllocateSessionMsgSeq(const std::string& session_id,
                                       int64_t sender_id,
                                       const std::string& client_msg_id,
                                       int64_t floor_seq,
                                       int dedup_ttl_seconds,
                                       int64_t* msg_seq,
                                       bool* is_new) {
    if (!pool_ || session_id.empty() || floor_seq < 0 || !msg_seq || !is_new) {
        return false;
    }
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    const std::string seq_key = "session:msg_seq:" + session_id;
    const std::string last_key = "session:last_seq:" + session_id;
    std::string dedup_key;
    if (!client_msg_id.empty()) {
        dedup_key = "message:dedup:" + session_id + ":" +
                    std::to_string(sender_id) + ":" + client_msg_id;
    }
    if (dedup_ttl_seconds <= 0) dedup_ttl_seconds = 86400;

    redisReply* reply = static_cast<redisReply*>(redisCommand(
        ctx, "EVAL %s 3 %s %s %s %lld %d", kAllocateSeqScript,
        seq_key.c_str(), last_key.c_str(), dedup_key.c_str(),
        static_cast<long long>(floor_seq), dedup_ttl_seconds));
    if (!reply) {
        LOG_ERROR << "Redis EVAL AllocateSessionMsgSeq failed";
        return false;
    }
    bool ok = reply->type == REDIS_REPLY_ARRAY && reply->elements == 2 &&
              reply->element[0] && reply->element[1] &&
              reply->element[0]->type == REDIS_REPLY_INTEGER &&
              reply->element[1]->type == REDIS_REPLY_INTEGER;
    if (ok) {
        *msg_seq = static_cast<int64_t>(reply->element[0]->integer);
        *is_new = reply->element[1]->integer != 0;
    } else if (reply->type == REDIS_REPLY_ERROR && reply->str) {
        LOG_ERROR << "Redis AllocateSessionMsgSeq error: " << reply->str;
    }
    freeReplyObject(reply);
    return ok;
}

// 会话消息序号原子递增（避免 MySQL session 行锁），同时防止 last_seq 回退。
bool RedisStore::IncrSessionMsgSeq(const std::string& session_id,
                                   int64_t* new_seq) {
    bool is_new = false;
    return AllocateSessionMsgSeq(session_id, 0, "", 0, 86400, new_seq,
                                 &is_new) && is_new;
}

// 设置会话最新序列号
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

// 获取会话最新序列号
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

// 设置用户会话已读序列
bool RedisStore::SetUserReadSeq(int64_t user_id, const std::string& session_id,
                                int64_t read_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "SET user_session:read_seq:%lld:%s %lld",
        static_cast<long long>(user_id), session_id.c_str(),
        static_cast<long long>(read_seq));
    if (!reply) {
        LOG_ERROR << "Redis SET user_session:read_seq failed";
        return false;
    }
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    return ok;
}

// 获取用户会话已读序列
bool RedisStore::GetUserReadSeq(int64_t user_id, const std::string& session_id,
                                int64_t* read_seq) {
    if (!pool_) return false;
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;
    redisReply* reply = (redisReply*)redisCommand(
        ctx, "GET user_session:read_seq:%lld:%s",
        static_cast<long long>(user_id), session_id.c_str());
    if (!reply) {
        LOG_ERROR << "Redis GET user_session:read_seq failed";
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

// 将 comet 加入房间路由集合
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

// 从房间路由集合移除 comet
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

// 获取房间所有 comet 路由
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

// 设置房间在线人数
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

// 获取房间在线人数
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

// 调整房间在线人数计数
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

// 按 room+comet 粒度维护计数
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

}  // namespace sparkpush
