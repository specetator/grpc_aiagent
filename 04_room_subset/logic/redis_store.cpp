// ============================================================================
// Redis 存储实现
//
// 使用 hiredis 库操作 Redis，通过连接池管理连接
// 提供 token 管理、用户连接路由和消息序号生成等功能
// ============================================================================
#include "redis_store.h"

#include <unordered_set>

#include "function_timer.h"
#include "logging.h"

namespace sparkpush {

// 存储 token 到 Redis
//
// 实现说明：
// 使用 SETEX 命令是原子操作，同时设置键值和过期时间，无需额外的 EXPIRE 命令
// 这样可以避免先 SET 后 EXPIRE 之间的时间窗口问题
//
// Redis 数据结构：
// Key 格式：token:<token_string>
// Value：user_id（数字字符串）
// TTL：ttl_seconds 秒后自动过期删除
//
// 示例：
// SETEX token:tk-1001-1234567890 86400 1001
// 表示 token "tk-1001-1234567890" 映射到用户 1001，24 小时后过期
//
// @param token: 用户登录时生成的 token 字符串
// @param user_id: 用户 ID
// @param ttl_seconds: 过期时间（秒）
// @return: 成功返回 true，失败返回 false
bool RedisStore::SetToken(const std::string& token, int64_t user_id, int ttl_seconds) {
    // 步骤1：检查连接池是否可用
    if (!pool_) return false;

    // 步骤2：从连接池获取 Redis 连接
    // guard 是 RAII 风格的连接保护对象，离开作用域时自动归还连接
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    // 步骤3：执行 SETEX 命令
    // 命令格式：SETEX key seconds value
    // %s: token 字符串
    // %d: 过期时间（秒）
    // %lld: user_id（long long 类型）
    redisReply* reply = (redisReply*)redisCommand(ctx, "SETEX token:%s %d %lld", token.c_str(),
                                                  ttl_seconds, static_cast<long long>(user_id));
    if (!reply) {
        LOG_ERROR << "Redis SETEX token failed";
        return false;
    }

    LOG_DEBUG << "SetToken " << token << ", reply type: " << std::to_string(reply->type);

    // 步骤4：检查回复类型
    // SETEX 成功时返回 REDIS_REPLY_STATUS（状态回复，内容为 "OK"）
    // 失败时返回 REDIS_REPLY_ERROR（错误回复）
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);  // 释放回复对象内存
    return ok;
}

// 通过 token 查询 user_id
//
// 实现说明：
// 使用 GET 命令读取 token 对应的 user_id
// 如果 token 不存在或已过期，GET 返回 nil（REDIS_REPLY_NIL）
//
// Redis 命令：GET token:<token>
//
// 返回值类型：
// - REDIS_REPLY_STRING: token 存在且未过期，返回 user_id 字符串
// - REDIS_REPLY_NIL: token 不存在或已过期
// - REDIS_REPLY_ERROR: Redis 执行错误
//
// @param token: 客户端提供的 token 字符串
// @param user_id: 输出参数，返回对应的用户 ID
// @return: 验证成功返回 true，失败返回 false
bool RedisStore::GetUserIdByToken(const std::string& token, int64_t* user_id) {
    // 步骤1：检查连接池和输出参数
    if (!pool_) return false;

    // 步骤2：从连接池获取 Redis 连接
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    LOG_DEBUG << "GetUserIdByToken " << token;

    // 步骤3：执行 GET 命令
    // 命令格式：GET token:<token>
    redisReply* reply = (redisReply*)redisCommand(ctx, "GET token:%s", token.c_str());
    if (!reply) {
        LOG_ERROR << "Redis GET token failed";
        return false;
    }

    // 步骤4：检查回复类型并解析结果
    // REDIS_REPLY_STRING: 找到值，reply->str 包含 user_id 的字符串表示
    // REDIS_REPLY_NIL: 键不存在或已过期
    bool ok = false;
    if (reply->type == REDIS_REPLY_STRING && reply->str) {
        // 将字符串转换为 64 位整数
        *user_id = std::stoll(reply->str);
        ok = true;
    }

    freeReplyObject(reply);  // 释放回复对象内存
    return ok;
}

// 插入或更新用户连接信息
//
// 实现说明：
// 使用 Hash 结构存储用户的所有连接信息，支持多端登录
// field 使用 conn_id（连接唯一标识），value 存储 comet_id（节点标识）
//
// Redis 数据结构：
// Key: user_connections:<user_id>
// Type: Hash
// Field: conn_id（连接标识）
// Value: comet_id（comet 节点标识）
//
// 示例：
// HSET user_connections:1001 conn123 comet1
// HSET user_connections:1001 conn456 comet2
// PEXPIRE user_connections:1001 60000
//
// 这样用户 1001 就有两个连接：
// - conn123 在 comet1 节点上
// - conn456 在 comet2 节点上
//
// @param user_id: 用户 ID
// @param comet_id: comet 节点标识
// @param conn_id: 连接唯一标识
// @param ttl_ms: 过期时间（毫秒）
// @return: 成功返回 true，失败返回 false
bool RedisStore::UpsertUserConnection(int64_t user_id, const std::string& comet_id,
                                      const std::string& conn_id, int64_t ttl_ms) {
    // 步骤1：参数验证
    // 检查连接池、comet_id 和 conn_id 是否有效
    if (!pool_ || comet_id.empty() || conn_id.empty()) return false;

    // 步骤2：获取 Redis 连接
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    // 步骤3：构造 Redis key 和 field
    // key 格式：user_connections:<user_id>
    std::string key = "user_connections:" + std::to_string(user_id);
    // field 使用 conn_id，保证每个连接的唯一性
    std::string field = conn_id;

    // 兼容清理：删除旧格式 field=<comet_id>:<conn_id>（value 常为 "{}"）
    // 旧格式会污染 HVALS（value 被当作 comet_id
    // 列表），因此在写入新格式前先清理。
    {
        std::string legacy_field = comet_id + ":" + conn_id;
        redisReply* r0 = (redisReply*)redisCommand(ctx, "HDEL %s %s", key.c_str(),
                                                   legacy_field.c_str());
        if (r0) freeReplyObject(r0);
    }

    // 步骤4：执行 HSET 命令
    // 命令格式：HSET key field value
    // 如果 field 已存在，会更新 value；如果不存在，会创建新的 field
    redisReply* reply = (redisReply*)redisCommand(ctx, "HSET %s %s %s", key.c_str(), field.c_str(),
                                                  comet_id.c_str());
    if (!reply) {
        LOG_ERROR << "Redis HSET user_connections failed";
        return false;
    }

    // 步骤5：检查 HSET 执行结果
    // HSET 成功时返回 REDIS_REPLY_INTEGER（返回值 0 或 1）
    // 0 表示 field 已存在并更新，1 表示新创建 field
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);
    if (!ok) return false;

    // 步骤6：设置过期时间
    // 使用 PEXPIRE 设置毫秒级过期时间
    // 注意：每次更新连接时都会重置 TTL，保持连接活跃
    if (ttl_ms > 0) {
        reply = (redisReply*)redisCommand(ctx, "PEXPIRE %s %lld", key.c_str(),
                                          static_cast<long long>(ttl_ms));
        if (reply) freeReplyObject(reply);
        // 注意：这里忽略 PEXPIRE 的错误，因为 HSET 已经成功
        // 即使 TTL 设置失败，连接信息也已经记录了
    }
    return true;
}

// 移除用户连接信息
//
// 实现说明：
// 用户断开 WebSocket 连接时，从 Redis Hash 中删除对应的连接记录
// 使用 HDEL 命令删除指定的 field
//
// Redis 命令：HDEL user_connections:<user_id> <conn_id>
//
// 行为说明：
// - 如果 field 存在，删除并返回 1
// - 如果 field 不存在，返回 0（不是错误）
// - 如果删除后 Hash 为空，Redis 会自动删除整个 key
//
// @param user_id: 用户 ID
// @param comet_id: comet 节点标识（当前未使用，保留用于日志和验证）
// @param conn_id: 连接唯一标识
// @return: 成功返回 true，失败返回 false
bool RedisStore::RemoveUserConnection(int64_t user_id, const std::string& comet_id,
                                      const std::string& conn_id) {
    // 步骤1：参数验证
    if (!pool_ || comet_id.empty() || conn_id.empty()) return false;

    // 步骤2：获取 Redis 连接
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    // 步骤3：构造 Redis key 和 field
    std::string key = "user_connections:" + std::to_string(user_id);
    std::string field = conn_id;

    // 步骤4：执行 HDEL 命令
    // 命令格式：HDEL key field [field ...]
    // 删除 Hash 中的指定 field
    redisReply* reply = (redisReply*)redisCommand(ctx, "HDEL %s %s", key.c_str(), field.c_str());
    if (!reply) return false;

    // 步骤5：检查执行结果
    // HDEL 成功时返回 REDIS_REPLY_INTEGER，值为删除的 field 数量
    // 即使 field 不存在（返回 0），也不算错误
    bool ok = (reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(reply);

    // 兼容清理：再删除一次旧格式 field=<comet_id>:<conn_id>
    // 即使不存在也不算错误，用于逐步清理存量脏字段。
    {
        std::string legacy_field = comet_id + ":" + conn_id;
        redisReply* r2 = (redisReply*)redisCommand(ctx, "HDEL %s %s", key.c_str(),
                                                   legacy_field.c_str());
        if (r2) freeReplyObject(r2);
    }
    return ok;
}

// 查询用户当前连接的所有 comet 节点列表
//
// 实现说明：
// 使用 HVALS 命令获取 Hash 中所有的 value（即 comet_id）
// 由于用户可能在同一个 comet 节点上有多个连接，需要去重
//
// Redis 数据结构：
// Key: user_connections:<user_id> (Hash)
// Field: conn_id（连接标识）
// Value: comet_id（节点标识）
//
// Redis 命令：HVALS user_connections:<user_id>
//
// 返回值示例：
// ["comet1", "comet2", "comet1"]  -> 去重后 ["comet1", "comet2"]
//
// 使用场景：
// 发送消息时，查询目标用户在哪些 comet 节点上有连接
// 然后向这些 comet 节点发送推送请求
//
// @param user_id: 用户 ID
// @param comets: 输出参数，返回去重后的 comet 节点列表
// @return: 成功返回 true（即使列表为空），失败返回 false
bool RedisStore::GetUserConnectionComets(int64_t user_id, std::vector<std::string>* comets) {
    // 步骤1：参数验证
    if (!pool_ || !comets) return false;

    // 步骤2：获取 Redis 连接
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    // 步骤3：构造 Redis key
    std::string key = "user_connections:" + std::to_string(user_id);

    // 步骤4：执行 HVALS 命令
    // HVALS 返回 Hash 中所有的 value，即所有的 comet_id
    // 注意：Hash 结构是 field=conn_id，value=comet_id
    redisReply* reply = (redisReply*)redisCommand(ctx, "HVALS %s", key.c_str());
    if (!reply) {
        LOG_ERROR << "Redis HVALS user_connections failed";
        return false;
    }

    // 步骤5：解析返回结果
    bool ok = false;
    if (reply->type == REDIS_REPLY_ARRAY) {
        ok = true;
        // 使用 unordered_set 去重
        // 因为用户可能在同一个 comet 节点上有多个连接（多个标签页、多个设备等）
        std::unordered_set<std::string> uniq;

        // 遍历数组中的每个元素（comet_id）
        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* v = reply->element[i];
            // 检查元素类型是否为字符串且非空
            if (v->type == REDIS_REPLY_STRING && v->str && v->len > 0) {
                uniq.insert(v->str);  // 自动去重
            }
        }

        // 将去重后的结果赋值给输出参数
        comets->assign(uniq.begin(), uniq.end());
    }

    freeReplyObject(reply);
    return ok;
}

// 为单聊会话生成递增的消息序号
//
// 实现说明：
// 使用 INCR 命令对计数器进行原子递增操作
// INCR 是原子操作，保证在并发场景下不会产生重复的序号
//
// Key 生成规则：
// 1. 将两个用户 ID 按大小排序（小的在前，大的在后）
// 2. 格式：msgid:<小uid>:<大uid>
// 3. 示例：用户 1001 和 1002 的会话，key 为 "msgid:1001:1002"
//
// Redis 命令：INCR msgid:<user1>:<user2>
//
// INCR 行为：
// - 如果 key 不存在，先初始化为 0，再执行 +1，返回 1
// - 如果 key 存在，直接 +1，返回递增后的值
// - 返回值从 1 开始递增：1, 2, 3, 4, ...
//
// 使用场景：
// 每次发送单聊消息时，调用此方法分配消息序号
// 序号用于：
// 1. 消息排序：客户端按序号显示消息
// 2. 消息去重：相同序号的消息认为是重复的
// 3. 历史同步：客户端可以请求某个序号之后的所有消息
//
// @param user1: 会话中的第一个用户 ID
// @param user2: 会话中的第二个用户 ID
// @param seq: 输出参数，返回分配的消息序号（从 1 开始）
// @return: 成功返回 true，失败返回 false
bool RedisStore::NextSingleMsgId(int64_t user1, int64_t user2, int64_t* seq) {
    // 步骤1：参数验证
    if (!seq) return false;
    if (user1 <= 0 || user2 <= 0) return false;

    // 步骤2：规范化用户 ID 顺序
    // 保证 user1 <= user2，这样无论调用时参数顺序如何，都能得到相同的 key
    // 例如：NextSingleMsgId(1001, 1002) 和 NextSingleMsgId(1002, 1001)
    // 都会使用 key "msgid:1001:1002"
    if (user1 > user2) std::swap(user1, user2);

    // 步骤3：检查连接池
    if (!pool_) return false;

    // 步骤4：获取 Redis 连接
    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    // 步骤5：构造 Redis key
    // 格式：msgid:<小uid>:<大uid>
    // 范例：msgid:1001:1002
    std::string key = "msgid:" + std::to_string(user1) + ":" + std::to_string(user2);

    // 步骤6：执行 INCR 命令
    // INCR 是原子操作，返回递增后的值
    // 如果是第一次调用，key 不存在，INCR 会先初始化为 0，再 +1，返回 1
    redisReply* reply = (redisReply*)redisCommand(ctx, "INCR %s", key.c_str());
    if (!reply) {
        LOG_ERROR << "Redis INCR single msg_id failed, key: " << key;
        return false;
    }

    // 步骤7：解析返回结果
    // INCR 成功时返回 REDIS_REPLY_INTEGER，值为递增后的计数
    bool ok = false;
    if (reply->type == REDIS_REPLY_INTEGER) {
        ok = true;
        *seq = reply->integer;  // 赋值给输出参数
    }

    freeReplyObject(reply);
    return ok;
}

// 为房间消息生成递增的消息序号
//
// 实现说明：
// 使用 INCR 命令为每个房间生成递增的消息序号
// Key 格式：room_msgid:<room_id>
//
// @param room_id: 房间 ID
// @param seq: 输出参数，返回分配的消息序号
// @return: 成功返回 true，失败返回 false
bool RedisStore::NextRoomMsgId(int64_t room_id, int64_t* seq) {
    if (!pool_) return false;

    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    std::string key = "room_msgid:" + std::to_string(room_id);

    redisReply* reply = (redisReply*)redisCommand(ctx, "INCR %s", key.c_str());
    if (!reply) {
        LOG_ERROR << "Redis INCR room msg_id failed, key: " << key;
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

// ============================================================================
// 房间成员缓存：读穿（Redis miss 时回源 MySQL）
// ============================================================================
bool RedisStore::GetRoomMembersFromCache(
    int64_t room_id, std::vector<int64_t>* user_ids,
    std::function<bool(int64_t, std::vector<int64_t>*)> fallback_func) {
    if (!pool_) return false;

    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    std::string key = "room_members:" + std::to_string(room_id);  // set的key

    // 步骤1：尝试从 Redis 读取
    redisReply* reply = (redisReply*)redisCommand(ctx, "SMEMBERS %s", key.c_str());
    if (!reply) {
        LOG_ERROR << "Redis SMEMBERS failed for key: " << key;
        return false;  // 读取失败返回false
    }

    // 步骤2：判断是否 miss
    if (reply->type == REDIS_REPLY_ARRAY && reply->elements > 0) {  //不为空是说明房间成员还有缓存
        // 缓存命中：解析成员列表
        if (user_ids) {
            user_ids->clear();
            for (size_t i = 0; i < reply->elements; ++i) {
                if (reply->element[i]->type == REDIS_REPLY_STRING) {
                    int64_t uid = std::stoll(reply->element[i]->str);
                    user_ids->push_back(uid);  // 把缓存中的成员列表赋值给输出参数
                }
            }
        }
        freeReplyObject(reply);
        LOG_INFO << "GetRoomMembersFromCache hit, room_id=" << room_id
                 << ", count=" << (user_ids ? user_ids->size() : 0);
        return true;
    }

    freeReplyObject(reply);

    // 步骤3：缓存 miss，回源 MySQL
    if (!fallback_func) {
        LOG_WARN << "GetRoomMembersFromCache miss and no fallback, room_id=" << room_id;
        if (user_ids) user_ids->clear();
        return true;  // 空结果也算成功
    }

    std::vector<int64_t> members;
    if (!fallback_func(room_id, &members)) {
        LOG_ERROR << "Fallback function failed for room_id=" << room_id;
        return false;
    }

    // 步骤4：回填 Redis（设置 TTL）
    if (!members.empty()) {
        // 使用 SADD 批量添加成员
        std::string sadd_cmd = "SADD " + key;
        for (auto uid : members) {
            sadd_cmd += " " + std::to_string(uid);
        }
        redisReply* r2 = (redisReply*)redisCommand(ctx, sadd_cmd.c_str());
        if (r2) freeReplyObject(r2);

        // 设置 TTL（建议 1小时 秒）
        redisReply* r3 = (redisReply*)redisCommand(ctx, "EXPIRE %s 300", key.c_str());
        if (r3) freeReplyObject(r3);

        LOG_INFO << "GetRoomMembersFromCache miss->backfill, room_id=" << room_id
                 << ", count=" << members.size();
    }

    if (user_ids) {
        *user_ids = members;
    }
    return true;
}

// ============================================================================
// 房间成员缓存：写穿（添加成员）
// ============================================================================
bool RedisStore::AddRoomMemberToCache(int64_t room_id, int64_t user_id, int ttl_seconds) {
    if (!pool_) return false;

    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    std::string key = "room_members:" + std::to_string(room_id);

    // SADD：添加成员到 Set
    redisReply* reply = (redisReply*)redisCommand(ctx, "SADD %s %lld", key.c_str(),
                                                  static_cast<long long>(user_id));
    if (!reply) {
        LOG_ERROR << "Redis SADD failed for key: " << key;
        return false;
    }
    freeReplyObject(reply);

    // EXPIRE：刷新 TTL
    if (ttl_seconds > 0) {
        redisReply* r2 = (redisReply*)redisCommand(ctx, "EXPIRE %s %d", key.c_str(), ttl_seconds);
        if (r2) freeReplyObject(r2);
    }

    LOG_INFO << "AddRoomMemberToCache room_id=" << room_id << ", user_id=" << user_id;
    return true;
}

// ============================================================================
// 房间成员缓存：写穿（移除成员）
// ============================================================================
bool RedisStore::RemoveRoomMemberFromCache(int64_t room_id, int64_t user_id) {
    if (!pool_) return false;

    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    std::string key = "room_members:" + std::to_string(room_id);

    // SREM：从 Set 中移除成员
    redisReply* reply = (redisReply*)redisCommand(ctx, "SREM %s %lld", key.c_str(),
                                                  static_cast<long long>(user_id));
    if (!reply) {
        LOG_ERROR << "Redis SREM failed for key: " << key;
        return false;
    }
    freeReplyObject(reply);

    LOG_INFO << "RemoveRoomMemberFromCache room_id=" << room_id << ", user_id=" << user_id;
    return true;
}

// ============================================================================
// 批量查询在线路由（pipeline 优化）
// ============================================================================
bool RedisStore::BatchGetUserConnectionsComets(
    const std::vector<int64_t>& user_ids,
    std::unordered_map<std::string, std::vector<int64_t>>* comet_users) {
    FUNCTION_TIMER();
    if (!pool_ || user_ids.empty()) return false;

    auto guard = pool_->Acquire();
    redisContext* ctx = guard.get();
    if (!ctx) return false;

    // 步骤1：使用 pipeline 批量发送 HVALS 命令
    // 限制单批大小，避免单次 payload 过大
    const size_t batch_size = 500;
    std::unordered_map<std::string, std::vector<int64_t>> result;

    for (size_t start = 0; start < user_ids.size(); start += batch_size) {
        size_t end = std::min(start + batch_size, user_ids.size());

        // 发送 pipeline 命令
        for (size_t i = start; i < end; ++i) {
            int64_t uid = user_ids[i];
            std::string key = "user_connections:" + std::to_string(uid);
            redisAppendCommand(ctx, "HVALS %s", key.c_str());
        }

        // 接收 pipeline 响应
        for (size_t i = start; i < end; ++i) {
            int64_t uid = user_ids[i];
            redisReply* reply = nullptr;
            if (redisGetReply(ctx, (void**)&reply) != REDIS_OK || !reply) {
                LOG_ERROR << "BatchGetUserConnectionsComets pipeline failed for uid=" << uid;
                continue;
            }

            // 解析 HVALS 返回的 comet_id 列表（去重）
            if (reply->type == REDIS_REPLY_ARRAY) {
                std::unordered_set<std::string> comet_set;
                for (size_t j = 0; j < reply->elements; ++j) {
                    if (reply->element[j]->type == REDIS_REPLY_STRING) {
                        redisReply* v = reply->element[j];
                        if (v && v->str && v->len > 0) {
                            comet_set.insert(v->str);
                        }
                    }
                }
                // 聚合到 comet_id -> user_ids 映射
                for (const auto& cid : comet_set) {
                    result[cid].push_back(uid);
                }
            }
            freeReplyObject(reply);
        }
    }

    if (comet_users) {
        *comet_users = std::move(result);
    }

    LOG_INFO << "BatchGetUserConnectionsComets completed, user_count=" << user_ids.size()
             << ", comet_count=" << result.size();
    return true;
}

}  // namespace sparkpush
