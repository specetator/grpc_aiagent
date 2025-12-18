// ============================================================================
// Redis 存储封装
//
// 提供业务层面的 Redis 访问接口，包括：
// 1. Token 管理：存储用户 token 到 user_id 的映射，支持自动过期
// 2. 连接路由：记录用户当前连接的 comet/conn，用于消息推送时查找用户
// 3. 消息序号：为单聊会话生成递增的消息序号
//
// Redis 数据结构设计：
// - token:<token_string> -> user_id (String 类型，带 TTL)
//   用途：用户登录后的身份凭证，自动过期
//   示例：token:tk-1001-1234567890 -> "1001"
//
// - user_connections:<user_id> (Hash 类型，带 TTL)
//   field: conn_id (连接唯一标识)
//   value: comet_id (comet 节点标识)
//   用途：记录用户当前所有在线连接，支持多端登录
//   示例：user_connections:1001 -> {conn123: "comet1", conn456: "comet2"}
//
// - msgid:<小uid>:<大uid> (String 类型，自增计数器)
//   用途：为单聊会话生成全局递增的消息序号
//   示例：msgid:1001:1002 -> "42" (第42条消息)
//
// 说明：
// - 一个用户可能同时在多个设备/浏览器登录，对应多个 comet 连接
// - token 使用 SETEX 命令设置过期时间，过期后自动删除
// - 用户连接信息使用 Hash 结构，便于批量查询和管理
// - 消息序号使用 INCR 命令保证原子性和全局唯一
// ============================================================================
#pragma once

#include <string>
#include <vector>

#include "redis_pool.h"

namespace sparkpush {

// Redis 存储访问类：封装 token、路由和消息序号相关的 Redis 操作
//
// 职责说明：
// 1. 提供 token 的存储和验证功能，支持用户身份认证
// 2. 管理用户连接路由信息，支持多端登录和消息推送
// 3. 生成单聊会话的消息序号，保证消息顺序
//
// 使用场景：
// - 用户登录时：SetToken 存储 token
// - comet 验证身份时：GetUserIdByToken 验证 token
// - 用户建立连接时：UpsertUserConnection 记录路由
// - 用户断开连接时：RemoveUserConnection 清理路由
// - 发送消息时：GetUserConnectionComets 查询接收方连接
// - 生成消息时：NextSingleMsgId 分配消息序号
class RedisStore {
   public:
    // 构造函数：注入 Redis 连接池
    // @param pool: Redis 连接池指针，由调用方管理生命周期
    explicit RedisStore(RedisConnectionPool* pool) : pool_(pool) {}

    // 存储 token 到 user_id 的映射
    //
    // 功能说明：
    // 用户登录成功后，生成 token 并通过此方法存储到 Redis
    // 使用 SETEX 命令同时设置键值和过期时间，保证原子性
    //
    // Redis 命令：SETEX token:<token> <ttl_seconds> <user_id>
    //
    // @param token: 登录时生成的 token 字符串，格式通常为
    // "tk-<uid>-<timestamp>"
    // @param user_id: 用户 ID，64 位整数
    // @param ttl_seconds: 过期时间（秒），通常为 24 小时（86400 秒）
    // @return: 成功返回 true，失败（连接池不可用或 Redis 错误）返回 false
    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);

    // 通过 token 查询对应的 user_id
    //
    // 功能说明：
    // comet 节点收到客户端连接请求时，通过此方法验证 token 的有效性
    // 如果 token 存在且未过期，返回对应的 user_id
    //
    // Redis 命令：GET token:<token>
    //
    // @param token: 客户端提供的 token 字符串
    // @param user_id: 输出参数，返回对应的用户 ID
    // @return: 找到返回 true，未找到或已过期返回 false
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    // 插入或更新用户连接信息
    //
    // 功能说明：
    // 用户通过 comet 节点建立 WebSocket 连接后，调用此方法记录路由信息
    // 使用 Hash 结构存储，field 为 conn_id，value 为 comet_id
    // 支持多端登录：同一用户可以有多个连接，分布在不同的 comet 节点上
    //
    // Redis 命令：
    // 1. HSET user_connections:<user_id> <conn_id> <comet_id>
    // 2. PEXPIRE user_connections:<user_id> <ttl_ms>
    //
    // @param user_id: 用户 ID
    // @param comet_id: comet 节点标识，格式如 "comet1"、"comet2" 等
    // @param conn_id: 连接唯一标识，由 comet 节点生成，格式如 "conn123456"
    // @param ttl_ms: 过期时间（毫秒），通常为 60000（60 秒）
    // @return: 成功返回 true，失败返回 false
    bool UpsertUserConnection(int64_t user_id, const std::string& comet_id,
                              const std::string& conn_id, int64_t ttl_ms);

    // 移除用户连接信息
    //
    // 功能说明：
    // 用户断开 WebSocket 连接时，调用此方法清理路由信息
    // 删除 Hash 中对应的 field，如果 Hash 为空会自动删除整个 key
    //
    // Redis 命令：HDEL user_connections:<user_id> <conn_id>
    //
    // @param user_id: 用户 ID
    // @param comet_id: comet 节点标识（用于日志和验证）
    // @param conn_id: 连接唯一标识
    // @return: 成功返回 true，失败返回 false
    bool RemoveUserConnection(int64_t user_id, const std::string& comet_id,
                              const std::string& conn_id);

    // 查询用户当前连接的所有 comet 节点列表
    //
    // 功能说明：
    // 发送消息时，通过此方法查询目标用户当前在线的所有 comet 节点
    // 返回的 comet 列表去重，因为用户可能在同一个 comet 节点上有多个连接
    //
    // Redis 命令：HVALS user_connections:<user_id>
    //
    // @param user_id: 用户 ID
    // @param comets: 输出参数，返回去重后的 comet 节点列表
    // @return: 成功返回 true（即使列表为空），失败返回 false
    bool GetUserConnectionComets(int64_t user_id,
                                 std::vector<std::string>* comets);

    // 为单聊会话生成递增的消息序号
    //
    // 功能说明：
    // 每次发送单聊消息时，调用此方法分配一个全局唯一且递增的消息序号
    // 使用 INCR 命令保证原子性，即使在并发情况下也不会重复
    // 消息序号用于客户端排序、去重和历史消息同步
    //
    // Redis 命令：INCR msgid:<小uid>:<大uid>
    //
    // Key 生成规则：
    // - 将两个用户 ID 按大小排序（小的在前，大的在后）
    // - 格式：msgid:<小uid>:<大uid>
    // - 示例：用户 1001 和 1002 的会话，key 为 "msgid:1001:1002"
    //
    // @param user1: 会话中的第一个用户 ID
    // @param user2: 会话中的第二个用户 ID
    // @param seq: 输出参数，返回分配的消息序号（从 1 开始递增）
    // @return: 成功返回 true，失败返回 false
    bool NextSingleMsgId(int64_t user1, int64_t user2, int64_t* seq);

   private:
    // Redis 连接池指针
    // 通过连接池管理 Redis 连接，避免频繁创建/销毁连接
    // 由调用方（如 main 函数）创建并注入，生命周期由调用方管理
    RedisConnectionPool* pool_;
};

}  // namespace sparkpush
