// ============================================================================
// Redis 存储封装
//
// 提供业务层面的 Redis 访问接口，包括：
// 1. Token 管理：存储用户 token 到 user_id 的映射，支持自动过期
// 2. 连接路由：记录用户当前连接的 comet/conn，用于消息推送时查找用户
//
// Redis 数据结构设计：
// - token:<token_string> -> user_id (String 类型，带 TTL)
// - user_connections:<user_id> HSET <comet_id>:<conn_id> "{}" 并设置 TTL
//
// 说明：
// - 一个用户可能同时在多个设备/浏览器登录
// - token 使用 SETEX 命令设置过期时间，过期后自动删除
// ============================================================================
#pragma once

#include <string>
#include <vector>

#include "redis_pool.h"

namespace sparkpush {

// Redis 存储访问类：封装 token 和路由相关的 Redis 操作
class RedisStore {
   public:
    // 构造函数：注入 Redis 连接池
    explicit RedisStore(RedisConnectionPool* pool) : pool_(pool) {}

    // 存储 token 到 user_id 的映射
    // @param token: 登录时生成的 token 字符串
    // @param user_id: 用户 ID
    // @param ttl_seconds: 过期时间（秒），通常为 24 小时
    // @return: 成功返回 true
    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);

    // 通过 token 查询对应的 user_id
    // @param token: token 字符串
    // @param user_id: 输出参数，返回用户 ID
    // @return: 找到返回 true，未找到或已过期返回 false
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    // 新版连接注册：HSET user_connections:{uid} "{comet_id}:{conn_id}" "{}"
    // 并设置 TTL
    // @param user_id: 用户 ID
    // @param comet_id: comet 节点标识
    // @param conn_id: 连接标识
    // @param ttl_ms: 过期时间（毫秒）
    // @return: 成功返回 true
    bool UpsertUserConnection(int64_t user_id, const std::string& comet_id,
                              const std::string& conn_id, int64_t ttl_ms);

    // 移除用户连接：用户断开连接时调用
    // @param user_id: 用户 ID
    // @param comet_id: comet 节点标识
    // @param conn_id: 连接标识
    // @return: 成功返回 true
    bool RemoveUserConnection(int64_t user_id, const std::string& comet_id,
                              const std::string& conn_id);
    // 查询用户当前连接的 comet 节点列表
    // @param user_id: 用户 ID
    // @param comets: 输出参数，返回 comet 节点列表
    // @return: 成功返回 true
    bool GetUserConnectionComets(int64_t user_id,
                                 std::vector<std::string>* comets);

    // 清理未读计数
    // @param user_id: 用户 ID
    // @param session_id: 会话标识
    // @return: 成功返回 true
    bool ClearUnreadCount(int64_t user_id, const std::string& session_id);

    // 未读计数增量
    // @param user_id: 用户 ID
    // @param session_id: 会话标识
    // @param delta: 增量
    // @param new_value: 输出参数，返回新的未读计数
    // @return: 成功返回 true
    bool IncrUnreadCount(int64_t user_id, const std::string& session_id,
                         int64_t delta, int64_t* new_value = nullptr);
    // 查询未读计数
    // @param user_id: 用户 ID
    // @param session_id: 会话标识
    // @param value: 输出参数，返回未读计数
    // @return: 成功返回 true
    bool GetUnreadCount(int64_t user_id, const std::string& session_id,
                        int64_t* value);

    // 会话列表（轻量 Redis 存储）
    // @param user_id: 用户 ID
    // @param session_id: 会话标识
    // @return: 成功返回 true
    bool AddUserSession(int64_t user_id, const std::string& session_id);
    // 查询用户会话列表
    // @param user_id: 用户 ID
    // @param sessions: 输出参数，返回会话列表
    // @return: 成功返回 true
    bool ListUserSessions(int64_t user_id, std::vector<std::string>* sessions);

    // 会话快照元数据（last_msg_*）
    // @param user_id: 用户 ID
    // @param session_id: 会话标识
    // @param last_msg_id: 最后消息 ID
    // @param last_msg_seq: 最后消息序号
    // @param last_msg_type: 最后消息类型
    // @param last_time_ms: 最后消息时间（毫秒）
    // @param last_preview: 最后消息预览
    // @return: 成功返回 true
    bool SetUserSessionMeta(int64_t user_id, const std::string& session_id,
                            const std::string& last_msg_id,
                            int64_t last_msg_seq,
                            const std::string& last_msg_type,
                            int64_t last_time_ms,
                            const std::string& last_preview);
    // 查询会话元数据
    // @param user_id: 用户 ID
    // @param session_id: 会话标识
    // @param last_msg_id: 最后消息 ID
    // @param last_msg_seq: 最后消息序号
    // @param last_msg_type: 最后消息类型
    // @param last_time_ms: 最后消息时间（毫秒）
    // @param last_preview: 最后消息预览
    // @return: 成功返回 true
    bool GetUserSessionMeta(int64_t user_id, const std::string& session_id,
                            std::string* last_msg_id, int64_t* last_msg_seq,
                            std::string* last_msg_type, int64_t* last_time_ms,
                            std::string* last_preview);
    // 更新会话最新消息序号
    bool SetSessionLastSeq(const std::string& session_id,
                           int64_t last_seq);  // 查询会话最新消息序号
    // 查询会话最新消息序号
    bool GetSessionLastSeq(const std::string& session_id, int64_t* last_seq);
    // 更新用户会话阅读序号
    bool SetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t read_seq);
    // 查询用户会话阅读序号
    bool GetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t* read_seq);
    bool AddRoomComet(int64_t room_id, const std::string& comet_id);
    // 移除房间 comet
    // @param room_id: 房间 ID
    // @param comet_id: comet 节点标识
    // @return: 成功返回 true
    bool RemoveRoomComet(int64_t room_id, const std::string& comet_id);
    // 查询房间 comet 列表
    // @param room_id: 房间 ID
    // @param comets: 输出参数，返回 comet 节点列表
    // @return: 成功返回 true
    bool GetRoomComets(int64_t room_id, std::vector<std::string>* comets);
    // 设置房间在线人数
    // @param room_id: 房间 ID
    // @param count: 在线人数
    // @return: 成功返回 true

    bool SetRoomOnlineCount(int64_t room_id, int64_t count);
    // 查询房间在线人数
    // @param room_id: 房间 ID
    // @param count: 输出参数，返回在线人数
    // @return: 成功返回 true
    bool GetRoomOnlineCount(int64_t room_id, int64_t* count);

    bool IncrRoomOnlineCount(int64_t room_id, int64_t delta,
                             int64_t* new_value = nullptr);

    bool IncrRoomCometCount(int64_t room_id, const std::string& comet_id,
                            int64_t delta, int64_t* new_value = nullptr);

    // 为单聊生成递增消息序号，key: msgid:<小uid>:<大uid>
    bool NextSingleMsgId(int64_t user1, int64_t user2, int64_t* seq);

    // 为群聊/聊天室生成递增消息序号，key: msgid_group:<group_id>
    bool NextGroupMsgId(int64_t group_id, int64_t* seq);

   private:
    RedisConnectionPool* pool_;
};

}  // namespace sparkpush
