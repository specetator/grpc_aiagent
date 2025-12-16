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
    // 更新会话最新消息序号（用于生成未读/定位“最后一条消息”）
    // @param session_id: 会话标识
    // @param last_seq: 最新消息序号
    // @return: 成功返回 true
    bool SetSessionLastSeq(const std::string& session_id, int64_t last_seq);
    // 查询会话最新消息序号
    // @param session_id: 会话标识
    // @param last_seq: 输出参数，最新消息序号
    // @return: 成功返回 true
    bool GetSessionLastSeq(const std::string& session_id, int64_t* last_seq);
    // 更新用户会话阅读序号
    // @param user_id: 用户 ID
    // @param session_id: 会话标识
    // @param read_seq: 已读到的消息序号
    // @return: 成功返回 true
    bool SetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t read_seq);
    // 查询用户会话阅读序号
    // @param user_id: 用户 ID
    // @param session_id: 会话标识
    // @param read_seq: 输出参数，已读序号
    // @return: 成功返回 true
    bool GetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t* read_seq);
    // 记录房间所在的 comet 节点（用于房间广播时定位订阅节点）
    // @param room_id: 房间 ID
    // @param comet_id: comet 节点标识
    // @return: 成功返回 true
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

    // 房间在线人数增量（可用于 join/leave 时维护在线数）
    // @param room_id: 房间 ID
    // @param delta: 增量（可为负）
    // @param new_value: 输出参数，新的在线人数（可选）
    // @return: 成功返回 true
    bool IncrRoomOnlineCount(int64_t room_id, int64_t delta,
                             int64_t* new_value = nullptr);

    // 房间在单个 comet 上的在线人数增量（用于统计各 comet 分布）
    // @param room_id: 房间 ID
    // @param comet_id: comet 节点标识
    // @param delta: 增量（可为负）
    // @param new_value: 输出参数，新的计数（可选）
    // @return: 成功返回 true
    bool IncrRoomCometCount(int64_t room_id, const std::string& comet_id,
                            int64_t delta, int64_t* new_value = nullptr);

    // 为单聊生成递增消息序号，key: msgid:<小uid>:<大uid>
    bool NextSingleMsgId(int64_t user1, int64_t user2, int64_t* seq);

    // 为群聊/聊天室生成递增消息序号，key: msgid_group:<group_id>
    bool NextGroupMsgId(int64_t group_id, int64_t* seq);

    // ========= 聊天室/话题(房间)管理（本版本不落 MySQL，统一落 Redis）
    // =========
    //
    // Redis Key 设计（房间维度）：
    // - rooms:next_id                string/integer，INCR 分配 room_id
    // - rooms:all                    set，存所有 room_id（string）
    // - room:meta:<room_id>          hash，字段：name/owner_id/created_at_ms
    // - room:members:<room_id>       set，房间成员 user_id（string）
    //
    // Redis Key 设计（用户维度）：
    // - users:all set，系统已见过的用户集合（用于“新话题自动加入”）
    // - user:rooms:<user_id>         set，该用户已加入的 room_id
    //
    // 注意：
    // - 该实现为 Demo 级别，SMEMBERS + 循环 SADD/SREM
    // 可能在大规模场景不够高效；
    //   真实生产环境可用 pipeline/lua/批量命令优化，并引入分页。

    // 记录“见过的用户”（通常在 register/login 成功后调用）。
    bool TrackUser(int64_t user_id);
    // 列出系统已见过的所有用户（users:all）
    bool ListAllUsers(std::vector<int64_t>* user_ids);

    // 创建房间：分配 room_id 并写入元信息与 rooms:all。
    bool CreateRoom(const std::string& name, int64_t owner_id,
                    int64_t* room_id);
    // 列出所有房间 ID（rooms:all）
    bool ListRoomIds(std::vector<int64_t>* room_ids);
    // 查询房间元信息（room:meta:<room_id>）
    bool GetRoomMeta(int64_t room_id, std::string* name, int64_t* owner_id,
                     int64_t* created_at_ms);

    // 房间成员管理
    // 用户加入房间：同时写 room:members 与 user:rooms
    bool JoinRoom(int64_t user_id, int64_t room_id);
    // 用户退出房间：同时删 room:members 与 user:rooms
    bool LeaveRoom(int64_t user_id, int64_t room_id);
    // 判断用户是否在房间内（SISMEMBER）
    bool IsUserInRoom(int64_t user_id, int64_t room_id, bool* is_in);
    // 列出房间成员（SMEMBERS）
    bool ListRoomMembers(int64_t room_id, std::vector<int64_t>* user_ids);
    // 获取房间成员数（SCARD）
    bool GetRoomMemberCount(int64_t room_id, int64_t* count);

    // 自动加入策略：
    // - 新用户：加入当前已存在的所有房间（rooms:all）
    // - 新房间：把 users:all 中的用户全部加入该房间
    bool AutoJoinAllRoomsForUser(int64_t user_id);
    bool AutoJoinRoomForAllUsers(int64_t room_id);

    // ========= 房间元信息缓存（用于 MySQL 持久化 + Redis 加速读取） =========
    // room:meta:<room_id> hash 字段：name/owner_id/created_at_ms/group_type
    // 判断元信息缓存是否存在（用于 read-through）
    bool RoomMetaCacheExists(int64_t room_id, bool* exists);
    // 读取元信息缓存
    bool GetRoomMetaCache(int64_t room_id, std::string* name, int64_t* owner_id,
                          int64_t* created_at_ms, int* group_type);
    // 写入元信息缓存并设置 TTL
    bool SetRoomMetaCache(int64_t room_id, const std::string& name,
                          int64_t owner_id, int64_t created_at_ms,
                          int group_type, int ttl_seconds);
    // 删除元信息缓存
    bool DeleteRoomMetaCache(int64_t room_id);
    // 刷新元信息缓存 TTL
    bool ExpireRoomMetaCache(int64_t room_id, int ttl_seconds);

    // ========= 房间成员列表缓存（用于 MySQL 持久化 + Redis 加速读取）
    // ========= 判断 room:members:<rid> 是否存在（区分“缓存 miss”和“空集合”）
    bool RoomMembersCacheExists(int64_t room_id, bool* exists);
    // 用 DB 的完整成员列表回填缓存（DEL + SADD + EXPIRE）
    bool ReplaceRoomMembersCache(int64_t room_id,
                                 const std::vector<int64_t>& user_ids,
                                 int ttl_seconds);
    // 删除成员缓存（用于批量写入后避免缓存不完整）
    bool DeleteRoomMembersCache(int64_t room_id);
    // 刷新成员缓存 TTL（可选）
    bool ExpireRoomMembersCache(int64_t room_id, int ttl_seconds);

   private:
    // Redis 连接池（RedisStore 不持有长连接）
    RedisConnectionPool* pool_;
};

}  // namespace sparkpush
