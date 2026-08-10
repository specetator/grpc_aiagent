#pragma once

#include <chrono>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "redis_pool.h"

namespace sparkpush {

// 封装 Redis 访问：token 存储、用户路由信息
class RedisStore {
   public:
    explicit RedisStore(RedisConnectionPool* pool, int route_cache_ttl_ms = 1000)
        : pool_(pool), route_cache_ttl_ms_(route_cache_ttl_ms) {}

    // 功能：写入 token 映射
    // 参数：token 字符串；user_id 用户；ttl_seconds 过期秒
    // 返回：成功 true
    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);
    // 功能：通过 token 获取 user_id
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    // 撤销用户的全部登录凭证，同时清理在线路由和本地路由缓存。
    // 返回 revoked_count，兼容没有反向索引的旧 token（会用 SCAN 兜底）。
    bool RevokeUserTokens(int64_t user_id, int* revoked_count = nullptr);

    // 功能：添加/移除/获取用户路由 comet 集合
    bool AddRoute(int64_t user_id, const std::string& comet_id);
    bool RemoveRoute(int64_t user_id, const std::string& comet_id);
    // 带本地缓存（shared_mutex + TTL），对齐 06 优化
    bool GetUserRoutes(int64_t user_id, std::vector<std::string>* comets);
    void InvalidateRouteCache(int64_t user_id);

    // 未读相关：会话最新 seq 与用户已读 seq
    bool SetSessionLastSeq(const std::string& session_id, int64_t last_seq);
    bool GetSessionLastSeq(const std::string& session_id, int64_t* last_seq);
    // 热路径：会话内消息序号原子递增，并以 max 语义更新 last_seq。
    bool IncrSessionMsgSeq(const std::string& session_id, int64_t* new_seq);
    // 原子完成：计数器至少校准到 floor_seq、client_msg_id 去重、分配新 seq、
    // 以 max 语义更新 last_seq。is_new=false 表示命中幂等键，不应再次推送/落库。
    bool AllocateSessionMsgSeq(const std::string& session_id,
                               int64_t sender_id,
                               const std::string& client_msg_id,
                               int64_t floor_seq,
                               int dedup_ttl_seconds,
                               int64_t* msg_seq,
                               bool* is_new);
    bool SetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t read_seq);
    bool GetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t* read_seq);

    // 聊天室房间路由：room_id -> set<comet_id>
    bool AddRoomComet(int64_t room_id, const std::string& comet_id);
    bool RemoveRoomComet(int64_t room_id, const std::string& comet_id);
    bool GetRoomComets(int64_t room_id, std::vector<std::string>* comets);

    // 聊天室在线人数：room_id -> online_count
    bool SetRoomOnlineCount(int64_t room_id, int64_t count);
    bool GetRoomOnlineCount(int64_t room_id, int64_t* count);

    // 使用 INCRBY/DECRBY 维护在线人数计数（delta 可为正/负）
    bool IncrRoomOnlineCount(int64_t room_id, int64_t delta,
                             int64_t* new_value = nullptr);

    // 按 room_id + comet_id 维度维护计数，用于精确维护 room:comets:{room_id}
    bool IncrRoomCometCount(int64_t room_id, const std::string& comet_id,
                            int64_t delta, int64_t* new_value = nullptr);

   private:
    bool GetUserRoutesFromRedis(int64_t user_id,
                                std::vector<std::string>* comets);

    RedisConnectionPool* pool_;

    struct RouteCacheEntry {
        std::vector<std::string> comets;
        std::chrono::steady_clock::time_point expire_time;
    };
    std::unordered_map<int64_t, RouteCacheEntry> route_cache_;
    mutable std::shared_mutex route_cache_mutex_;
    int route_cache_ttl_ms_{1000};
};

}  // namespace sparkpush
