#pragma once

#include <string>
#include <vector>

#include "redis_pool.h"

namespace sparkpush {

// 封装 Redis 访问：token 存储、用户路由信息
class RedisStore {
   public:
    explicit RedisStore(RedisConnectionPool* pool) : pool_(pool) {}

    // 功能：写入 token 映射
    // 参数：token 字符串；user_id 用户；ttl_seconds 过期秒
    // 返回：成功 true
    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);
    // 功能：通过 token 获取 user_id
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    // 功能：添加/移除/获取用户路由 comet 集合
    bool AddRoute(int64_t user_id, const std::string& comet_id);
    bool RemoveRoute(int64_t user_id, const std::string& comet_id);
    bool GetUserRoutes(int64_t user_id, std::vector<std::string>* comets);

    // 未读相关：会话最新 seq 与用户已读 seq
    bool SetSessionLastSeq(const std::string& session_id, int64_t last_seq);
    bool GetSessionLastSeq(const std::string& session_id, int64_t* last_seq);
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
    RedisConnectionPool* pool_;
};

}  // namespace sparkpush
