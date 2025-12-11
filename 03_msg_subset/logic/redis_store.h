#pragma once

#include <string>
#include <vector>

#include "redis_pool.h"

namespace sparkpush {

class RedisStore {
   public:
    explicit RedisStore(RedisConnectionPool* pool) : pool_(pool) {}

    bool SetToken(const std::string& token, int64_t user_id, int ttl_seconds);
    bool GetUserIdByToken(const std::string& token, int64_t* user_id);

    bool AddRoute(int64_t user_id, const std::string& comet_id);
    bool RemoveRoute(int64_t user_id, const std::string& comet_id);
    bool GetUserRoutes(int64_t user_id, std::vector<std::string>* comets);

    bool SetSessionLastSeq(const std::string& session_id, int64_t last_seq);
    bool GetSessionLastSeq(const std::string& session_id, int64_t* last_seq);
    bool SetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t read_seq);
    bool GetUserReadSeq(int64_t user_id, const std::string& session_id,
                        int64_t* read_seq);

    bool AddRoomComet(int64_t room_id, const std::string& comet_id);
    bool RemoveRoomComet(int64_t room_id, const std::string& comet_id);
    bool GetRoomComets(int64_t room_id, std::vector<std::string>* comets);

    bool SetRoomOnlineCount(int64_t room_id, int64_t count);
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
