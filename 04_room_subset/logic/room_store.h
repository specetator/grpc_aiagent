#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "redis_store.h"
#include "room_dao.h"

namespace sparkpush {

// RoomStore：组合 MySQL(RoomDao) + Redis(成员列表缓存)
// - MySQL：最终一致的持久化来源
// - Redis：成员列表 read-through 缓存，join/leave 写穿
class RoomStore {
   public:
    RoomStore(RoomDao* dao, RedisStore* redis_store,
              int members_cache_ttl_seconds = 300,
              int meta_cache_ttl_seconds = 600,
              bool room_list_prefer_redis = false)
        : dao_(dao),
          redis_store_(redis_store),
          members_cache_ttl_seconds_(members_cache_ttl_seconds),
          meta_cache_ttl_seconds_(meta_cache_ttl_seconds),
          room_list_prefer_redis_(room_list_prefer_redis) {}

    bool CreateRoom(const std::string& name, int64_t owner_id,
                    bool auto_join_all, int64_t* room_id, std::string* err_msg);
    bool ListRooms(std::vector<RoomInfo>* rooms, std::string* err_msg);

    bool JoinRoom(int64_t user_id, int64_t room_id, std::string* err_msg);
    bool LeaveRoom(int64_t user_id, int64_t room_id, std::string* err_msg);

    bool IsUserInRoom(int64_t user_id, int64_t room_id, bool* is_in,
                      std::string* err_msg);
    bool ListRoomMembers(int64_t room_id, std::vector<int64_t>* user_ids,
                         std::string* err_msg);
    bool GetRoomMemberCount(int64_t room_id, int64_t* count,
                            std::string* err_msg);

    // 登录/注册后：自动加入所有已存在房间（MySQL 落库）
    bool AutoJoinAllRoomsForUser(int64_t user_id, std::string* err_msg);

   private:
    bool RoomMembersCacheExists(int64_t room_id, bool* exists);
    bool WarmRoomMembersCacheFromDb(int64_t room_id,
                                    std::vector<int64_t>* members_out,
                                    std::string* err_msg);
    void WarmRoomMetaCache(const RoomInfo& room);

    RoomDao* dao_{nullptr};
    RedisStore* redis_store_{nullptr};
    int members_cache_ttl_seconds_{300};
    int meta_cache_ttl_seconds_{600};
    bool room_list_prefer_redis_{false};
};

}  // namespace sparkpush
