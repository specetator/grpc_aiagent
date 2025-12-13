#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mysql_pool.h"

namespace sparkpush {

struct RoomInfo {
    int64_t id{0};
    std::string name;
    int64_t owner_id{0};
    int64_t created_at_ms{0};
    int group_type{1};  // 0=normal_group,1=chatroom,2=danmaku_room（这里用于 im_group 表）
};

// Room/Group DAO：负责 im_group / group_member 的持久化访问
class RoomDao {
   public:
    explicit RoomDao(MySqlConnectionPool* pool) : pool_(pool) {}

    bool CreateRoom(const std::string& name, int64_t owner_id, int group_type,
                    int64_t* room_id, std::string* err_msg);
    bool ListRoomIds(std::vector<int64_t>* room_ids, std::string* err_msg);
    bool ListRooms(std::vector<RoomInfo>* rooms, std::string* err_msg);
    bool GetRoomById(int64_t room_id, RoomInfo* room, std::string* err_msg);

    bool AddMember(int64_t room_id, int64_t user_id, int role,
                   std::string* err_msg);
    bool RemoveMember(int64_t room_id, int64_t user_id, std::string* err_msg);
    bool IsMember(int64_t room_id, int64_t user_id, bool* is_member,
                  std::string* err_msg);
    bool ListMembers(int64_t room_id, std::vector<int64_t>* user_ids,
                     std::string* err_msg);
    bool CountMembers(int64_t room_id, int64_t* count, std::string* err_msg);

    // 自动加入策略（与旧 demo 的 Redis users:all/rooms:all 语义对应）
    bool AutoJoinAllRoomsForUser(int64_t user_id, std::string* err_msg);
    bool AutoJoinRoomForAllUsers(int64_t room_id, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush


