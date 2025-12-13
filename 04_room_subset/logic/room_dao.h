#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mysql_pool.h"

namespace sparkpush {

// RoomInfo：话题(房间)/群组的基础元信息（对应 im_group 表）
struct RoomInfo {
    // 房间/群组 ID
    int64_t id{0};
    // 房间名称
    std::string name;
    // 创建者/群主用户 ID
    int64_t owner_id{0};
    // 创建时间（毫秒时间戳）
    int64_t created_at_ms{0};
    // 群组类型：0=normal_group,1=chatroom,2=danmaku_room（对应 im_group.group_type）
    int group_type{1};  // 0=normal_group,1=chatroom,2=danmaku_room（这里用于 im_group 表）
};

// Room/Group DAO：负责 im_group / group_member 的持久化访问
class RoomDao {
   public:
    // 构造函数：注入 MySQL 连接池
    explicit RoomDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 创建房间/群组（写 im_group），返回 room_id
    bool CreateRoom(const std::string& name, int64_t owner_id, int group_type,
                    int64_t* room_id, std::string* err_msg);
    // 查询所有房间 ID（用于列表/自动加入等）
    bool ListRoomIds(std::vector<int64_t>* room_ids, std::string* err_msg);
    // 查询房间列表（返回完整元信息）
    bool ListRooms(std::vector<RoomInfo>* rooms, std::string* err_msg);
    // 按 room_id 查询房间元信息
    bool GetRoomById(int64_t room_id, RoomInfo* room, std::string* err_msg);

    // 添加成员（写 group_member）
    // @param role: 成员角色（由业务定义，如 0=member/1=admin 等）
    bool AddMember(int64_t room_id, int64_t user_id, int role,
                   std::string* err_msg);
    // 移除成员（删 group_member）
    bool RemoveMember(int64_t room_id, int64_t user_id, std::string* err_msg);
    // 判断用户是否为房间成员
    bool IsMember(int64_t room_id, int64_t user_id, bool* is_member,
                  std::string* err_msg);
    // 列出房间成员 user_id 列表
    bool ListMembers(int64_t room_id, std::vector<int64_t>* user_ids,
                     std::string* err_msg);
    // 统计房间成员数
    bool CountMembers(int64_t room_id, int64_t* count, std::string* err_msg);

    // 自动加入策略（与旧 demo 的 Redis users:all/rooms:all 语义对应）
    // - 新用户：加入所有已存在房间
    // - 新房间：把所有用户加入该房间
    bool AutoJoinAllRoomsForUser(int64_t user_id, std::string* err_msg);
    bool AutoJoinRoomForAllUsers(int64_t room_id, std::string* err_msg);

   private:
    // MySQL 连接池（DAO 不持有长连接）
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush


