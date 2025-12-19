// ============================================================================
// 房间数据访问对象（Room DAO）
//
// 封装房间（im_group）和成员（group_member）表的 CRUD 操作
//
// 数据库表结构：
//   im_group: 房间基本信息（id, name, owner_id, group_type, created_at）
//   group_member: 房间成员关系（id, group_id, user_id, role, join_at）
//
// 设计考虑：
// - 使用 MySQL 预编译语句防止 SQL 注入
// - 支持事务操作（创建房间时自动加入创建者）
// - 预留分库分表扩展能力
// ============================================================================
#pragma once

#include <string>
#include <vector>

#include "mysql_pool.h"

namespace sparkpush {

// 房间信息结构体
struct Room {
    int64_t id{0};        // 房间 ID
    std::string name;     // 房间名称
    int64_t owner_id{0};  // 创建者用户 ID
    int group_type{1};    // 房间类型：1=聊天室
};

// 房间成员信息结构体
struct RoomMember {
    int64_t id{0};        // 记录 ID
    int64_t group_id{0};  // 房间 ID
    int64_t user_id{0};   // 用户 ID
    std::string role;     // 角色：owner/admin/member
};

// 房间 DAO：负责房间和成员数据的持久化操作
class RoomDao {
   public:
    // 构造函数：注入 MySQL 连接池
    explicit RoomDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 创建房间（带事务：创建房间 + 自动加入创建者为 owner）
    // @param name: 房间名称
    // @param owner_id: 创建者用户 ID
    // @param group_type: 房间类型（默认 1=聊天室）
    // @param room_id: 输出参数，返回新创建的房间 ID
    // @param err_msg: 输出参数，错误消息
    // @return: 成功返回 true，失败返回 false
    bool CreateRoom(const std::string& name, int64_t owner_id, int group_type,
                    int64_t* room_id, std::string* err_msg);

    // 查询房间信息
    // @param room_id: 房间 ID
    // @param room: 输出参数，返回房间信息
    // @param err_msg: 输出参数，错误消息
    // @return: 找到返回 true，未找到或出错返回 false
    bool GetRoom(int64_t room_id, Room* room, std::string* err_msg);

    // 用户加入房间
    // @param group_id: 房间 ID
    // @param user_id: 用户 ID
    // @param role: 角色（默认 "member"）
    // @param err_msg: 输出参数，错误消息
    // @return: 成功返回 true，失败（例如重复加入）返回 false
    bool JoinRoom(int64_t group_id, int64_t user_id, const std::string& role,
                  std::string* err_msg);

    // 用户离开房间
    // @param group_id: 房间 ID
    // @param user_id: 用户 ID
    // @param err_msg: 输出参数，错误消息
    // @return: 成功返回 true，失败返回 false
    bool LeaveRoom(int64_t group_id, int64_t user_id, std::string* err_msg);

    // 查询房间所有成员的用户 ID 列表
    // @param group_id: 房间 ID
    // @param user_ids: 输出参数，返回成员用户 ID 列表
    // @param err_msg: 输出参数，错误消息
    // @return: 成功返回 true（即使列表为空），失败返回 false
    bool GetRoomMembers(int64_t group_id, std::vector<int64_t>* user_ids,
                        std::string* err_msg);

    // 检查用户是否是房间成员
    // @param group_id: 房间 ID
    // @param user_id: 用户 ID
    // @param err_msg: 输出参数，错误消息
    // @return: 是成员返回 true，不是或出错返回 false
    bool IsMember(int64_t group_id, int64_t user_id, std::string* err_msg);

    // 查询用户已加入的房间列表
    // @param user_id: 用户 ID
    // @param rooms: 输出参数，返回房间列表
    // @param err_msg: 输出参数，错误消息
    // @return: 成功返回 true（即使列表为空），失败返回 false
    bool GetUserRooms(int64_t user_id, std::vector<Room>* rooms,
                      std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;  // MySQL 连接池
};

}  // namespace sparkpush
