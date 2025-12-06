#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mysql_pool.h"

namespace sparkpush {

// 对应 sql/schema.sql 中的 im_group / group_member 表，当前仅使用 group_type=1
// 作为聊天室
struct ImGroup {
    int64_t id{0};
    std::string name;
    int64_t owner_id{0};
    int group_type{0};
};

class GroupDao {
   public:
    explicit GroupDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 功能：创建聊天室（group_type=1）
    // 参数：name 房间名；owner_id 创建者；group_id 输出新房间 id；err_msg
    // 错误信息 返回：成功 true，失败 false
    bool CreateChatroom(const std::string& name, int64_t owner_id,
                        int64_t* group_id, std::string* err_msg);

    // 功能：按 id 查询聊天室信息
    // 参数：group_id 房间 id；g 输出房间结构；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool GetGroup(int64_t group_id, ImGroup* g, std::string* err_msg);

    // 功能：分页列出聊天室列表
    // 参数：offset 偏移；limit 数量；groups 输出列表；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool ListChatrooms(int offset, int limit, std::vector<ImGroup>* groups,
                       std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

class GroupMemberDao {
   public:
    explicit GroupMemberDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 功能：添加或更新聊天室成员角色
    // 参数：group_id 房间；user_id 用户；role 角色；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool AddOrUpdateMember(int64_t group_id, int64_t user_id, int role,
                           std::string* err_msg);

    // 功能：从成员表移除用户（取消订阅）
    // 参数：group_id 房间；user_id 用户；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool RemoveMember(int64_t group_id, int64_t user_id, std::string* err_msg);

    // 功能：查询用户订阅的聊天室 ID（仅 group_type=1）
    // 参数：user_id 用户；group_ids 输出房间 id；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool ListUserChatrooms(int64_t user_id, std::vector<int64_t>* group_ids,
                           std::string* err_msg);

    // 功能：列出聊天室成员列表
    // 参数：group_id 房间；user_ids 输出成员；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool ListRoomMembers(int64_t group_id, std::vector<int64_t>* user_ids,
                         std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
