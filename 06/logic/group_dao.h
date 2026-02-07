#pragma once

#include "mysql_pool.h"

#include <cstdint>
#include <string>
#include <vector>

namespace sparkpush {

// 对应 sql/schema.sql 中的 im_group / group_member 表，当前仅使用 group_type=1 作为聊天室
struct ImGroup {
  int64_t id{0};
  std::string name;
  int64_t owner_id{0};
  int group_type{0};
};

class GroupDao {
 public:
  explicit GroupDao(MySqlConnectionPool* pool) : pool_(pool) {}

  // 创建聊天室（group_type=1）
  bool CreateChatroom(const std::string& name,
                      int64_t owner_id,
                      int64_t* group_id,
                      std::string* err_msg);

  bool GetGroup(int64_t group_id, ImGroup* g, std::string* err_msg);

  bool ListChatrooms(int offset,
                     int limit,
                     std::vector<ImGroup>* groups,
                     std::string* err_msg);

 private:
  MySqlConnectionPool* pool_;
};

class GroupMemberDao {
 public:
  explicit GroupMemberDao(MySqlConnectionPool* pool) : pool_(pool) {}

  // 将用户加入到聊天室成员表（若已存在则忽略）
  bool AddOrUpdateMember(int64_t group_id,
                         int64_t user_id,
                         int role,
                         std::string* err_msg);

  // 从聊天室成员表移除（用于取消订阅）
  bool RemoveMember(int64_t group_id,
                    int64_t user_id,
                    std::string* err_msg);

  // 查询某个用户订阅的所有聊天室 ID（仅 group_type=1）
  bool ListUserChatrooms(int64_t user_id,
                         std::vector<int64_t>* group_ids,
                         std::string* err_msg);

  bool ListRoomMembers(int64_t group_id,
                       std::vector<int64_t>* user_ids,
                       std::string* err_msg);

 private:
  MySqlConnectionPool* pool_;
};

}  // namespace sparkpush



