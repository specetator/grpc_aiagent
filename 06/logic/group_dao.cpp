#include "group_dao.h"

#include "logging.h"

#include <sstream>

namespace sparkpush {

bool GroupDao::CreateChatroom(const std::string& name,
                              int64_t owner_id,
                              int64_t* group_id,
                              std::string* err_msg) {
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }

  std::ostringstream oss;
  // group_type=1 表示聊天室
  oss << "INSERT INTO im_group(name, owner_id, group_type) VALUES('"
      << name << "'," << owner_id << ",1)";
  std::string sql = oss.str();
  if (mysql_query(conn, sql.c_str()) != 0) {
    if (err_msg) *err_msg = mysql_error(conn);
    LogError("CreateChatroom failed: " + std::string(mysql_error(conn)));
    return false;
  }
  if (group_id) {
    *group_id = static_cast<int64_t>(mysql_insert_id(conn));
  }
  return true;
}

bool GroupDao::GetGroup(int64_t group_id, ImGroup* g, std::string* err_msg) {
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }
  std::ostringstream oss;
  oss << "SELECT id, name, owner_id, group_type FROM im_group WHERE id="
      << group_id << " LIMIT 1";
  std::string sql = oss.str();
  if (mysql_query(conn, sql.c_str()) != 0) {
    if (err_msg) *err_msg = mysql_error(conn);
    LogError("GetGroup query failed: " + std::string(mysql_error(conn)));
    return false;
  }
  MYSQL_RES* res = mysql_store_result(conn);
  if (!res) {
    if (err_msg) *err_msg = "store_result failed";
    return false;
  }
  MYSQL_ROW row = mysql_fetch_row(res);
  if (!row) {
    mysql_free_result(res);
    if (err_msg) *err_msg = "group not found";
    return false;
  }
  if (g) {
    g->id = std::stoll(row[0]);
    g->name = row[1] ? row[1] : "";
    g->owner_id = row[2] ? std::stoll(row[2]) : 0;
    g->group_type = row[3] ? std::stoi(row[3]) : 0;
  }
  mysql_free_result(res);
  return true;
}

bool GroupDao::ListChatrooms(int offset,
                             int limit,
                             std::vector<ImGroup>* groups,
                             std::string* err_msg) {
  if (!groups) return false;
  groups->clear();
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }
  if (offset < 0) offset = 0;
  if (limit <= 0) limit = 50;
  std::ostringstream oss;
  oss << "SELECT id, name, owner_id, group_type FROM im_group"
      << " WHERE group_type=1"
      << " ORDER BY id DESC"
      << " LIMIT " << limit << " OFFSET " << offset;
  std::string sql = oss.str();
  if (mysql_query(conn, sql.c_str()) != 0) {
    if (err_msg) *err_msg = mysql_error(conn);
    LogError("ListChatrooms query failed: " + std::string(mysql_error(conn)));
    return false;
  }
  MYSQL_RES* res = mysql_store_result(conn);
  if (!res) {
    if (err_msg) *err_msg = "store_result failed";
    return false;
  }
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    ImGroup g;
    g.id = row[0] ? std::stoll(row[0]) : 0;
    g.name = row[1] ? row[1] : "";
    g.owner_id = row[2] ? std::stoll(row[2]) : 0;
    g.group_type = row[3] ? std::stoi(row[3]) : 0;
    groups->push_back(std::move(g));
  }
  mysql_free_result(res);
  return true;
}

bool GroupMemberDao::AddOrUpdateMember(int64_t group_id,
                                       int64_t user_id,
                                       int role,
                                       std::string* err_msg) {
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }
  std::ostringstream oss;
  // 简化：使用 INSERT IGNORE，再按需更新 role
  oss << "INSERT IGNORE INTO group_member(group_id, user_id, role) VALUES("
      << group_id << "," << user_id << "," << role << ")";
  std::string sql = oss.str();
  if (mysql_query(conn, sql.c_str()) != 0) {
    if (err_msg) *err_msg = mysql_error(conn);
    LogError("AddOrUpdateMember(insert) failed: " + std::string(mysql_error(conn)));
    return false;
  }
  return true;
}

bool GroupMemberDao::RemoveMember(int64_t group_id,
                                  int64_t user_id,
                                  std::string* err_msg) {
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }
  std::ostringstream oss;
  oss << "DELETE FROM group_member WHERE group_id=" << group_id
      << " AND user_id=" << user_id;
  std::string sql = oss.str();
  if (mysql_query(conn, sql.c_str()) != 0) {
    if (err_msg) *err_msg = mysql_error(conn);
    LogError("RemoveMember failed: " + std::string(mysql_error(conn)));
    return false;
  }
  return true;
}

bool GroupMemberDao::ListUserChatrooms(int64_t user_id,
                                       std::vector<int64_t>* group_ids,
                                       std::string* err_msg) {
  if (!group_ids) return false;
  group_ids->clear();
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }
  // 仅返回 group_type=1 的聊天室
  std::ostringstream oss;
  oss << "SELECT gm.group_id"
      << " FROM group_member gm"
      << " JOIN im_group g ON gm.group_id=g.id"
      << " WHERE gm.user_id=" << user_id
      << " AND g.group_type=1";
  std::string sql = oss.str();
  if (mysql_query(conn, sql.c_str()) != 0) {
    if (err_msg) *err_msg = mysql_error(conn);
    LogError("ListUserChatrooms query failed: " + std::string(mysql_error(conn)));
    return false;
  }
  MYSQL_RES* res = mysql_store_result(conn);
  if (!res) {
    if (err_msg) *err_msg = "store_result failed";
    return false;
  }
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    if (row[0]) {
      group_ids->push_back(std::stoll(row[0]));
    }
  }
  mysql_free_result(res);
  return true;
}

bool GroupMemberDao::ListRoomMembers(int64_t group_id,
                                     std::vector<int64_t>* user_ids,
                                     std::string* err_msg) {
  if (!user_ids) return false;
  user_ids->clear();
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }
  std::ostringstream oss;
  oss << "SELECT user_id FROM group_member WHERE group_id=" << group_id;
  std::string sql = oss.str();
  if (mysql_query(conn, sql.c_str()) != 0) {
    if (err_msg) *err_msg = mysql_error(conn);
    LogError("ListRoomMembers query failed: " + std::string(mysql_error(conn)));
    return false;
  }
  MYSQL_RES* res = mysql_store_result(conn);
  if (!res) {
    if (err_msg) *err_msg = "store_result failed";
    return false;
  }
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    if (row[0]) {
      user_ids->push_back(std::stoll(row[0]));
    }
  }
  mysql_free_result(res);
  return true;
}

}  // namespace sparkpush



