#include "group_dao.h"

#include <sstream>

#include "logging.h"

namespace sparkpush {

// 功能：创建聊天室（group_type=1）
bool GroupDao::CreateChatroom(const std::string& name, int64_t owner_id,
                              int64_t* group_id, std::string* err_msg) {
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
    // 安全缺陷：name 直接拼进单引号里，**没有过 mysql_real_escape_string**，
    // 也没用占位符绑定。一个包含单引号的房间名就能截断语句并注入任意 SQL。
    // 当前只能靠上游兜：该路径是 /api/admin/chatroom/create，有 requireAdmin 门禁，
    // 且 RoomService 只校验了 name.empty()。不要把这个接口开给普通用户，
    // 也不要拷贝这段写法到新代码里。
    oss << "INSERT INTO im_group(name, owner_id, group_type) VALUES('" << name
        << "'," << owner_id << ",1)";
    std::string sql = oss.str();
    if (mysql_query(conn, sql.c_str()) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        LOG_ERROR << "CreateChatroom failed: " << std::string(mysql_error(conn));
        return false;
    }
    // 必须在连接还给池之前取 insert_id：它属于连接而不是结果集，
    // guard 析构后连接会被其他请求复用，晚取就可能拿到别人的自增值。
    if (group_id) {
        *group_id = static_cast<int64_t>(mysql_insert_id(conn));
    }
    return true;
}

// 功能：按 id 查询群信息
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
        LOG_ERROR << "GetGroup query failed: " << std::string(mysql_error(conn));
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

// 功能：分页列出聊天室列表
bool GroupDao::ListChatrooms(int offset, int limit,
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
        << " WHERE group_type=1" << " ORDER BY id DESC" << " LIMIT " << limit
        << " OFFSET " << offset;
    std::string sql = oss.str();
    if (mysql_query(conn, sql.c_str()) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        LOG_ERROR << "ListChatrooms query failed: "
                  << std::string(mysql_error(conn));
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

// 功能：添加或更新聊天室成员（存在则忽略）
bool GroupMemberDao::AddOrUpdateMember(int64_t group_id, int64_t user_id,
                                       int role, std::string* err_msg) {
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
    // 名实不符：下面**只有 INSERT IGNORE，没有任何 UPDATE role 的语句**。
    // 因此成员已存在时 role 会被静默忽略，函数返回 true 但什么都没改。
    // 另外 INSERT IGNORE 会吸收**所有**错误（包括数据截断、外键失败）
    // 而不只是主键冲突，真正的故障会被当成「已存在」静默吞掉。
    // 若要修，应改成 INSERT ... ON DUPLICATE KEY UPDATE role=VALUES(role)，
    // 并靠 affected_rows 区分「新插入(1)」与「已存在(0)」。
    oss << "INSERT IGNORE INTO group_member(group_id, user_id, role) VALUES("
        << group_id << "," << user_id << "," << role << ")";
    std::string sql = oss.str();
    if (mysql_query(conn, sql.c_str()) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        LOG_ERROR << "AddOrUpdateMember(insert) failed: "
                  << std::string(mysql_error(conn));
        return false;
    }
    return true;
}

// 功能：移除聊天室成员
bool GroupMemberDao::RemoveMember(int64_t group_id, int64_t user_id,
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
        LOG_ERROR << "RemoveMember failed: " << std::string(mysql_error(conn));
        return false;
    }
    return true;
}

// 功能：列出用户订阅的聊天室 ID
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
    oss << "SELECT gm.group_id" << " FROM group_member gm"
        << " JOIN im_group g ON gm.group_id=g.id"
        << " WHERE gm.user_id=" << user_id << " AND g.group_type=1";
    std::string sql = oss.str();
    if (mysql_query(conn, sql.c_str()) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        LOG_ERROR << "ListUserChatrooms query failed: "
                  << std::string(mysql_error(conn));
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

bool GroupMemberDao::IsMember(int64_t group_id, int64_t user_id,
                              bool* is_member, std::string* err_msg) {
    if (!is_member || group_id <= 0 || user_id <= 0) {
        if (err_msg) *err_msg = "invalid membership arguments";
        return false;
    }
    // 先置 false 再查：本方法是对象级鉴权的依据，必须 fail-closed。
    // 下面任何一条失败路径（拿不到连接、SQL 报错、store_result 失败）
    // 都会留下 *is_member == false，调用方因此会拒绝访问而不是放行。
    *is_member = false;
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
    // COUNT(*) 而不是 SELECT 1 LIMIT 1：两者都能回答「存不存在」，
    // 但 COUNT 的返回值不需要处理空结果集，下面的 row 判空逻辑更简单。
    oss << "SELECT COUNT(*) FROM group_member WHERE group_id=" << group_id
        << " AND user_id=" << user_id;
    if (mysql_query(conn, oss.str().c_str()) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        return false;
    }
    MYSQL_RES* res = mysql_store_result(conn);
    if (!res) {
        if (err_msg) *err_msg = "store_result failed";
        return false;
    }
    MYSQL_ROW row = mysql_fetch_row(res);
    if (row && row[0]) *is_member = std::stoll(row[0]) > 0;
    mysql_free_result(res);
    return true;
}

// 功能：列出聊天室成员。
// 这个列表是多处扇出逻辑的共同输入：
//   message_service  群聊/房间消息按成员查 comet_id 再写 push_group topic
//   danmaku_service  弹幕广播的目标集
//   room_service     在线人数统计的兜底路径
// 因此本查询的正确性直接决定了「谁能收到房间消息」。
// 注意没有 LIMIT：成员数无上限，超大房间会一次拉回全部 id。
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
        LOG_ERROR << "ListRoomMembers query failed: "
                  << std::string(mysql_error(conn));
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
