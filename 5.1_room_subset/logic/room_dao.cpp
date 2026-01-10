// ============================================================================
// 房间 DAO 实现
//
// 使用 MySQL C API 的预编译语句（Prepared Statement）执行数据库操作
// ============================================================================
#include "room_dao.h"

#include <mysql/mysql.h>

#include "logging.h"

namespace sparkpush {

// ============================================================================
// CreateRoom: 创建房间（事务：创建房间 + 自动加入创建者）
// ============================================================================
bool RoomDao::CreateRoom(const std::string& name, int64_t owner_id,
                         int group_type, int64_t* room_id,
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

    // 开始事务
    if (mysql_query(conn, "START TRANSACTION") != 0) {
        std::string e = mysql_error(conn);
        LOG_ERROR << "START TRANSACTION failed: " << e;
        if (err_msg) *err_msg = e;
        return false;
    }

    // 步骤1：插入房间记录
    const char* sql1 =
        "INSERT INTO im_group(name, owner_id, group_type) VALUES(?, ?, ?)";
    MYSQL_STMT* stmt1 = mysql_stmt_init(conn);
    if (!stmt1) {
        std::string e = "mysql_stmt_init failed";
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    if (mysql_stmt_prepare(stmt1, sql1,
                           static_cast<unsigned long>(strlen(sql1))) != 0) {
        std::string e = mysql_stmt_error(stmt1);
        LOG_ERROR << "CreateRoom prepare im_group failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt1);
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    MYSQL_BIND bind1[3];
    memset(bind1, 0, sizeof(bind1));

    unsigned long name_len = static_cast<unsigned long>(name.size());
    bind1[0].buffer_type = MYSQL_TYPE_STRING;
    bind1[0].buffer = const_cast<char*>(name.data());
    bind1[0].buffer_length = name_len;
    bind1[0].length = &name_len;

    bind1[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind1[1].buffer = &owner_id;

    bind1[2].buffer_type = MYSQL_TYPE_LONG;
    bind1[2].buffer = &group_type;

    if (mysql_stmt_bind_param(stmt1, bind1) != 0) {
        std::string e = mysql_stmt_error(stmt1);
        LOG_ERROR << "CreateRoom bind_param im_group failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt1);
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    if (mysql_stmt_execute(stmt1) != 0) {
        std::string e = mysql_stmt_error(stmt1);
        LOG_ERROR << "CreateRoom execute im_group failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt1);
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    // 获取新创建的房间 ID
    int64_t new_room_id = static_cast<int64_t>(mysql_insert_id(conn));
    mysql_stmt_close(stmt1);

    // 步骤2：自动加入创建者为 owner
    const char* sql2 =
        "INSERT INTO group_member(group_id, user_id, role) VALUES(?, ?, 'owner')";
    MYSQL_STMT* stmt2 = mysql_stmt_init(conn);
    if (!stmt2) {
        std::string e = "mysql_stmt_init failed for group_member";
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    if (mysql_stmt_prepare(stmt2, sql2,
                           static_cast<unsigned long>(strlen(sql2))) != 0) {
        std::string e = mysql_stmt_error(stmt2);
        LOG_ERROR << "CreateRoom prepare group_member failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt2);
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    MYSQL_BIND bind2[2];
    memset(bind2, 0, sizeof(bind2));

    bind2[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind2[0].buffer = &new_room_id;

    bind2[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind2[1].buffer = &owner_id;

    if (mysql_stmt_bind_param(stmt2, bind2) != 0) {
        std::string e = mysql_stmt_error(stmt2);
        LOG_ERROR << "CreateRoom bind_param group_member failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt2);
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    if (mysql_stmt_execute(stmt2) != 0) {
        std::string e = mysql_stmt_error(stmt2);
        LOG_ERROR << "CreateRoom execute group_member failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt2);
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    mysql_stmt_close(stmt2);

    // 提交事务
    if (mysql_query(conn, "COMMIT") != 0) {
        std::string e = mysql_error(conn);
        LOG_ERROR << "COMMIT failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_query(conn, "ROLLBACK");
        return false;
    }

    if (room_id) {
        *room_id = new_room_id;
    }

    LOG_INFO << "CreateRoom success, room_id=" << new_room_id
             << ", owner_id=" << owner_id;
    return true;
}

// ============================================================================
// GetRoom: 查询房间信息
// ============================================================================
bool RoomDao::GetRoom(int64_t room_id, Room* room, std::string* err_msg) {
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

    const char* sql =
        "SELECT id, name, owner_id, group_type FROM im_group WHERE id = ? LIMIT 1";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        std::string e = "mysql_stmt_init failed";
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoom prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &room_id;

    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoom bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoom execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoom store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    my_ulonglong row_count = mysql_stmt_num_rows(stmt);
    if (row_count == 0) {
        if (err_msg) *err_msg = "room not found";
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[4];
    memset(result, 0, sizeof(result));

    long long id_buf = 0;
    char name_buf[256] = {0};
    long long owner_id_buf = 0;
    int type_buf = 0;
    unsigned long name_len = 0;

    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;

    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = name_buf;
    result[1].buffer_length = sizeof(name_buf);
    result[1].length = &name_len;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &owner_id_buf;

    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &type_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoom bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_fetch(stmt) == 0) {
        if (room) {
            room->id = id_buf;
            room->name = std::string(name_buf, name_len);
            room->owner_id = owner_id_buf;
            room->group_type = type_buf;
        }
        mysql_stmt_close(stmt);
        return true;
    }

    if (err_msg) *err_msg = "fetch row failed";
    mysql_stmt_close(stmt);
    return false;
}

// ============================================================================
// JoinRoom: 用户加入房间
// ============================================================================
bool RoomDao::JoinRoom(int64_t group_id, int64_t user_id,
                       const std::string& role, std::string* err_msg) {
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

    const char* sql =
        "INSERT INTO group_member(group_id, user_id, role) VALUES(?, ?, ?)";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        std::string e = "mysql_stmt_init failed";
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "JoinRoom prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));

    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &group_id;

    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &user_id;

    unsigned long role_len = static_cast<unsigned long>(role.size());
    bind[2].buffer_type = MYSQL_TYPE_STRING;
    bind[2].buffer = const_cast<char*>(role.data());
    bind[2].buffer_length = role_len;
    bind[2].length = &role_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "JoinRoom bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "JoinRoom execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    mysql_stmt_close(stmt);
    LOG_INFO << "JoinRoom success, group_id=" << group_id
             << ", user_id=" << user_id << ", role=" << role;
    return true;
}

// ============================================================================
// LeaveRoom: 用户离开房间
// ============================================================================
bool RoomDao::LeaveRoom(int64_t group_id, int64_t user_id,
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

    const char* sql =
        "DELETE FROM group_member WHERE group_id = ? AND user_id = ?";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        std::string e = "mysql_stmt_init failed";
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "LeaveRoom prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));

    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &group_id;

    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &user_id;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "LeaveRoom bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "LeaveRoom execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    // 若未删除任何行，说明用户本来就不在房间内
    my_ulonglong affected = mysql_stmt_affected_rows(stmt);
    if (affected == 0) {
        if (err_msg) *err_msg = "not room member";
        mysql_stmt_close(stmt);
        return false;
    }

    mysql_stmt_close(stmt);
    LOG_INFO << "LeaveRoom success, group_id=" << group_id
             << ", user_id=" << user_id;
    return true;
}

// ============================================================================
// GetRoomMembers: 查询房间所有成员的用户 ID 列表
// ============================================================================
bool RoomDao::GetRoomMembers(int64_t group_id, std::vector<int64_t>* user_ids,
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

    const char* sql = "SELECT user_id FROM group_member WHERE group_id = ?";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        std::string e = "mysql_stmt_init failed";
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomMembers prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &group_id;

    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomMembers bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomMembers execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomMembers store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));

    long long user_id_buf = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &user_id_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomMembers bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (user_ids) {
        user_ids->clear();
        while (mysql_stmt_fetch(stmt) == 0) {
            user_ids->push_back(user_id_buf);
        }
    }

    mysql_stmt_close(stmt);
    return true;
}

// ============================================================================
// IsMember: 检查用户是否是房间成员
// ============================================================================
bool RoomDao::IsMember(int64_t group_id, int64_t user_id,
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

    const char* sql =
        "SELECT COUNT(*) FROM group_member WHERE group_id = ? AND user_id = ?";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        std::string e = "mysql_stmt_init failed";
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "IsMember prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND param[2];
    memset(param, 0, sizeof(param));

    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &group_id;

    param[1].buffer_type = MYSQL_TYPE_LONGLONG;
    param[1].buffer = &user_id;

    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "IsMember bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "IsMember execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "IsMember store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));

    long long count = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &count;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "IsMember bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    bool is_member = false;
    if (mysql_stmt_fetch(stmt) == 0) {
        is_member = (count > 0);
    }

    mysql_stmt_close(stmt);
    return is_member;
}

// ============================================================================
// GetUserRooms: 查询用户已加入的房间列表
// ============================================================================
bool RoomDao::GetUserRooms(int64_t user_id, std::vector<Room>* rooms,
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

    // 说明：为了接口可用性，这里只返回用户作为成员的房间基本信息
    // 依赖：group_member(group_id,user_id)、im_group(id,name,owner_id,group_type)
    const char* sql =
        "SELECT g.id, g.name, g.owner_id, g.group_type "
        "FROM group_member m "
        "JOIN im_group g ON g.id = m.group_id "
        "WHERE m.user_id = ? "
        "ORDER BY g.id DESC";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        std::string e = "mysql_stmt_init failed";
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetUserRooms prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &user_id;

    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetUserRooms bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetUserRooms execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetUserRooms store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[4];
    memset(result, 0, sizeof(result));

    long long id_buf = 0;
    char name_buf[256] = {0};
    unsigned long name_len = 0;
    long long owner_id_buf = 0;
    int type_buf = 0;

    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;

    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = name_buf;
    result[1].buffer_length = sizeof(name_buf);
    result[1].length = &name_len;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &owner_id_buf;

    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &type_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetUserRooms bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (rooms) rooms->clear();
    while (mysql_stmt_fetch(stmt) == 0) {
        if (!rooms) continue;
        Room r;
        r.id = id_buf;
        r.name = std::string(name_buf, name_len);
        r.owner_id = owner_id_buf;
        r.group_type = type_buf;
        rooms->push_back(std::move(r));
    }

    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush

