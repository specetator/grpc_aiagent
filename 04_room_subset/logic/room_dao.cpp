// ============================================================================
// Room/Group DAO 实现：MySQL 持久化 im_group / group_member
// - 使用 MySQL C API 预编译语句防 SQL 注入
// - 仅实现本子集（04_room_subset）所需的最小功能
// ============================================================================
#include "room_dao.h"

#include <mysql/mysql.h>

#include <cstring>

#include "logging.h"

namespace sparkpush {

bool RoomDao::ListRoomIds(std::vector<int64_t>* room_ids,
                          std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (!room_ids) return false;
    room_ids->clear();

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql = "SELECT id FROM im_group ORDER BY id DESC";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListRoomIds prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListRoomIds execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListRoomIds store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    long long id_buf = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[0].is_unsigned = 0;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListRoomIds bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    while (true) {
        int r = mysql_stmt_fetch(stmt);
        if (r == MYSQL_NO_DATA) break;
        if (r != 0 && r != MYSQL_DATA_TRUNCATED) {
            std::string e = mysql_stmt_error(stmt);
            LOG_ERROR << "ListRoomIds fetch failed: " << e;
            if (err_msg) *err_msg = e;
            mysql_stmt_close(stmt);
            return false;
        }
        room_ids->push_back(static_cast<int64_t>(id_buf));
    }
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::CreateRoom(const std::string& name, int64_t owner_id,
                         int group_type, int64_t* room_id,
                         std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (name.empty() || owner_id <= 0) {
        if (err_msg) *err_msg = "invalid name/owner_id";
        return false;
    }

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "INSERT INTO im_group(name, owner_id, group_type) VALUES(?, ?, ?)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CreateRoom prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));

    unsigned long name_len = static_cast<unsigned long>(name.size());
    long long owner_param = static_cast<long long>(owner_id);
    int group_type_param = group_type;

    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(name.data());
    bind[0].buffer_length = name_len;
    bind[0].length = &name_len;

    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &owner_param;
    bind[1].is_unsigned = 0;

    bind[2].buffer_type = MYSQL_TYPE_LONG;
    bind[2].buffer = &group_type_param;
    bind[2].is_unsigned = 0;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CreateRoom bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CreateRoom execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (room_id) {
        *room_id = static_cast<int64_t>(mysql_insert_id(conn));
    }
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::ListRooms(std::vector<RoomInfo>* rooms, std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (!rooms) return false;
    rooms->clear();

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    // created_at_ms 用 UNIX_TIMESTAMP 转换，避免解析时间字符串
    const char* sql =
        "SELECT id, name, owner_id, group_type, "
        "UNIX_TIMESTAMP(created_at)*1000 "
        "FROM im_group ORDER BY id DESC";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListRooms prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListRooms execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListRooms store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[5];
    memset(result, 0, sizeof(result));

    long long id_buf = 0;
    char name_buf[256] = {0};
    unsigned long name_out_len = 0;
    long long owner_buf = 0;
    int group_type_buf = 1;
    long long created_ms_buf = 0;

    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[0].is_unsigned = 0;

    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = name_buf;
    result[1].buffer_length = sizeof(name_buf);
    result[1].length = &name_out_len;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &owner_buf;
    result[2].is_unsigned = 0;

    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &group_type_buf;
    result[3].is_unsigned = 0;

    result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    result[4].buffer = &created_ms_buf;
    result[4].is_unsigned = 0;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListRooms bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    while (true) {
        int r = mysql_stmt_fetch(stmt);
        if (r == MYSQL_NO_DATA) break;
        if (r != 0 && r != MYSQL_DATA_TRUNCATED) {
            std::string e = mysql_stmt_error(stmt);
            LOG_ERROR << "ListRooms fetch failed: " << e;
            if (err_msg) *err_msg = e;
            mysql_stmt_close(stmt);
            return false;
        }
        RoomInfo info;
        info.id = id_buf;
        info.name.assign(name_buf, name_out_len);
        info.owner_id = owner_buf;
        info.group_type = group_type_buf;
        info.created_at_ms = created_ms_buf;
        rooms->push_back(std::move(info));
    }

    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::GetRoomById(int64_t room_id, RoomInfo* room,
                          std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (!room || room_id <= 0) {
        if (err_msg) *err_msg = "invalid room_id";
        return false;
    }

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT id, name, owner_id, group_type, UNIX_TIMESTAMP(created_at)*1000 "
        "FROM im_group WHERE id = ? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomById prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long rid_param = static_cast<long long>(room_id);
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &rid_param;
    param[0].is_unsigned = 0;
    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomById bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomById execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomById store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_num_rows(stmt) == 0) {
        if (err_msg) *err_msg = "room not found";
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[5];
    memset(result, 0, sizeof(result));
    long long id_buf = 0;
    char name_buf[256] = {0};
    unsigned long name_out_len = 0;
    long long owner_buf = 0;
    int group_type_buf = 1;
    long long created_ms_buf = 0;

    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &id_buf;
    result[0].is_unsigned = 0;

    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = name_buf;
    result[1].buffer_length = sizeof(name_buf);
    result[1].length = &name_out_len;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &owner_buf;
    result[2].is_unsigned = 0;

    result[3].buffer_type = MYSQL_TYPE_LONG;
    result[3].buffer = &group_type_buf;
    result[3].is_unsigned = 0;

    result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    result[4].buffer = &created_ms_buf;
    result[4].is_unsigned = 0;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomById bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "GetRoomById fetch failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    room->id = id_buf;
    room->name.assign(name_buf, name_out_len);
    room->owner_id = owner_buf;
    room->group_type = group_type_buf;
    room->created_at_ms = created_ms_buf;
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::AddMember(int64_t room_id, int64_t user_id, int role,
                        std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (room_id <= 0 || user_id <= 0) {
        if (err_msg) *err_msg = "invalid room_id/user_id";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "INSERT IGNORE INTO group_member(group_id, user_id, role) "
        "VALUES(?, ?, ?)";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AddMember prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    long long rid_param = static_cast<long long>(room_id);
    long long uid_param = static_cast<long long>(user_id);
    int role_param = role;

    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &rid_param;
    bind[0].is_unsigned = 0;

    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &uid_param;
    bind[1].is_unsigned = 0;

    bind[2].buffer_type = MYSQL_TYPE_LONG;
    bind[2].buffer = &role_param;
    bind[2].is_unsigned = 0;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AddMember bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AddMember execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::RemoveMember(int64_t room_id, int64_t user_id,
                           std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (room_id <= 0 || user_id <= 0) {
        if (err_msg) *err_msg = "invalid room_id/user_id";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql = "DELETE FROM group_member WHERE group_id=? AND user_id=?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "RemoveMember prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    long long rid_param = static_cast<long long>(room_id);
    long long uid_param = static_cast<long long>(user_id);
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &rid_param;
    bind[0].is_unsigned = 0;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &uid_param;
    bind[1].is_unsigned = 0;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "RemoveMember bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "RemoveMember execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::IsMember(int64_t room_id, int64_t user_id, bool* is_member,
                       std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (!is_member) return false;
    *is_member = false;
    if (room_id <= 0 || user_id <= 0) return true;

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "SELECT 1 FROM group_member WHERE group_id=? AND user_id=? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
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
    long long rid_param = static_cast<long long>(room_id);
    long long uid_param = static_cast<long long>(user_id);
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &rid_param;
    param[0].is_unsigned = 0;
    param[1].buffer_type = MYSQL_TYPE_LONGLONG;
    param[1].buffer = &uid_param;
    param[1].is_unsigned = 0;
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
    *is_member = (mysql_stmt_num_rows(stmt) > 0);
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::ListMembers(int64_t room_id, std::vector<int64_t>* user_ids,
                          std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (!user_ids) return false;
    user_ids->clear();
    if (room_id <= 0) return true;

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql = "SELECT user_id FROM group_member WHERE group_id=?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListMembers prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long rid_param = static_cast<long long>(room_id);
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &rid_param;
    param[0].is_unsigned = 0;
    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListMembers bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListMembers execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListMembers store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    long long uid_buf = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &uid_buf;
    result[0].is_unsigned = 0;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "ListMembers bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    while (true) {
        int r = mysql_stmt_fetch(stmt);
        if (r == MYSQL_NO_DATA) break;
        if (r != 0 && r != MYSQL_DATA_TRUNCATED) {
            std::string e = mysql_stmt_error(stmt);
            LOG_ERROR << "ListMembers fetch failed: " << e;
            if (err_msg) *err_msg = e;
            mysql_stmt_close(stmt);
            return false;
        }
        user_ids->push_back(static_cast<int64_t>(uid_buf));
    }
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::CountMembers(int64_t room_id, int64_t* count,
                           std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (!count) return false;
    *count = 0;
    if (room_id <= 0) return true;

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql = "SELECT COUNT(*) FROM group_member WHERE group_id=?";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CountMembers prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long rid_param = static_cast<long long>(room_id);
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &rid_param;
    param[0].is_unsigned = 0;
    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CountMembers bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CountMembers execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CountMembers store_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_num_rows(stmt) == 0) {
        mysql_stmt_close(stmt);
        *count = 0;
        return true;
    }

    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    long long cnt_buf = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &cnt_buf;
    result[0].is_unsigned = 0;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CountMembers bind_result failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "CountMembers fetch failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    *count = static_cast<int64_t>(cnt_buf);
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::AutoJoinAllRoomsForUser(int64_t user_id, std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (user_id <= 0) return true;
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "INSERT IGNORE INTO group_member(group_id, user_id, role) "
        "SELECT id, ?, 0 FROM im_group";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AutoJoinAllRoomsForUser prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long uid_param = static_cast<long long>(user_id);
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &uid_param;
    param[0].is_unsigned = 0;
    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AutoJoinAllRoomsForUser bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AutoJoinAllRoomsForUser execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool RoomDao::AutoJoinRoomForAllUsers(int64_t room_id, std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (room_id <= 0) return true;
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql =
        "INSERT IGNORE INTO group_member(group_id, user_id, role) "
        "SELECT ?, id, 0 FROM user";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AutoJoinRoomForAllUsers prepare failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND param[1];
    memset(param, 0, sizeof(param));
    long long rid_param = static_cast<long long>(room_id);
    param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    param[0].buffer = &rid_param;
    param[0].is_unsigned = 0;
    if (mysql_stmt_bind_param(stmt, param) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AutoJoinRoomForAllUsers bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "AutoJoinRoomForAllUsers execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush


