#include "user_session_state_dao.h"

#include "logging.h"

namespace sparkpush {

bool UserSessionStateDao::UpsertReadSeq(int64_t user_id,
                                        const std::string& session_id,
                                        int64_t read_seq,
                                        std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (user_id <= 0 || session_id.empty() || read_seq < 0) {
        if (err_msg) *err_msg = "invalid parameters";
        return false;
    }

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "failed to acquire mysql connection";
        return false;
    }

    // SQL: INSERT ... ON DUPLICATE KEY UPDATE
    // 只有当新的 read_seq 更大时才更新
    const char* sql =
        "INSERT INTO user_session_state(user_id, session_id, read_seq, updated_at) "
        "VALUES(?, ?, ?, NOW()) "
        "ON DUPLICATE KEY UPDATE "
        "read_seq = IF(VALUES(read_seq) > read_seq, VALUES(read_seq), read_seq), "
        "updated_at = NOW()";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = std::string("mysql_stmt_prepare failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));

    // user_id (BIGINT)
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &user_id;

    // session_id (VARCHAR)
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(session_id.c_str());
    bind[1].buffer_length = session_id.size();

    // read_seq (BIGINT)
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &read_seq;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = std::string("mysql_stmt_bind_param failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = std::string("mysql_stmt_execute failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    mysql_stmt_close(stmt);
    return true;
}

bool UserSessionStateDao::GetReadSeq(int64_t user_id,
                                     const std::string& session_id,
                                     int64_t* read_seq, std::string* err_msg) {
    if (!pool_) {
        if (err_msg) *err_msg = "mysql pool not initialized";
        return false;
    }
    if (!read_seq) {
        if (err_msg) *err_msg = "read_seq pointer is null";
        return false;
    }
    if (user_id <= 0 || session_id.empty()) {
        if (err_msg) *err_msg = "invalid parameters";
        return false;
    }

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "failed to acquire mysql connection";
        return false;
    }

    const char* sql =
        "SELECT read_seq FROM user_session_state "
        "WHERE user_id = ? AND session_id = ?";

    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = std::string("mysql_stmt_prepare failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND bind_param[2];
    memset(bind_param, 0, sizeof(bind_param));

    // user_id
    bind_param[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_param[0].buffer = &user_id;

    // session_id
    bind_param[1].buffer_type = MYSQL_TYPE_STRING;
    bind_param[1].buffer = const_cast<char*>(session_id.c_str());
    bind_param[1].buffer_length = session_id.size();

    if (mysql_stmt_bind_param(stmt, bind_param) != 0) {
        if (err_msg) *err_msg = std::string("mysql_stmt_bind_param failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = std::string("mysql_stmt_execute failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    // 绑定结果
    MYSQL_BIND bind_result[1];
    memset(bind_result, 0, sizeof(bind_result));

    int64_t result_seq = 0;
    bind_result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind_result[0].buffer = &result_seq;

    if (mysql_stmt_bind_result(stmt, bind_result) != 0) {
        if (err_msg) *err_msg = std::string("mysql_stmt_bind_result failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = std::string("mysql_stmt_store_result failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    int fetch_result = mysql_stmt_fetch(stmt);
    if (fetch_result == 0) {
        // 查询到数据
        *read_seq = result_seq;
        mysql_stmt_close(stmt);
        return true;
    } else if (fetch_result == MYSQL_NO_DATA) {
        // 没有数据，返回 0
        *read_seq = 0;
        mysql_stmt_close(stmt);
        return true;
    } else {
        if (err_msg) *err_msg = std::string("mysql_stmt_fetch failed: ") + mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
}

}  // namespace sparkpush


