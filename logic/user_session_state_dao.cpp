#include "user_session_state_dao.h"

#include <mysql/mysql.h>

#include <cstring>

#include "logging.h"

namespace sparkpush {

// 插入或更新用户会话已读序列
bool UserSessionStateDao::UpsertReadSeq(int64_t user_id,
                                        const std::string& session_id,
                                        int64_t read_seq,
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
        "INSERT INTO user_session_state(user_id, session_id, read_seq) "
        "VALUES(?, ?, ?) "
        "ON DUPLICATE KEY UPDATE read_seq=GREATEST(read_seq, "
        "VALUES(read_seq)), "
        "last_visit_at=CURRENT_TIMESTAMP";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    long long uid_buf = user_id;
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &uid_buf;

    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(session_id.data());
    bind[1].buffer_length = sid_len;
    bind[1].length = &sid_len;

    long long read_buf = read_seq;
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &read_buf;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

// 查询用户会话已读序列
bool UserSessionStateDao::GetReadSeq(int64_t user_id,
                                     const std::string& session_id,
                                     int64_t* read_seq, std::string* err_msg) {
    if (!pool_ || !read_seq) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "SELECT read_seq FROM user_session_state "
        "WHERE user_id=? AND session_id=? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    long long uid_buf = user_id;
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &uid_buf;

    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(session_id.data());
    bind[1].buffer_length = sid_len;
    bind[1].length = &sid_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    long long read_buf = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &read_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret == MYSQL_NO_DATA) {
        *read_seq = 0;
        mysql_stmt_close(stmt);
        return true;
    }
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    *read_seq = read_buf;
    mysql_stmt_close(stmt);
    return true;
}

bool UserSessionStateDao::UpsertDeliveredSeq(
    int64_t user_id, const std::string& session_id, int64_t delivered_seq,
    std::string* err_msg) {
    if (!pool_ || delivered_seq < 0) {
        if (err_msg) *err_msg = "invalid delivered cursor arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "INSERT INTO user_session_state(user_id, session_id, read_seq, delivered_seq) "
        "VALUES(?, ?, 0, ?) ON DUPLICATE KEY UPDATE "
        "delivered_seq=GREATEST(delivered_seq, VALUES(delivered_seq)), "
        "last_visit_at=CURRENT_TIMESTAMP";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[3];
    memset(bind, 0, sizeof(bind));
    long long uid_buf = user_id;
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &uid_buf;
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(session_id.data());
    bind[1].buffer_length = sid_len;
    bind[1].length = &sid_len;
    long long seq_buf = delivered_seq;
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &seq_buf;
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool UserSessionStateDao::GetDeliveredSeq(
    int64_t user_id, const std::string& session_id, int64_t* delivered_seq,
    std::string* err_msg) {
    if (!pool_ || !delivered_seq) {
        if (err_msg) *err_msg = "invalid delivered cursor arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "SELECT delivered_seq FROM user_session_state "
        "WHERE user_id=? AND session_id=? LIMIT 1";
    MYSQL_STMT* stmt = mysql_stmt_init(conn);
    if (!stmt) {
        if (err_msg) *err_msg = "mysql_stmt_init failed";
        return false;
    }
    if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND bind[2];
    memset(bind, 0, sizeof(bind));
    long long uid_buf = user_id;
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &uid_buf;
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(session_id.data());
    bind[1].buffer_length = sid_len;
    bind[1].length = &sid_len;
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    long long seq_buf = 0;
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &seq_buf;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret == MYSQL_NO_DATA) {
        *delivered_seq = 0;
        mysql_stmt_close(stmt);
        return true;
    }
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    *delivered_seq = seq_buf;
    mysql_stmt_close(stmt);
    return true;
}

bool UserSessionStateDao::EnsureDeliveredSeqColumn(std::string* err_msg) {
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
    // 兼容 MySQL 8.0 的不同小版本：部分版本不支持 ADD COLUMN IF NOT
    // EXISTS，因此用 1060（Duplicate column）作为幂等成功结果。
    const char* sql =
        "ALTER TABLE user_session_state ADD COLUMN delivered_seq "
        "BIGINT NOT NULL DEFAULT 0 AFTER read_seq";
    if (mysql_query(conn, sql) != 0) {
        if (mysql_errno(conn) == 1060) return true;
        if (err_msg) *err_msg = mysql_error(conn);
        return false;
    }
    return true;
}

}  // namespace sparkpush
