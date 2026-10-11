#include "user_session_state_dao.h"
#include "mysql_statement.h"
#include "message_reservation.h"
#include <algorithm>

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

bool UserSessionStateDao::UpsertDeliveredSeq(int64_t user, const std::string& session,
                                              int64_t seq, std::string* error) {
    int64_t advanced = 0;
    return AdvanceDelivered(user, session, seq, &advanced, error);
}
bool UserSessionStateDao::GetDeliveredSeq(int64_t user, const std::string& session,
                                         int64_t* seq, std::string* error) {
    return AdvanceDelivered(user, session, 0, seq, error);
}
bool UserSessionStateDao::AdvanceDelivered(int64_t user, const std::string& session,
                                           int64_t receipt, int64_t* seq, std::string* error) {
    if (!pool_ || !seq || user <= 0 || session.empty() || receipt < 0) return false;
    auto guard = pool_->Acquire();
    MYSQL* connection = guard.get();
    if (!connection) { if (error) *error = "no MySQL delivery connection"; return false; }
    SqlTransaction transaction(connection);
    if (!transaction.active()) return false;
    const auto uid = std::to_string(user);
    if (!SqlExecute(connection, "INSERT INTO user_session_state(user_id,session_id,read_seq,delivered_seq) VALUES(?,?,0,0) "
        "ON DUPLICATE KEY UPDATE delivered_seq=user_session_state.delivered_seq",
        {uid, session}, nullptr, error)) return false;
    std::vector<std::vector<std::string>> rows;
    if (!SqlExecute(connection, "SELECT delivered_seq FROM user_session_state WHERE user_id=? AND session_id=? FOR UPDATE",
        {uid, session}, &rows, error) || rows.size() != 1) return false;
    const int64_t previous = std::stoll(rows[0][0]);
    if (receipt > previous && !SqlExecute(connection,
        "INSERT IGNORE INTO message_delivery_receipt(user_id,session_id,msg_seq) VALUES(?,?,?)",
        {uid, session, std::to_string(receipt)}, nullptr, error)) return false;
    int64_t safe = 0;
    if (!MySqlMessageReservations::SafePrefix(connection, session, &safe, error)) return false;
    if (!SqlExecute(connection,
        "SELECT COALESCE(MIN(m.msg_seq)-1,?) FROM message m LEFT JOIN message_delivery_receipt r "
        "ON r.user_id=? AND r.session_id=m.session_id AND r.msg_seq=m.msg_seq "
        "WHERE m.session_id=? AND m.msg_seq>? AND m.msg_seq<=? AND r.msg_seq IS NULL",
        {std::to_string(safe), uid, session, std::to_string(previous), std::to_string(safe)}, &rows, error) || rows.size() != 1)
        return false;
    *seq = std::max<int64_t>(previous, std::stoll(rows[0][0]));
    if (!SqlExecute(connection, "UPDATE user_session_state SET delivered_seq=? WHERE user_id=? AND session_id=?",
        {std::to_string(*seq), uid, session}, nullptr, error) ||
        !SqlExecute(connection, "DELETE FROM message_delivery_receipt WHERE user_id=? AND session_id=? AND msg_seq<=?",
        {uid, session, std::to_string(*seq)}, nullptr, error)) return false;
    return transaction.Commit(error);
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
