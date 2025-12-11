#include "session_dao.h"

#include <mysql/mysql.h>

#include <cstring>

#include "logging.h"

namespace sparkpush {

namespace {

SessionType ParseSessionType(int type) {
    switch (type) {
        case 0:
            return SessionType::kSingle;
        default:
            return SessionType::kSingle;
    }
}

}  // namespace

// 绑定 stmt 结果到 Session 结构
bool SessionDao::FillSessionFromStmt(MYSQL_STMT* stmt, Session* session,
                                     std::string* err_msg) {
    if (!session) return false;

    MYSQL_BIND result[6];
    memset(result, 0, sizeof(result));

    char session_id_buf[256] = {0};
    unsigned long session_id_len = 0;
    int type_buf = 0;
    long long user1_buf = 0;
    long long user2_buf = 0;
    long long group_id_buf = 0;
    long long last_seq_buf = 0;

    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = session_id_buf;
    result[0].buffer_length = sizeof(session_id_buf);
    result[0].length = &session_id_len;

    result[1].buffer_type = MYSQL_TYPE_LONG;
    result[1].buffer = &type_buf;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &user1_buf;

    result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    result[3].buffer = &user2_buf;

    result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    result[4].buffer = &group_id_buf;

    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &last_seq_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        return false;
    }

    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        return false;
    }

    session->id.assign(session_id_buf, session_id_len);
    session->type = ParseSessionType(type_buf);
    session->user1_id = user1_buf;
    session->user2_id = user2_buf;
    session->last_msg_seq = last_seq_buf;
    return true;
}

// 确保单聊会话存在（若不存在则插入）
bool SessionDao::EnsureSingleSession(MYSQL* conn, const std::string& session_id,
                                     int64_t user1, int64_t user2,
                                     std::string* err_msg) {
    const char* sql =
        "INSERT INTO `session`(session_id, type, user1_id, user2_id, group_id, "
        "last_msg_seq) "
        "VALUES(?, 0, ?, ?, 0, 0)";
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
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;

    long long user1_buf = user1;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &user1_buf;

    long long user2_buf = user2;
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &user2_buf;

    bool ok = true;
    // 绑定参数
    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        unsigned int err = mysql_stmt_errno(stmt);
        if (err != 1062) {  // duplicate key
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            ok = false;
        }
    }
    mysql_stmt_close(stmt);
    return ok;
}

// 按 session_id 查询会话
bool SessionDao::GetSessionById(const std::string& session_id, Session* session,
                                std::string* err_msg) {
    if (!pool_ || !session) {
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
        "SELECT session_id, type, user1_id, user2_id, group_id, last_msg_seq "
        "FROM `session` WHERE session_id=? LIMIT 1";
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
    MYSQL_BIND bind[1];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;
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
    if (!FillSessionFromStmt(stmt, session, err_msg)) {
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

// 获取或创建单聊会话
bool SessionDao::GetOrCreateSingleSession(int64_t user1, int64_t user2,
                                          Session* session,
                                          std::string* err_msg) {
    if (!session || !pool_) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    if (user1 > user2) std::swap(user1, user2);
    std::string session_id =
        "s_" + std::to_string(user1) + "_" + std::to_string(user2);

    {
        auto guard = pool_->Acquire();
        MYSQL* conn = guard.get();
        if (!conn) {
            if (err_msg) *err_msg = "no mysql connection";
            return false;
        }
        std::string err;
        if (!EnsureSingleSession(conn, session_id, user1, user2, &err)) {
            LOG_ERROR << "EnsureSingleSession failed: " << err;
        }
    }

    return GetSessionById(session_id, session, err_msg);
}

// 更新会话的 last_msg_seq（用于未读计数）
// msg_id_seq 是从 msg_id 字符串解析出的序号部分
bool SessionDao::UpdateLastMsgSeq(const std::string& session_id,
                                  int64_t msg_id_seq, std::string* err_msg) {
    if (!pool_ || msg_id_seq <= 0) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    // 只有当新的 msg_id_seq 大于当前值时才更新
    const char* sql =
        "UPDATE `session` SET last_msg_seq = ? WHERE session_id = ? AND "
        "last_msg_seq < ?";
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
    
    long long seq_buf = msg_id_seq;
    bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[0].buffer = &seq_buf;

    unsigned long sid_len = static_cast<unsigned long>(session_id.size());
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(session_id.data());
    bind[1].buffer_length = sid_len;
    bind[1].length = &sid_len;

    long long seq_buf2 = msg_id_seq;
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &seq_buf2;

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

// 列出用户参与的单聊会话
bool SessionDao::ListUserSingleSessions(int64_t user_id,
                                        std::vector<Session>* sessions,
                                        std::string* err_msg) {
    if (!sessions || !pool_) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    sessions->clear();
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "SELECT session_id, type, user1_id, user2_id, group_id, last_msg_seq "
        "FROM `session` WHERE type=0 AND (user1_id=? OR user2_id=?)";
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
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &uid_buf;
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
    if (mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    MYSQL_BIND result[6];
    memset(result, 0, sizeof(result));
    char session_id_buf[256];
    unsigned long session_id_len = 0;
    int type_buf = 0;
    long long user1_buf = 0;
    long long user2_buf = 0;
    long long group_id_buf = 0;
    long long last_seq_buf = 0;

    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = session_id_buf;
    result[0].buffer_length = sizeof(session_id_buf);
    result[0].length = &session_id_len;

    result[1].buffer_type = MYSQL_TYPE_LONG;
    result[1].buffer = &type_buf;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &user1_buf;

    result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    result[3].buffer = &user2_buf;

    result[4].buffer_type = MYSQL_TYPE_LONGLONG;
    result[4].buffer = &group_id_buf;

    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &last_seq_buf;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) {
            break;
        }
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        Session s;
        s.id.assign(session_id_buf, session_id_len);
        s.type = ParseSessionType(type_buf);
        s.user1_id = user1_buf;
        s.user2_id = user2_buf;
        sessions->push_back(std::move(s));
    }
    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush
