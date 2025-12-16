#include "message_dao.h"

#include <mysql/mysql.h>

#include <cstring>

namespace sparkpush {

// 插入消息
bool MessageDao::Insert(const PersistMessage& m, std::string* err_msg) {
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
        "INSERT INTO message(msg_id, session_id, msg_seq, sender_id, "
        "msg_type, content_json, timestamp_ms, client_msg_id) "
        "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
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

    MYSQL_BIND bind[8];
    memset(bind, 0, sizeof(bind));

    unsigned long msg_id_len = static_cast<unsigned long>(m.msg_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(m.msg_id.data());
    bind[0].buffer_length = msg_id_len;
    bind[0].length = &msg_id_len;

    unsigned long sid_len = static_cast<unsigned long>(m.session_id.size());
    bind[1].buffer_type = MYSQL_TYPE_STRING;
    bind[1].buffer = const_cast<char*>(m.session_id.data());
    bind[1].buffer_length = sid_len;
    bind[1].length = &sid_len;

    long long seq_buf = m.msg_seq;
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &seq_buf;

    long long sender_buf = m.sender_id;
    bind[3].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[3].buffer = &sender_buf;

    unsigned long type_len = static_cast<unsigned long>(m.msg_type.size());
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = const_cast<char*>(m.msg_type.data());
    bind[4].buffer_length = type_len;
    bind[4].length = &type_len;

    unsigned long content_len =
        static_cast<unsigned long>(m.content_json.size());
    bind[5].buffer_type = MYSQL_TYPE_STRING;
    bind[5].buffer = const_cast<char*>(m.content_json.data());
    bind[5].buffer_length = content_len;
    bind[5].length = &content_len;

    long long ts_buf = m.timestamp_ms;
    bind[6].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[6].buffer = &ts_buf;

    unsigned long client_len =
        static_cast<unsigned long>(m.client_msg_id.size());
    bind[7].buffer_type = MYSQL_TYPE_STRING;
    bind[7].buffer = const_cast<char*>(m.client_msg_id.data());
    bind[7].buffer_length = client_len;
    bind[7].length = &client_len;

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

// 查询历史消息
bool MessageDao::List(const std::string& session_id, int64_t anchor_seq,
                      int limit, std::vector<PersistMessage>* messages,
                      std::string* err_msg) {
    if (!pool_ || !messages) {
        if (err_msg) *err_msg = "invalid arguments";
        return false;
    }
    messages->clear();

    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }

    const char* sql_with_anchor =
        "SELECT msg_id, session_id, msg_seq, sender_id, msg_type, "
        "content_json, timestamp_ms, client_msg_id "
        "FROM message WHERE session_id=? AND msg_seq<? "
        "ORDER BY msg_seq DESC LIMIT ?";
    const char* sql_without_anchor =
        "SELECT msg_id, session_id, msg_seq, sender_id, msg_type, "
        "content_json, timestamp_ms, client_msg_id "
        "FROM message WHERE session_id=? "
        "ORDER BY msg_seq DESC LIMIT ?";

    const char* sql = (anchor_seq > 0) ? sql_with_anchor : sql_without_anchor;

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

    int index = 1;
    int bind_count = 2;
    long long anchor_buf = 0;
    if (anchor_seq > 0) {
        anchor_buf = anchor_seq;
        bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
        bind[1].buffer = &anchor_buf;
        index = 2;
        bind_count = 3;
    }
    int limit_buf = (limit <= 0 || limit > 200) ? 50 : limit;
    bind[index].buffer_type = MYSQL_TYPE_LONG;
    bind[index].buffer = &limit_buf;

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

    MYSQL_BIND result[8];
    memset(result, 0, sizeof(result));
    char msg_id_buf[256];
    unsigned long msg_id_len = 0;
    char session_buf[256];
    unsigned long session_len = 0;
    long long seq_buf = 0;
    long long sender_buf = 0;
    char type_buf[64];
    unsigned long type_len = 0;
    char content_buf[65536];
    unsigned long content_len = 0;
    long long ts_buf = 0;
    char client_buf[256];
    unsigned long client_len = 0;

    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = msg_id_buf;
    result[0].buffer_length = sizeof(msg_id_buf);
    result[0].length = &msg_id_len;

    result[1].buffer_type = MYSQL_TYPE_STRING;
    result[1].buffer = session_buf;
    result[1].buffer_length = sizeof(session_buf);
    result[1].length = &session_len;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &seq_buf;

    result[3].buffer_type = MYSQL_TYPE_LONGLONG;
    result[3].buffer = &sender_buf;

    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = type_buf;
    result[4].buffer_length = sizeof(type_buf);
    result[4].length = &type_len;

    result[5].buffer_type = MYSQL_TYPE_STRING;
    result[5].buffer = content_buf;
    result[5].buffer_length = sizeof(content_buf);
    result[5].length = &content_len;

    result[6].buffer_type = MYSQL_TYPE_LONGLONG;
    result[6].buffer = &ts_buf;

    result[7].buffer_type = MYSQL_TYPE_STRING;
    result[7].buffer = client_buf;
    result[7].buffer_length = sizeof(client_buf);
    result[7].length = &client_len;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    int fetch_ret = 0;
    while ((fetch_ret = mysql_stmt_fetch(stmt)) == 0 ||
           fetch_ret == MYSQL_DATA_TRUNCATED) {
        PersistMessage pm;
        pm.msg_id.assign(msg_id_buf, msg_id_len);
        pm.session_id.assign(session_buf, session_len);
        pm.msg_seq = seq_buf;
        pm.sender_id = sender_buf;
        pm.msg_type.assign(type_buf, type_len);
        pm.content_json.assign(content_buf, content_len);
        pm.timestamp_ms = ts_buf;
        pm.client_msg_id.assign(client_buf, client_len);
        messages->push_back(std::move(pm));
    }

    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush

