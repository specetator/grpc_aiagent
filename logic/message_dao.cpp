#include "message_dao.h"

#include <mysql/mysql.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <vector>

#include "text_metrics.h"

#include "logging.h"

namespace sparkpush {
namespace {

// mysql_stmt_fetch reports MYSQL_DATA_TRUNCATED when the result buffer is
// smaller than a TEXT/MEDIUMTEXT value. Fetch the current column again into a
// dynamically sized string so a long JSON message is never silently shortened.
bool FetchFullTextColumn(MYSQL_STMT* stmt, unsigned int column,
                         unsigned long reported_length,
                         std::string* output, std::string* err_msg) {
    if (!output) return false;
    output->clear();
    if (reported_length == 0) return true;
    if (static_cast<unsigned long long>(reported_length) >
        static_cast<unsigned long long>(std::string().max_size())) {
        if (err_msg) *err_msg = "message content exceeds local string capacity";
        return false;
    }
    output->assign(static_cast<std::size_t>(reported_length), '\0');
    MYSQL_BIND column_bind{};
    unsigned long fetched_length = 0;
    column_bind.buffer_type = MYSQL_TYPE_BLOB;
    column_bind.buffer = output->data();
    column_bind.buffer_length = reported_length;
    column_bind.length = &fetched_length;
    if (mysql_stmt_fetch_column(stmt, &column_bind, column, 0) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        output->clear();
        return false;
    }
    if (fetched_length > reported_length) {
        if (err_msg) *err_msg = "mysql returned an invalid message length";
        output->clear();
        return false;
    }
    output->resize(fetched_length);
    return true;
}

bool AssignTextColumn(MYSQL_STMT* stmt, unsigned int column, int fetch_ret,
                      unsigned long reported_length, const char* initial_buffer,
                      std::size_t initial_capacity, std::string* output,
                      std::string* err_msg) {
    if (!initial_buffer || !output) return false;
    // The common path reads ordinary messages directly. A value at or above
    // the initial buffer boundary is fetched again dynamically, so this fast
    // path never turns a large MEDIUMTEXT value into a silent prefix.
    if (reported_length > initial_capacity ||
        (fetch_ret == MYSQL_DATA_TRUNCATED &&
         reported_length >= initial_capacity)) {
        return FetchFullTextColumn(stmt, column, reported_length, output, err_msg);
    }
    output->assign(initial_buffer, static_cast<std::size_t>(reported_length));
    return true;
}

void LogMessageLength(const char* stage, const Message& message) {
    if (!LengthAuditEnabled()) return;
    const auto metrics = MeasureText(message.content_json);
    LOG_INFO << "length_audit stage=" << stage
             << " client_msg_id=" << message.client_msg_id
             << " msg_seq=" << message.msg_seq
             << " bytes=" << metrics.utf8_bytes
             << " chars=" << metrics.unicode_chars;
}

}  // namespace

// 插入消息记录到 message 表
bool MessageDao::InsertMessage(const Message& message, std::string* err_msg) {
    LogMessageLength("db_before_write", message);
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
        "INSERT INTO message(session_id, msg_seq, sender_id, msg_type, "
        "content_json, timestamp_ms, client_msg_id) "
        "VALUES(?, ?, ?, ?, ?, ?, ?)";
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

    MYSQL_BIND bind[7];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len =
        static_cast<unsigned long>(message.session_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(message.session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;

    long long seq_buf = message.msg_seq;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &seq_buf;

    long long sender_buf = message.sender_id;
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &sender_buf;

    unsigned long type_len =
        static_cast<unsigned long>(message.msg_type.size());
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = const_cast<char*>(message.msg_type.data());
    bind[3].buffer_length = type_len;
    bind[3].length = &type_len;

    unsigned long content_len =
        static_cast<unsigned long>(message.content_json.size());
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = const_cast<char*>(message.content_json.data());
    bind[4].buffer_length = content_len;
    bind[4].length = &content_len;

    long long ts_buf = message.timestamp_ms;
    bind[5].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[5].buffer = &ts_buf;

    unsigned long client_msg_id_len =
        static_cast<unsigned long>(message.client_msg_id.size());
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = const_cast<char*>(message.client_msg_id.data());
    bind[6].buffer_length = client_msg_id_len;
    bind[6].length = &client_msg_id_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_execute(stmt) != 0) {
        const unsigned int mysql_errno = mysql_stmt_errno(stmt);
        const std::string mysql_error = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        if (mysql_errno == 1062) {
            bool same = false;
            std::string compare_error;
            if (IsSameStoredMessage(conn, message, &same, &compare_error) &&
                same) {
                return true;
            }
            if (err_msg) {
                *err_msg = "message sequence collision for " + message.msg_id;
                if (!compare_error.empty()) *err_msg += ": " + compare_error;
            }
            return false;
        }
        if (err_msg) *err_msg = mysql_error;
        return false;
    }
    mysql_stmt_close(stmt);
    return true;
}

bool MessageDao::IsSameStoredMessage(MYSQL* conn, const Message& message,
                                     bool* same,
                                     std::string* err_msg) {
    if (!conn || !same) {
        if (err_msg) *err_msg = "invalid idempotency comparison arguments";
        return false;
    }
    *same = false;
    const char* sql =
        "SELECT COUNT(*) FROM message WHERE session_id=? AND msg_seq=? "
        "AND sender_id=? AND msg_type=? AND content_json=? AND timestamp_ms=? "
        "AND client_msg_id=?";
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

    MYSQL_BIND bind[7];
    memset(bind, 0, sizeof(bind));
    unsigned long sid_len = static_cast<unsigned long>(message.session_id.size());
    bind[0].buffer_type = MYSQL_TYPE_STRING;
    bind[0].buffer = const_cast<char*>(message.session_id.data());
    bind[0].buffer_length = sid_len;
    bind[0].length = &sid_len;
    long long seq_buf = message.msg_seq;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &seq_buf;
    long long sender_buf = message.sender_id;
    bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[2].buffer = &sender_buf;
    unsigned long type_len = static_cast<unsigned long>(message.msg_type.size());
    bind[3].buffer_type = MYSQL_TYPE_STRING;
    bind[3].buffer = const_cast<char*>(message.msg_type.data());
    bind[3].buffer_length = type_len;
    bind[3].length = &type_len;
    unsigned long content_len =
        static_cast<unsigned long>(message.content_json.size());
    bind[4].buffer_type = MYSQL_TYPE_STRING;
    bind[4].buffer = const_cast<char*>(message.content_json.data());
    bind[4].buffer_length = content_len;
    bind[4].length = &content_len;
    long long ts_buf = message.timestamp_ms;
    bind[5].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[5].buffer = &ts_buf;
    unsigned long client_len =
        static_cast<unsigned long>(message.client_msg_id.size());
    bind[6].buffer_type = MYSQL_TYPE_STRING;
    bind[6].buffer = const_cast<char*>(message.client_msg_id.data());
    bind[6].buffer_length = client_len;
    bind[6].length = &client_len;

    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    long long count = 0;
    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &count;
    if (mysql_stmt_bind_result(stmt, result) != 0 ||
        mysql_stmt_fetch(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    mysql_stmt_close(stmt);
    *same = count > 0;
    return true;
}

bool MessageDao::GetMaxMsgSeq(const std::string& session_id, int64_t* max_seq,
                              std::string* err_msg) {
    if (!pool_ || !max_seq) {
        if (err_msg) *err_msg = "invalid args";
        return false;
    }
    auto guard = pool_->Acquire();
    MYSQL* conn = guard.get();
    if (!conn) {
        if (err_msg) *err_msg = "no mysql connection";
        return false;
    }
    const char* sql =
        "SELECT COALESCE(MAX(msg_seq),0) FROM message WHERE session_id=?";
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
    long long max_buf = 0;
    MYSQL_BIND result[1];
    memset(result, 0, sizeof(result));
    result[0].buffer_type = MYSQL_TYPE_LONGLONG;
    result[0].buffer = &max_buf;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    int fr = mysql_stmt_fetch(stmt);
    mysql_stmt_close(stmt);
    if (fr != 0 && fr != MYSQL_NO_DATA) {
        if (err_msg) *err_msg = "fetch max seq failed";
        return false;
    }
    *max_seq = static_cast<int64_t>(max_buf);
    return true;
}

// 按会话查询历史消息，支持 anchor/limit
bool MessageDao::ListMessages(const std::string& session_id, int64_t anchor_seq,
                              int limit, std::vector<Message>* messages,
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
        "SELECT session_id, msg_seq, sender_id, msg_type, content_json, "
        "timestamp_ms, client_msg_id "
        "FROM message WHERE session_id=? AND msg_seq<? "
        "ORDER BY msg_seq DESC LIMIT ?";
    const char* sql_without_anchor =
        "SELECT session_id, msg_seq, sender_id, msg_type, content_json, "
        "timestamp_ms, client_msg_id "
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

    int bind_count = 2;
    int index = 1;
    if (anchor_seq > 0) {
        long long anchor_buf = anchor_seq;
        bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
        bind[1].buffer = &anchor_buf;
        ++index;
        bind_count = 3;
    }
    int limit_buf = limit <= 0 ? 50 : limit;
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

    MYSQL_BIND result[7];
    memset(result, 0, sizeof(result));
    char session_buf[256];
    unsigned long session_len = 0;
    long long seq_buf = 0;
    long long sender_buf = 0;
    char type_buf[64];
    unsigned long type_len = 0;
    constexpr std::size_t kInitialContentBufferBytes = 64 * 1024;
    std::vector<char> content_buf(kInitialContentBufferBytes, '\0');
    unsigned long content_len = 0;
    long long ts_buf = 0;
    char client_msg_id_buf[128];
    unsigned long client_msg_id_len = 0;

    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = session_buf;
    result[0].buffer_length = sizeof(session_buf);
    result[0].length = &session_len;

    result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result[1].buffer = &seq_buf;

    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &sender_buf;

    result[3].buffer_type = MYSQL_TYPE_STRING;
    result[3].buffer = type_buf;
    result[3].buffer_length = sizeof(type_buf);
    result[3].length = &type_len;

    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = content_buf.data();
    result[4].buffer_length = static_cast<unsigned long>(content_buf.size());
    result[4].length = &content_len;

    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &ts_buf;

    result[6].buffer_type = MYSQL_TYPE_STRING;
    result[6].buffer = client_msg_id_buf;
    result[6].buffer_length = sizeof(client_msg_id_buf);
    result[6].length = &client_msg_id_len;

    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) break;
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        Message msg;
        msg.session_id.assign(session_buf, session_len);
        msg.msg_seq = seq_buf;
        msg.sender_id = sender_buf;
        msg.msg_type.assign(type_buf, type_len);
        if (!AssignTextColumn(stmt, 4, fetch_ret, content_len,
                               content_buf.data(), content_buf.size(),
                               &msg.content_json, err_msg)) {
            mysql_stmt_close(stmt);
            return false;
        }
        LogMessageLength("db_after_read", msg);
        msg.timestamp_ms = ts_buf;
        msg.client_msg_id.assign(client_msg_id_buf, client_msg_id_len);
        msg.msg_id = msg.session_id + "-" + std::to_string(msg.msg_seq);
        messages->push_back(std::move(msg));
    }
    mysql_stmt_close(stmt);
    // 目前查询按倒序返回，需翻转成升序
    std::reverse(messages->begin(), messages->end());
    return true;
}

bool MessageDao::ListMessagesAfter(const std::string& session_id,
                                   int64_t after_seq, int limit,
                                   std::vector<Message>* messages,
                                   std::string* err_msg) {
    if (!pool_ || !messages || after_seq < 0) {
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
    const char* sql =
        "SELECT session_id, msg_seq, sender_id, msg_type, content_json, "
        "timestamp_ms, client_msg_id FROM message WHERE session_id=? "
        "AND msg_seq>? ORDER BY msg_seq ASC LIMIT ?";
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
    long long after_buf = after_seq;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &after_buf;
    int limit_buf = limit <= 0 ? 50 : std::min(limit, 1000);
    bind[2].buffer_type = MYSQL_TYPE_LONG;
    bind[2].buffer = &limit_buf;
    if (mysql_stmt_bind_param(stmt, bind) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    if (mysql_stmt_store_result(stmt) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }

    MYSQL_BIND result[7];
    memset(result, 0, sizeof(result));
    char session_buf[256] = {0};
    unsigned long session_len = 0;
    long long seq_buf = 0;
    long long sender_buf = 0;
    char type_buf[64] = {0};
    unsigned long type_len = 0;
    constexpr std::size_t kInitialContentBufferBytes = 64 * 1024;
    std::vector<char> content_buf(kInitialContentBufferBytes, '\0');
    unsigned long content_len = 0;
    long long ts_buf = 0;
    char client_msg_id_buf[128] = {0};
    unsigned long client_msg_id_len = 0;

    result[0].buffer_type = MYSQL_TYPE_STRING;
    result[0].buffer = session_buf;
    result[0].buffer_length = sizeof(session_buf);
    result[0].length = &session_len;
    result[1].buffer_type = MYSQL_TYPE_LONGLONG;
    result[1].buffer = &seq_buf;
    result[2].buffer_type = MYSQL_TYPE_LONGLONG;
    result[2].buffer = &sender_buf;
    result[3].buffer_type = MYSQL_TYPE_STRING;
    result[3].buffer = type_buf;
    result[3].buffer_length = sizeof(type_buf);
    result[3].length = &type_len;
    result[4].buffer_type = MYSQL_TYPE_STRING;
    result[4].buffer = content_buf.data();
    result[4].buffer_length = static_cast<unsigned long>(content_buf.size());
    result[4].length = &content_len;
    result[5].buffer_type = MYSQL_TYPE_LONGLONG;
    result[5].buffer = &ts_buf;
    result[6].buffer_type = MYSQL_TYPE_STRING;
    result[6].buffer = client_msg_id_buf;
    result[6].buffer_length = sizeof(client_msg_id_buf);
    result[6].length = &client_msg_id_len;
    if (mysql_stmt_bind_result(stmt, result) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
    }
    while (true) {
        int fetch_ret = mysql_stmt_fetch(stmt);
        if (fetch_ret == MYSQL_NO_DATA) break;
        if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
            if (err_msg) *err_msg = mysql_stmt_error(stmt);
            mysql_stmt_close(stmt);
            return false;
        }
        Message msg;
        msg.session_id.assign(session_buf, session_len);
        msg.msg_seq = seq_buf;
        msg.sender_id = sender_buf;
        msg.msg_type.assign(type_buf, type_len);
        if (!AssignTextColumn(stmt, 4, fetch_ret, content_len,
                               content_buf.data(), content_buf.size(),
                               &msg.content_json, err_msg)) {
            mysql_stmt_close(stmt);
            return false;
        }
        LogMessageLength("db_after_read", msg);
        msg.timestamp_ms = ts_buf;
        msg.client_msg_id.assign(client_msg_id_buf, client_msg_id_len);
        msg.msg_id = msg.session_id + "-" + std::to_string(msg.msg_seq);
        messages->push_back(std::move(msg));
    }
    mysql_stmt_close(stmt);
    return true;
}

}  // namespace sparkpush
