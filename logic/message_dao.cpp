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
//
// 这是一个很容易踩的坑：mysql_stmt_fetch 遇到缓冲区不够时不会报错，
// 而是返回 MYSQL_DATA_TRUNCATED 并把内容**截断**到缓冲区长度。
// 若直接拿缓冲区里的字节当完整消息，就会存下一条不合法的 JSON 前缀，
// 而且全程无任何错误日志。content_json 是 MEDIUMTEXT（上限 16 MiB），
// 而初始缓冲区只有 64 KiB，因此这个分支必然会被真实流量命中。
bool FetchFullTextColumn(MYSQL_STMT* stmt, unsigned int column,
                         unsigned long reported_length,
                         std::string* output, std::string* err_msg) {
    if (!output) return false;
    output->clear();
    if (reported_length == 0) return true;
    // 先挡掉不合理的长度：reported_length 来自网络，若不校验就 assign
    // 会直接抛 bad_alloc 或 length_error，把整个进程带下去。
    if (static_cast<unsigned long long>(reported_length) >
        static_cast<unsigned long long>(std::string().max_size())) {
        if (err_msg) *err_msg = "message content exceeds local string capacity";
        return false;
    }
    // 先按报告长度占好空间，再把 data() 交给 mysql 写。
    // 注意 C++17 起 std::string::data() 才有非 const 重载，这是本工程的最低标准要求之一。
    output->assign(static_cast<std::size_t>(reported_length), '\0');
    MYSQL_BIND column_bind{};
    unsigned long fetched_length = 0;
    column_bind.buffer_type = MYSQL_TYPE_BLOB;
    column_bind.buffer = output->data();
    column_bind.buffer_length = reported_length;
    column_bind.length = &fetched_length;
    // offset=0：重取整列而不是接着上次的位置，否则会和已写入的部分重叠。
    if (mysql_stmt_fetch_column(stmt, &column_bind, column, 0) != 0) {
        if (err_msg) *err_msg = mysql_stmt_error(stmt);
        output->clear();
        return false;
    }
    // 实际取回比报告还长 -> 元数据不一致，宁可报错也不交一个可能被截断的值。
    if (fetched_length > reported_length) {
        if (err_msg) *err_msg = "mysql returned an invalid message length";
        output->clear();
        return false;
    }
    // 按实际长度收紧：报告值可能偏大（包含尾部填充）。
    output->resize(fetched_length);
    return true;
}

// 统一的「取一列文本」入口：短值走快路径，长值转动态重取。
bool AssignTextColumn(MYSQL_STMT* stmt, unsigned int column, int fetch_ret,
                      unsigned long reported_length, const char* initial_buffer,
                      std::size_t initial_capacity, std::string* output,
                      std::string* err_msg) {
    if (!initial_buffer || !output) return false;
    // The common path reads ordinary messages directly. A value at or above
    // the initial buffer boundary is fetched again dynamically, so this fast
    // path never turns a large MEDIUMTEXT value into a silent prefix.
    //
    // 两个条件是并列的，不能只留一个：
    //   reported_length > capacity            确定装不下
    //   TRUNCATED && reported_length >= capacity  刚好填满时 mysql 也会报截断，
    //                                               此时无法区分「正好等长」与「被截断」，
    //                                               一律重取才是安全的
    if (reported_length > initial_capacity ||
        (fetch_ret == MYSQL_DATA_TRUNCATED &&
         reported_length >= initial_capacity)) {
        return FetchFullTextColumn(stmt, column, reported_length, output, err_msg);
    }
    // 快路径：用 reported_length 而不是 strlen，因为内容里允许出现 '\0'。
    output->assign(initial_buffer, static_cast<std::size_t>(reported_length));
    return true;
}

// 长度审计开关打开时，在写库前/读库后各打一条日志。
// 目的是定位「字符数 vs 字节数」类的不一致：中文一个字符占 3 字节，
// 前后端若一边算字符一边算字节，限流阈值就会对不上。
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

// 插入消息记录到 message 表。
//
// 这是「至少一次投递」模型能成立的基石：Kafka 重放同一条消息时，
// 本函数必须返回 true 而不是报错，否则 Job 会把消息无限重试或丢进 DLQ。
// 幂等不是靠 INSERT IGNORE / ON DUPLICATE，而是靠普通 INSERT 撞唯一索引
// uk_session_seq(session_id,msg_seq) 得到 errno 1062，再做内容比对（见下面分支）。
// 这么选的理由：INSERT IGNORE 会默默吸收**任何**错误（包括数据截断、外键失败），
// 把真正的 bug 藏起来；而 1062 + 显式比对能区分「同一条重放」与「序号真冲突」。
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
        // 必须在 close 之前把 errno/error 取出来：mysql_stmt_close 会释放 stmt，
        // 之后再调 mysql_stmt_error 拿到的是空字符串，错误信息就丢了。
        mysql_stmt_close(stmt);
        if (mysql_errno == 1062) {
            // 1062 = Duplicate entry，意味着这个 (session_id,msg_seq) 已经存在。
            // 两种可能：同一条消息被重放（正常），或不同内容撞了同一个序号（严重）。
            bool same = false;
            std::string compare_error;
            if (IsSameStoredMessage(conn, message, &same, &compare_error) &&
                same) {
                // 内容完全一致 -> 幂等吸收，对调用方而言就是一次成功写入。
                return true;
            }
            // 内容不一致 -> 这是真冲突，必须报错让消息进 DLQ。
            // 绝不能默默返回 true：那等于把一条从未落库的消息当成了已持久化。
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

// 幂等比对的实现。返回 false 表示「比对本身失败」（无法得出结论），
// 与 *same=false（比对成功且内容不同）是两回事，调用方必须区分。
bool MessageDao::IsSameStoredMessage(MYSQL* conn, const Message& message,
                                     bool* same,
                                     std::string* err_msg) {
    if (!conn || !same) {
        if (err_msg) *err_msg = "invalid idempotency comparison arguments";
        return false;
    }
    // 先置 false：下面任何一条提前 return 的路径都不能让调用方看到一个未初始化的值。
    *same = false;
    // COUNT(*) 而非 SELECT 整行：只需要知道「存不存在完全相同的那一行」，
    // 避开把 MEDIUMTEXT 正文拉回客户端。
    // BINARY 修饰三个文本列是必需的：默认 collation（如 utf8mb4_0900_ai_ci）
    // 不区分大小写也不区分尾部空格，不加 BINARY 会把 "Hi" 与 "hi" 当成同一条，
    // 从而静默吸收掉一次真正的序号冲突。
    const char* sql =
        "SELECT COUNT(*) FROM message WHERE session_id=? AND msg_seq=? "
        "AND sender_id=? AND BINARY msg_type=? AND BINARY content_json=? AND timestamp_ms=? "
        "AND BINARY client_msg_id=?";
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

// 查询会话当前最大 msg_seq。
// COALESCE 把空会话的 NULL 变成 0，让调用方可以直接 std::stoll 而不必处理 NULL。
// 这个值主要给序号 floor 校准用（Redis 计数器丢失后重建），
// 而不是给客户端当同步游标——后者必须用 SafeSyncPrefix，
// 因为 MAX(msg_seq) 只看已落库的部分，不反映还在 outbox 里的空洞。
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

// 按会话查询历史消息，支持 anchor/limit。
//
// 方向约定：SQL 倒序取数（从 anchor 往旧方向数 limit 条），返回前反转成升序。
// 这是历史分页的标准写法：客户端要的是「anchor 之前最近的 N 条，按时间正序展示」，
// 而正序 SQL + LIMIT 只能拿到「会话最开头的 N 条」，语义完全不同。
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

    // 两条 SQL 分开写而不是拼字符串：anchor 是可选条件，拼接会引入注入面，
    // 而预编译两条固定语句既能复用执行计划又完全避开拼接。
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

    // anchor_seq <= 0 视为「没有游标」，取最新一页。
    // 不能把 0 直接当条件传下去：msg_seq < 0 永远不成立，会返回空页。
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

    // 两个 SQL 分支的占位符个数不同（2 或 3），因此用 index 动态定位 limit 的位置。
    // bind_count 只为可读性留着，mysql_stmt_bind_param 靠的是 SQL 里的 '?' 个数。
    int bind_count = 2;
    int index = 1;
    if (anchor_seq > 0) {
        long long anchor_buf = anchor_seq;
        bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
        bind[1].buffer = &anchor_buf;
        ++index;
        bind_count = 3;
    }
    // 注意：这里只处理了 <=0，**没有上限钳制**，与下面 ListMessagesAfter 不一致。
    // 调用链上 HTTP /history 的 limit 直接来自客户端 JSON，ConversationService::History
    // 也是原样透传（只有 Sync 路径走了 SyncLimit 钳到 [1,1000]）。
    // 也就是说一次请求可以拉回超大结果集，而 mysql_stmt_store_result 会把它
    // 全量缓存到客户端内存。修改本行时请同步考虑上层是否需要加钳制。
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
    // 翻转的代价是 O(N) 且 N <= limit，比在 SQL 里用子查询排序便宜。
    std::reverse(messages->begin(), messages->end());
    return true;
}

// 正序读取 after_seq 之后的消息。与 ListMessages 是互补的两个方向：
//   ListMessages      向上翻历史（用户主动下拉）
//   ListMessagesAfter 向下追增量（断线重连后的游标补偿、离线补推）
// 后者必须正序：补推需要严格按 seq 递增发送，否则客户端本地排序会错。
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
    // 与 ListMessages 不同，这里把 limit 钳到 [1,1000]。
    // 上限 1000 与 ConversationService::SyncLimit 保持一致：调用方会传 bounded_limit+1
    // 来探测 has_more，若不钳住就可能一次拉回整个会话。
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
