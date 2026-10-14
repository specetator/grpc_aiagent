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
    // 丢弃算出来的水位：调用方只关心「登记成功」，不需要知道游标推到哪了。
    return AdvanceDelivered(user, session, seq, &advanced, error);
}
bool UserSessionStateDao::GetDeliveredSeq(int64_t user, const std::string& session,
                                         int64_t* seq, std::string* error) {
    // receipt=0 使下面的 `receipt > previous` 永远为假，因此不会写入任何回执，
    // 但仍会重算水位并清理旧回执——这就是它不是纯只读查询的原因。
    return AdvanceDelivered(user, session, 0, seq, error);
}
// 把送达水位向前推。这是整个离线补推机制的核心，算法如下：
//
//   1. 确保 user_session_state 行存在（并用 FOR UPDATE 锁住）
//   2. 把本次回执写进 message_delivery_receipt（INSERT IGNORE 吸收重复）
//   3. 取 SafePrefix 得到上界 safe（未落库的消息不能算送达）
//   4. 在 (previous, safe] 区间里找**第一个没有回执的 msg_seq**，水位停在它前一格
//   5. 回写水位，并删掉 <= 水位 的回执行（已被覆盖，留着只会无限膨胀）
//
// 第 4 步是关键：它把「乱序到达的回执集合」压缩成一个连续前缀。
// 举例：已收到 1、3、4，缺 2 —— 水位只能停在 1。等 2 的回执到了，
// 下一次重算会一次性推到 4。tests/message_reservation_integration_test.cpp
// 里的 "receipt gap closes after the missing delivery" 就是在验这个行为。
bool UserSessionStateDao::AdvanceDelivered(int64_t user, const std::string& session,
                                           int64_t receipt, int64_t* seq, std::string* error) {
    if (!pool_ || !seq || user <= 0 || session.empty() || receipt < 0) return false;
    auto guard = pool_->Acquire();
    MYSQL* connection = guard.get();
    if (!connection) { if (error) *error = "no MySQL delivery connection"; return false; }
    SqlTransaction transaction(connection);
    if (!transaction.active()) return false;
    const auto uid = std::to_string(user);
    // 建行的同时 ON DUPLICATE KEY UPDATE delivered_seq=user_session_state.delivered_seq
    // 是个空赋值，只为把行插入或拿到已有行，不改变任何值。
    // read_seq 给 0 是安全的：它与 delivered_seq 相互独立，下面不会被覆写。
    if (!SqlExecute(connection, "INSERT INTO user_session_state(user_id,session_id,read_seq,delivered_seq) VALUES(?,?,0,0) "
        "ON DUPLICATE KEY UPDATE delivered_seq=user_session_state.delivered_seq",
        {uid, session}, nullptr, error)) return false;
    std::vector<std::vector<std::string>> rows;
    // FOR UPDATE 把同一用户+会话的并发回执串行化。必须在这里就上锁：
    // 否则两个线程同时算出同一个「第一个空洞」，会把水位推过真正未送达的消息。
    // 注意锁的是 user_session_state 行，而不是后面的 message 表，
    // 因此不同用户之间完全并行（测试里 16 线程并发回执验的就是这个）。
    if (!SqlExecute(connection, "SELECT delivered_seq FROM user_session_state WHERE user_id=? AND session_id=? FOR UPDATE",
        {uid, session}, &rows, error) || rows.size() != 1) return false;
    const int64_t previous = std::stoll(rows[0][0]);
    // 只登记比当前水位更新的回执：更旧的已经被水位覆盖且行已被删，
    // 再写一次只会留下一行永远没人清理的垃圾。
    if (receipt > previous && !SqlExecute(connection,
        "INSERT IGNORE INTO message_delivery_receipt(user_id,session_id,msg_seq) VALUES(?,?,?)",
        {uid, session, std::to_string(receipt)}, nullptr, error)) return false;
    int64_t safe = 0;
    // 同一个连接、同一个事务里取 SafePrefix：必须看到本事务刚写的行，
    // 否则「刚落库的消息」会被误判为未持久而把水位卡住。
    if (!MySqlMessageReservations::SafePrefix(connection, session, &safe, error)) return false;
    // LEFT JOIN + r.msg_seq IS NULL 就是「在 message 里但没回执」的反连接写法。
    // MIN(...)-1 取出第一个空洞的前一格；COALESCE 的默认值是 safe，
    // 意思是「一个空洞都没找到，那么整个安全区间都已送达」。
    // 区间限定在 (previous, safe]：下界避开已覆盖部分（它们的回执已删），
    // 上界避开尚未落库的消息（否则会把还在 outbox 里的消息当已送达，重连后不再补推）。
    if (!SqlExecute(connection,
        "SELECT COALESCE(MIN(m.msg_seq)-1,?) FROM message m LEFT JOIN message_delivery_receipt r "
        "ON r.user_id=? AND r.session_id=m.session_id AND r.msg_seq=m.msg_seq "
        "WHERE m.session_id=? AND m.msg_seq>? AND m.msg_seq<=? AND r.msg_seq IS NULL",
        {std::to_string(safe), uid, session, std::to_string(previous), std::to_string(safe)}, &rows, error) || rows.size() != 1)
        return false;
    // max 兼容一种退化情形：若 safe 因封存/清理而回退，不能让已公布的水位往回跑。
    // 客户端已经按旧水位丢掉本地缓存了，往回拉会造成重复推送。
    *seq = std::max<int64_t>(previous, std::stoll(rows[0][0]));
    if (!SqlExecute(connection, "UPDATE user_session_state SET delivered_seq=? WHERE user_id=? AND session_id=?",
        {std::to_string(*seq), uid, session}, nullptr, error) ||
        // 删除已被水位覆盖的回执。这些行从此不再参与计算（查询下界就是水位），
        // 不删的话活跃会话的回执表会无上限增长。放在同一事务里，
        // 保证「水位推进」与「回执清理」要么全成功要么全回滚。
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
    // 用裸 mysql_query 而不是 prepared statement：DDL 在 MySQL 里不能预编译参数化，
    // 而且这是一次性的启动动作，不在热路径上。
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
