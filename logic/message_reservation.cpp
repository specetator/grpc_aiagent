#include "message_reservation.h"
#include "message_dao.h"
#include "mysql_statement.h"
#include "security.h"
#include <limits>
#include <nlohmann/json.hpp>
namespace sparkpush {
namespace {
// 把 Message 序列化成 JSON 存进 original_message / 从里面读回来。
// 字段名故意用缩写（session/seq/sender/type/content/time/client）：
// 这一列每条消息都要存一份，缩短键名能直接降低存储与 binlog 压力。
std::string Encode(const Message& m) {
    return nlohmann::json{{"session", m.session_id}, {"seq", m.msg_seq}, {"sender", m.sender_id},
        {"type", m.msg_type}, {"content", m.content_json}, {"time", m.timestamp_ms},
        {"client", m.client_msg_id}}.dump();
}
Message Decode(const std::string& value) {
    const auto j = nlohmann::json::parse(value);
    Message m;
    m.session_id = j.at("session"); m.msg_seq = j.at("seq"); m.sender_id = j.at("sender");
    m.msg_type = j.at("type"); m.content_json = j.at("content"); m.timestamp_ms = j.at("time");
    m.client_msg_id = j.at("client"); m.msg_id = m.session_id + "-" + std::to_string(m.msg_seq);
    return m;
}
}
// 为一条消息占号。全程在一个事务里，靠 SELECT ... FOR UPDATE 串行化同会话的并发占号。
// 注意这是 MySQL 路径，不是热路径：正常发消息的序号由 Redis Lua 脚本分配（见
// redis_store.cpp 的 kAllocateSeqScript）。本函数用于 Redis 不可用时的兜底、
// 以及 outbox 恢复场景下重建权威序号。
bool MySqlMessageReservations::Reserve(const Message& proposed, Message* canonical,
                                      bool* fresh, std::string* error) {
    // session_id 长度上限 128 必须与 schema 里的 VARCHAR(128) 一致，
    // 否则超长 id 会被 MySQL 默默截断，导致两条不同会话撞到同一个计数器上。
    if (!pool_ || !canonical || !fresh || proposed.session_id.empty() || proposed.session_id.size() > 128)
        return false;
    auto guard = pool_->Acquire();
    MYSQL* connection = guard.get();
    if (!connection) { if (error) *error = "MySQL reservation connection unavailable"; return false; }
    SqlTransaction transaction(connection);
    if (!transaction.active()) { if (error) *error = mysql_error(connection); return false; }
    const auto sid = proposed.session_id;
    // 先保证计数器行存在。ON DUPLICATE KEY UPDATE last_seq=message_sequence.last_seq
    // 是一个「什么都不改」的赋值，目的只是把这一行拿到手（同时建立行锁），
    // 而不是把 last_seq 重置。若写成 VALUES(last_seq) 会把已有序号归零，造成序号重发。
    // Initial floor includes legacy history. Deployment must drain the pre-migration Kafka backlog.
    if (!SqlExecute(connection,
        "INSERT INTO message_sequence(session_id,last_seq) VALUES(?,0) "
        "ON DUPLICATE KEY UPDATE last_seq=message_sequence.last_seq",
        {sid}, nullptr, error)) return false;
    std::vector<std::vector<std::string>> rows;
    // FOR UPDATE 是本函数并发安全的全部：同会话的多个占号请求在这里排队。
    if (!SqlExecute(connection, "SELECT last_seq FROM message_sequence WHERE session_id=? FOR UPDATE",
                    {sid}, &rows, error) || rows.size() != 1) return false;
    int64_t previous = std::stoll(rows[0][0]);
    if (previous == 0) {
        // 计数器还是初始值，说明这张表是新建的（或 Redis 路径先跑了一阵）。
        // 此时必须把历史最大值当成 floor，否则第一条消息会拿到 seq=1，
        // 直接和已经落库的老消息撞主键。GREATEST 同时看 message 和 session 两张表，
        // 因为迁移前可能存在「session.last_msg_seq 已推进但 message 还没落库」的在途消息。
        if (!SqlExecute(connection, "SELECT GREATEST(COALESCE((SELECT MAX(msg_seq) FROM message WHERE session_id=?),0),"
            "COALESCE((SELECT MAX(last_msg_seq) FROM session WHERE session_id=?),0))", {sid, sid}, &rows, error)) return false;
        previous = std::stoll(rows[0][0]);
    }
    std::string client = proposed.client_msg_id;
    // Empty IDs remain unique submissions, preserving the legacy HTTP contract.
    // 空 id 不参与去重：每次提交都生成一个新随机串，因此永远不会命中旧预约。
    // 这里用 GenerateSecureToken 而不是计数器/时间戳，避免客户端能猜到别人的 id。
    if (client.empty() && !GenerateSecureToken(&client, error)) return false;
    if (!SqlExecute(connection,
        "SELECT original_message,stage FROM message_reservation WHERE session_id=? AND sender_id=? AND client_msg_id=?",
        {sid, std::to_string(proposed.sender_id), client}, &rows, error)) return false;
    if (!rows.empty()) {
        // 命中已封存的预约：这个号已经作废了，绝不能把它再发一次。
        // 只能让客户端换一个新的 client_msg_id 重试。
        if (rows[0][1] == "4") { if (error) *error = "reservation expired; use a new client_msg_id"; return false; }
        *canonical = Decode(rows[0][0]);
        // 内容不一致说明客户端拿同一个 id 发了不同的消息。这里必须报错而不是
        // 默默返回旧内容：后者会让客户端以为新消息发成功了，实际上根本没进系统。
        if (canonical->msg_type != proposed.msg_type || canonical->content_json != proposed.content_json) {
            if (error) *error = "client_msg_id reused with different content";
            return false;
        }
        *fresh = false;
        return transaction.Commit(error);
    }
    if (previous == std::numeric_limits<int64_t>::max()) { if (error) *error = "message sequence exhausted"; return false; }
    *canonical = proposed;
    canonical->msg_seq = previous + 1;
    // msg_id 是 "session-seq" 的拼接，不是独立生成的：这样同一个 id 在任何地方
    // 重新算出来都一样，不需要额外存储或查询。
    canonical->msg_id = sid + "-" + std::to_string(canonical->msg_seq);
    if (!SqlExecute(connection, "UPDATE message_sequence SET last_seq=? WHERE session_id=?",
        {std::to_string(canonical->msg_seq), sid}, nullptr, error) ||
        !SqlExecute(connection,
        "INSERT INTO message_reservation(session_id,msg_seq,sender_id,client_msg_id,original_message,stage) VALUES(?,?,?,?,?,0)",
        {sid, std::to_string(canonical->msg_seq), std::to_string(proposed.sender_id), client, Encode(*canonical)}, nullptr, error))
        return false;
    *fresh = true;
    return transaction.Commit(error);
}
bool MySqlMessageReservations::CheckSchema(std::string* error) {
    auto guard = pool_->Acquire();
    return guard.get() &&
        SqlExecute(guard.get(), "SELECT last_seq FROM message_sequence LIMIT 0", {}, nullptr, error) &&
        SqlExecute(guard.get(), "SELECT stage,persist_payload FROM message_reservation LIMIT 0", {}, nullptr, error) &&
        SqlExecute(guard.get(), "SELECT msg_seq FROM message_delivery_receipt LIMIT 0", {}, nullptr, error);
}
bool MySqlMessageReservations::PrepareOutbox(const Message& m, const std::string& payload, std::string* error) {
    auto guard = pool_->Acquire();
    if (!guard.get()) return false;
    // A record may be sealed only while it has never been prepared for publication.
    // WHERE 里的 stage<>4 与 payload 相等判定共同构成一个 CAS：
    // 要么这条记录还没预备过（persist_payload IS NULL），要么预备的字节和本次
    // 完全一样（重试场景）。UPDATE 影响 0 行也不报错，交给下面的回读去判失败，
    // 这样并发下两个写者不会互相覆盖。
    if (!SqlExecute(guard.get(), "UPDATE message_reservation SET persist_payload=?,stage=GREATEST(stage,1) "
        "WHERE session_id=? AND msg_seq=? AND stage<>4 AND (persist_payload IS NULL OR persist_payload=?)",
        {payload, m.session_id, std::to_string(m.msg_seq), payload}, nullptr, error)) return false;
    std::vector<std::vector<std::string>> rows;
    // 回读校验：UPDATE 成功不代表写进去了（可能被 WHERE 挡掉），
    // 必须确认存下来的 payload 就是本次要发的那一份，否则宁可报错也不能发。
    if (!SqlExecute(guard.get(), "SELECT persist_payload,stage FROM message_reservation WHERE session_id=? AND msg_seq=?",
        {m.session_id, std::to_string(m.msg_seq)}, &rows, error)) return false;
    if (rows.size() != 1 || rows[0][1] == "4" || rows[0][0] != payload) {
        if (error) *error = "outbox payload conflict or expired reservation";
        return false;
    }
    return true;
}
bool MySqlMessageReservations::Published(const Message& m, std::string* error) {
    auto guard = pool_->Acquire();
    return guard.get() && SqlExecute(guard.get(),
        "UPDATE message_reservation SET stage=GREATEST(stage,2) WHERE session_id=? AND msg_seq=? AND stage<>4",
        {m.session_id, std::to_string(m.msg_seq)}, nullptr, error);
}
bool MySqlMessageReservations::Persisted(const Message& m, std::string* error) {
    auto guard = pool_->Acquire();
    return guard.get() && SqlExecute(guard.get(),
        "UPDATE message_reservation SET stage=3 WHERE session_id=? AND msg_seq=? AND stage<>4",
        {m.session_id, std::to_string(m.msg_seq)}, nullptr, error);
}
bool MySqlMessageReservations::Pending(std::vector<std::pair<Message, std::string>>* out, int limit, std::string* error) {
    auto guard = pool_->Acquire();
    if (!guard.get()) return false;
    // Raw-only reservations cannot have reached Kafka. Seal them before allowing a cursor to skip them.
    // stage=0 意味着 payload 从未写入，这条消息绝不可能到过 Kafka，可以安全作废。
    // 10 分钟阈值是给「Reserve 刚提交、PrepareOutbox 还没来得及跑」留的宽限期，
    // 太短会把正常在途的预约误封，造成消息直接丢失。
    // 封存顺序必须在扫描之前：否则 SafePrefix 会把安全水位一直压在这些空洞下面。
    if (!SqlExecute(guard.get(), "UPDATE message_reservation SET stage=4 WHERE stage=0 AND "
        "created_at < CURRENT_TIMESTAMP(3) - INTERVAL 10 MINUTE", {}, nullptr, error)) return false;
    // 清理已终结的记录。LIMIT 1000 把单次删除量封顶，避免堆积后一次删掉
    // 几百万行造成长事务和主从延迟。删不掉也不要紧，下一轮接着删。
    if (!SqlExecute(guard.get(), "DELETE FROM message_reservation WHERE stage IN (3,4) AND "
        "created_at < CURRENT_TIMESTAMP(3) - INTERVAL 1 DAY LIMIT 1000", {}, nullptr, error)) return false;
    std::vector<std::vector<std::string>> rows;
    // 只取 stage=1：payload 已存但 Kafka 还没确认。按 created_at 升序，
    // 保证重发顺序与首次提交顺序一致（同会话内有序是上层契约）。
    if (!SqlExecute(guard.get(), "SELECT original_message,persist_payload FROM message_reservation WHERE stage=1 "
        "ORDER BY created_at LIMIT ?", {std::to_string(limit)}, &rows, error)) return false;
    out->clear();
    for (const auto& row : rows) out->emplace_back(Decode(row[0]), row[1]);
    return true;
}
// 安全同步前缀。三个子查询各堵一种「以为持久了其实没持久」的情况：
//   last_seq            已分配到哪（Redis 丢了也能从这里重建）
//   MAX(message.msg_seq) 已真正落库到哪（Job 可能还没消费完）
//   MIN(stage<3)-1       最早的未完成预约，即序号空间里的第一个空洞
// 第三项里的 9223372036854775807 是 int64 最大值，作用是「没有空洞时不参与 LEAST」，
// 否则 MIN() 会返回 NULL 让整个表达式变 NULL。
bool MySqlMessageReservations::SafePrefix(MYSQL* connection, const std::string& sid,
                                         int64_t* seq, std::string* error) {
    std::vector<std::vector<std::string>> rows;
    if (!SqlExecute(connection,
        "SELECT LEAST(COALESCE((SELECT last_seq FROM message_sequence WHERE session_id=?),"
        "(SELECT COALESCE(MAX(msg_seq),0) FROM message WHERE session_id=?)),"
        "COALESCE((SELECT MIN(msg_seq)-1 FROM message_reservation WHERE session_id=? AND stage<3),9223372036854775807))",
        {sid, sid, sid}, &rows, error) || rows.size() != 1) return false;
    *seq = std::stoll(rows[0][0]);
    return true;
}
// MessageDao 侧的包装：自己从池里拿连接再转给 static 版 SafePrefix。
// 单独存在是为了让不需要事务的调用方（如 conversation_service 的 sync 接口）
// 不必关心连接管理。
bool MessageDao::SafeSyncPrefix(const std::string& session, int64_t* seq, std::string* error) {
    auto guard = pool_->Acquire();
    return guard.get() && MySqlMessageReservations::SafePrefix(guard.get(), session, seq, error);
}
}  // namespace sparkpush
