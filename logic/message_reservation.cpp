#include "message_reservation.h"
#include "message_dao.h"
#include "mysql_statement.h"
#include "security.h"
#include <limits>
#include <nlohmann/json.hpp>
namespace sparkpush {
namespace {
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
bool MySqlMessageReservations::Reserve(const Message& proposed, Message* canonical,
                                      bool* fresh, std::string* error) {
    if (!pool_ || !canonical || !fresh || proposed.session_id.empty() || proposed.session_id.size() > 128)
        return false;
    auto guard = pool_->Acquire();
    MYSQL* connection = guard.get();
    if (!connection) { if (error) *error = "MySQL reservation connection unavailable"; return false; }
    SqlTransaction transaction(connection);
    if (!transaction.active()) { if (error) *error = mysql_error(connection); return false; }
    const auto sid = proposed.session_id;
    // Initial floor includes legacy history. Deployment must drain the pre-migration Kafka backlog.
    if (!SqlExecute(connection,
        "INSERT INTO message_sequence(session_id,last_seq) VALUES(?,0) "
        "ON DUPLICATE KEY UPDATE last_seq=message_sequence.last_seq",
        {sid}, nullptr, error)) return false;
    std::vector<std::vector<std::string>> rows;
    if (!SqlExecute(connection, "SELECT last_seq FROM message_sequence WHERE session_id=? FOR UPDATE",
                    {sid}, &rows, error) || rows.size() != 1) return false;
    int64_t previous = std::stoll(rows[0][0]);
    if (previous == 0) {
        if (!SqlExecute(connection, "SELECT GREATEST(COALESCE((SELECT MAX(msg_seq) FROM message WHERE session_id=?),0),"
            "COALESCE((SELECT MAX(last_msg_seq) FROM session WHERE session_id=?),0))", {sid, sid}, &rows, error)) return false;
        previous = std::stoll(rows[0][0]);
    }
    std::string client = proposed.client_msg_id;
    // Empty IDs remain unique submissions, preserving the legacy HTTP contract.
    if (client.empty() && !GenerateSecureToken(&client, error)) return false;
    if (!SqlExecute(connection,
        "SELECT original_message,stage FROM message_reservation WHERE session_id=? AND sender_id=? AND client_msg_id=?",
        {sid, std::to_string(proposed.sender_id), client}, &rows, error)) return false;
    if (!rows.empty()) {
        if (rows[0][1] == "4") { if (error) *error = "reservation expired; use a new client_msg_id"; return false; }
        *canonical = Decode(rows[0][0]);
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
    if (!SqlExecute(guard.get(), "UPDATE message_reservation SET persist_payload=?,stage=GREATEST(stage,1) "
        "WHERE session_id=? AND msg_seq=? AND stage<>4 AND (persist_payload IS NULL OR persist_payload=?)",
        {payload, m.session_id, std::to_string(m.msg_seq), payload}, nullptr, error)) return false;
    std::vector<std::vector<std::string>> rows;
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
    if (!SqlExecute(guard.get(), "UPDATE message_reservation SET stage=4 WHERE stage=0 AND "
        "created_at < CURRENT_TIMESTAMP(3) - INTERVAL 10 MINUTE", {}, nullptr, error)) return false;
    if (!SqlExecute(guard.get(), "DELETE FROM message_reservation WHERE stage IN (3,4) AND "
        "created_at < CURRENT_TIMESTAMP(3) - INTERVAL 1 DAY LIMIT 1000", {}, nullptr, error)) return false;
    std::vector<std::vector<std::string>> rows;
    if (!SqlExecute(guard.get(), "SELECT original_message,persist_payload FROM message_reservation WHERE stage=1 "
        "ORDER BY created_at LIMIT ?", {std::to_string(limit)}, &rows, error)) return false;
    out->clear();
    for (const auto& row : rows) out->emplace_back(Decode(row[0]), row[1]);
    return true;
}
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
bool MessageDao::SafeSyncPrefix(const std::string& session, int64_t* seq, std::string* error) {
    auto guard = pool_->Acquire();
    return guard.get() && MySqlMessageReservations::SafePrefix(guard.get(), session, seq, error);
}
}  // namespace sparkpush
