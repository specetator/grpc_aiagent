#include "message_reservation.h"
#include "sql_helpers.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <limits>
#include <vector>

namespace sparkpush {
namespace {
struct Transaction {
    MYSQL* connection;
    bool committed{false};
    ~Transaction() { if (!committed) mysql_query(connection, "ROLLBACK"); }
};
std::string Encode(const Message& m) {
    return nlohmann::json{{"msg_id", m.msg_id}, {"session_id", m.session_id},
        {"msg_seq", m.msg_seq}, {"sender_id", m.sender_id}, {"timestamp_ms", m.timestamp_ms},
        {"msg_type", m.msg_type}, {"content_json", m.content_json},
        {"client_msg_id", m.client_msg_id}}.dump();
}
Message Decode(const std::string& payload) {
    const auto j = nlohmann::json::parse(payload);
    Message m;
    m.msg_id = j.at("msg_id").get<std::string>();
    m.session_id = j.at("session_id").get<std::string>();
    m.msg_seq = j.at("msg_seq").get<int64_t>();
    m.sender_id = j.at("sender_id").get<int64_t>();
    m.timestamp_ms = j.at("timestamp_ms").get<int64_t>();
    m.msg_type = j.at("msg_type").get<std::string>();
    m.content_json = j.at("content_json").get<std::string>();
    m.client_msg_id = j.at("client_msg_id").get<std::string>();
    return m;
}
}  // namespace

bool MessageReservation::EnsureSchema(std::string* error) {
    if (!pool_) return false;
    auto guard = pool_->Acquire(2000);
    auto* conn = guard.get();
    return SqlExec(conn,
        "CREATE TABLE IF NOT EXISTS session_sequence_reservation ("
        "session_id VARBINARY(128) NOT NULL,highest_seq BIGINT NOT NULL,"
        "PRIMARY KEY(session_id)) ENGINE=InnoDB", error) && SqlExec(conn,
        "CREATE TABLE IF NOT EXISTS message_reservation ("
        "session_id VARBINARY(128) NOT NULL,sender_id BIGINT NOT NULL,"
        "client_msg_id VARBINARY(128) NOT NULL,msg_seq BIGINT NOT NULL,payload MEDIUMBLOB NOT NULL,"
        "created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
        "PRIMARY KEY(session_id,sender_id,client_msg_id),"
        "UNIQUE KEY uk_reserved_seq(session_id,msg_seq)) ENGINE=InnoDB", error);
}

bool MessageReservation::Reserve(const Message& candidate, int64_t floor, Message* message,
                                 bool* is_new, std::string* error) {
    if (!pool_ || !message || !is_new || candidate.session_id.empty() ||
        candidate.session_id.size() > 128 || candidate.sender_id <= 0 ||
        candidate.client_msg_id.size() > 128 || candidate.msg_type.empty() ||
        candidate.msg_type.size() > 32 || candidate.timestamp_ms < 0 || floor < 0) {
        if (error) *error = "invalid message reservation";
        return false;
    }
    auto guard = pool_->Acquire(2000);
    auto* conn = guard.get();
    if (!conn || !SqlExec(conn, "START TRANSACTION", error)) return false;
    Transaction transaction{conn};
    try {
        const auto session = "_binary" + SqlQuote(conn, candidate.session_id);
        if (!SqlExec(conn, "INSERT INTO session_sequence_reservation(session_id,highest_seq) VALUES(" +
            session + "," + std::to_string(floor) + ") ON DUPLICATE KEY UPDATE "
            "highest_seq=GREATEST(highest_seq,VALUES(highest_seq))", error)) return false;
        std::vector<std::vector<std::string>> rows;
        if (!SqlRows(conn, "SELECT highest_seq FROM session_sequence_reservation WHERE session_id=" +
            session + " FOR UPDATE", &rows, error) || rows.size() != 1) return false;
        const auto highest = std::stoll(rows.front().front());
        if (!candidate.client_msg_id.empty()) {
            if (!SqlRows(conn, "SELECT payload FROM message_reservation WHERE session_id=" + session +
                " AND sender_id=" + std::to_string(candidate.sender_id) + " AND client_msg_id=_binary" +
                SqlQuote(conn, candidate.client_msg_id), &rows, error)) return false;
            if (!rows.empty()) {
                const auto original = Decode(rows.front().front());
                if (original.session_id != candidate.session_id || original.sender_id != candidate.sender_id ||
                    original.client_msg_id != candidate.client_msg_id || original.msg_seq <= 0) {
                    if (error) *error = "corrupt original reservation identity";
                    return false;
                }
                if (!SqlExec(conn, "COMMIT", error)) return false;
                transaction.committed = true;
                *message = original;
                *is_new = false;
                return true;
            }
        }
        if (highest == std::numeric_limits<int64_t>::max()) {
            if (error) *error = "message sequence exhausted";
            return false;
        }
        Message reserved = candidate;
        reserved.msg_seq = highest + 1;
        reserved.msg_id = reserved.session_id + "-" + std::to_string(reserved.msg_seq);
        if (!SqlExec(conn, "UPDATE session_sequence_reservation SET highest_seq=" + std::to_string(reserved.msg_seq) +
            " WHERE session_id=" + session, error)) return false;
        if (!candidate.client_msg_id.empty()) {
            const auto payload = Encode(reserved);
            if (!SqlExec(conn, "INSERT INTO message_reservation(session_id,sender_id,client_msg_id,msg_seq,payload) VALUES(" +
                session + "," + std::to_string(reserved.sender_id) + ",_binary" + SqlQuote(conn, reserved.client_msg_id) +
                "," + std::to_string(reserved.msg_seq) + ",_binary" + SqlQuote(conn, payload) + ")", error)) return false;
        }
        if (!SqlExec(conn, "COMMIT", error)) return false;
        transaction.committed = true;
        *message = std::move(reserved);
        *is_new = true;
        return true;
    } catch (...) {
        // Parser exceptions can contain message bodies. Keep diagnostics content-free.
        if (error) *error = "message reservation encoding or result validation failed";
        return false;
    }
}
}  // namespace sparkpush
