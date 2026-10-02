#include "delivery_outbox.h"
#include "sql_helpers.h"

#include <mysql/mysql.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <unistd.h>

namespace sparkpush {
namespace {
std::string Quote(MYSQL* conn, const std::string& value) {
    std::string escaped(value.size() * 2 + 1, '\0');
    const auto size = mysql_real_escape_string(conn, escaped.data(), value.data(), value.size());
    escaped.resize(size);
    return "'" + escaped + "'";
}
bool Query(MYSQL* conn, const std::string& sql, std::string* error) {
    if (!conn || mysql_real_query(conn, sql.data(), sql.size()) != 0) {
        if (error) *error = conn ? mysql_error(conn) : "no MySQL connection";
        return false;
    }
    return true;
}
std::string LeaseTime(int milliseconds) {
    return "DATE_ADD(NOW(3), INTERVAL " + std::to_string(std::max(1, milliseconds) * 1000LL) + " MICROSECOND)";
}
std::string Fence(MYSQL* conn, const DeliveryOutbox::Task& task) {
    return " WHERE msg_id=" + Quote(conn, task.msg_id) + " AND lease_token=" +
           Quote(conn, task.lease_token) + " AND completed_at IS NULL AND lease_until>NOW(3)";
}
bool Mutate(MySqlConnectionPool* pool, const DeliveryOutbox::Task& task,
            const std::string& update, std::string* error) {
    if (!pool) return false;
    auto guard = pool->Acquire(2000);
    auto* conn = guard.get();
    if (!conn || !Query(conn, update + Fence(conn, task), error)) return false;
    if (mysql_affected_rows(conn) != 1) {
        if (error) *error = "delivery task lease no longer owned";
        return false;
    }
    return true;
}
}  // namespace

bool DeliveryOutbox::EnsureSchema(std::string* error) {
    if (!pool_) return false;
    auto guard = pool_->Acquire(2000);
    return Query(guard.get(),
        "CREATE TABLE IF NOT EXISTS delivery_outbox ("
        "msg_id VARCHAR(160) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,"
        "session_id VARCHAR(128) NOT NULL,msg_seq BIGINT NOT NULL,payload MEDIUMBLOB NOT NULL,"
        "attempts INT NOT NULL DEFAULT 0,next_attempt_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
        "lease_token VARCHAR(96) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',"
        "lease_until DATETIME(3) NULL,completed_at DATETIME(3) NULL,"
        "created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),"
        "PRIMARY KEY(msg_id),KEY idx_delivery_ready(completed_at,next_attempt_at),"
        "KEY idx_delivery_session(session_id,completed_at,msg_seq)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", error);
}

bool DeliveryOutbox::InsertTask(const PersistMessageRequest& request, std::string* error) {
    const auto& msg = request.message();
    if (!pool_ || msg.msg_id().empty() || msg.msg_id().size() > 160 ||
        msg.session_id().empty() || msg.session_id().size() > 128 || msg.msg_seq() <= 0 ||
        (request.scene() != "single" && request.scene() != "chatroom")) {
        if (error) *error = "invalid delivery persistence event";
        return false;
    }
    std::string payload;
    if (!request.SerializeToString(&payload)) return false;
    auto guard = pool_->Acquire(2000);
    auto* conn = guard.get();
    if (!conn) return false;
    // Preserve the first event, including its original ACK target, on replays.
    return Query(conn, "INSERT INTO delivery_outbox(msg_id,session_id,msg_seq,payload) VALUES(" +
        Quote(conn, msg.msg_id()) + "," + Quote(conn, msg.session_id()) + "," +
        std::to_string(msg.msg_seq()) + ",_binary" + Quote(conn, payload) +
        ") ON DUPLICATE KEY UPDATE msg_id=VALUES(msg_id)", error);
}

bool DeliveryOutbox::PersistAtomically(const PersistMessageRequest& request, std::string* error) {
    const auto& m = request.message();
    const bool single = request.scene() == "single";
    if (!pool_ || m.session_id().empty() || m.session_id().size() > 128 ||
        m.msg_id().empty() || m.msg_id().size() > 160 || m.msg_seq() <= 0 || m.sender_id() <= 0 ||
        m.msg_type().empty() || m.msg_type().size() > 32 || m.client_msg_id().size() > 128 ||
        m.timestamp_ms() < 0 || (!single && request.scene() != "chatroom") ||
        (single && (request.user1_id() <= 0 || request.user2_id() <= 0)) ||
        (!single && request.room_id() <= 0)) {
        if (error) *error = "invalid delivery persistence event";
        return false;
    }
    std::string payload;
    if (!request.SerializeToString(&payload)) return false;
    auto guard = pool_->Acquire(2000);
    auto* conn = guard.get();
    if (!conn || !Query(conn, "START TRANSACTION", error)) return false;
    struct Rollback {
        MYSQL* conn; bool committed{false};
        ~Rollback() { if (!committed) mysql_query(conn, "ROLLBACK"); }
    } rollback{conn};
    const auto session = Quote(conn, m.session_id());
    const auto user1 = single ? std::min(request.user1_id(), request.user2_id()) : 0;
    const auto user2 = single ? std::max(request.user1_id(), request.user2_id()) : 0;
    const auto room = single ? 0 : request.room_id();
    const std::string metadata = std::to_string(single ? 0 : 2) + "," + std::to_string(user1) +
        "," + std::to_string(user2) + "," + std::to_string(room);
    if (!Query(conn, "INSERT INTO `session`(session_id,type,user1_id,user2_id,group_id,last_msg_seq) VALUES(" +
        session + "," + metadata + ",0) ON DUPLICATE KEY UPDATE session_id=VALUES(session_id)", error)) return false;
    std::vector<std::vector<std::string>> rows;
    if (!SqlRows(conn, "SELECT type,user1_id,user2_id,group_id FROM `session` WHERE session_id=" + session,
                  &rows, error)) return false;
    if (rows.size() != 1 || std::stoll(rows[0][0]) != (single ? 0 : 2) ||
        std::stoll(rows[0][1]) != user1 || std::stoll(rows[0][2]) != user2 || std::stoll(rows[0][3]) != room) {
        if (error) *error = "invalid delivery persistence event session collision";
        return false;
    }
    if (!Query(conn, "INSERT INTO delivery_outbox(msg_id,session_id,msg_seq,payload) VALUES(" +
        Quote(conn, m.msg_id()) + "," + session + "," + std::to_string(m.msg_seq()) + ",_binary" +
        Quote(conn, payload) + ") ON DUPLICATE KEY UPDATE msg_id=VALUES(msg_id)", error)) return false;
    if (!SqlRows(conn, "SELECT payload FROM delivery_outbox WHERE msg_id=" + Quote(conn, m.msg_id()),
                 &rows, error)) return false;
    PersistMessageRequest original;
    if (rows.size() != 1 || !original.ParseFromString(rows[0][0]) ||
        original.message().SerializeAsString() != m.SerializeAsString()) {
        if (error) *error = "message sequence collision for " + m.msg_id();
        return false;
    }
    const std::string columns = "session_id,msg_seq,sender_id,msg_type,content_json,timestamp_ms,client_msg_id";
    const std::string values = session + "," + std::to_string(m.msg_seq()) + "," + std::to_string(m.sender_id()) +
        "," + Quote(conn, m.msg_type()) + "," + Quote(conn, m.content_json()) + "," +
        std::to_string(m.timestamp_ms()) + "," + Quote(conn, m.client_msg_id());
    if (!Query(conn, "INSERT INTO message(" + columns + ") VALUES(" + values + ")", error)) {
        if (mysql_errno(conn) != 1062) return false;
        // A retry succeeds only if every immutable field matches byte-for-byte.
        if (!SqlRows(conn, "SELECT sender_id,msg_type,content_json,timestamp_ms,client_msg_id FROM message WHERE "
            "session_id=" + session + " AND msg_seq=" + std::to_string(m.msg_seq()), &rows, error)) return false;
        if (rows.size() != 1 || std::stoll(rows[0][0]) != m.sender_id() || rows[0][1] != m.msg_type() ||
            rows[0][2] != m.content_json() || std::stoll(rows[0][3]) != m.timestamp_ms() || rows[0][4] != m.client_msg_id()) {
            if (error) *error = "message sequence collision for " + m.msg_id();
            return false;
        }
    }
    if (!Query(conn, "UPDATE `session` SET last_msg_seq=GREATEST(last_msg_seq," + std::to_string(m.msg_seq()) +
        ") WHERE session_id=" + session, error) || !Query(conn, "COMMIT", error)) return false;
    rollback.committed = true;
    return true;
}

bool DeliveryOutbox::ClaimReady(int lease_ms, Task* task, bool* found, std::string* error) {
    if (!pool_ || !task || !found || lease_ms <= 0) return false;
    *found = false;
    auto guard = pool_->Acquire(2000);
    auto* conn = guard.get();
    if (!conn) return false;
    // A delayed head blocks only its own session; independent sessions still run.
    if (!Query(conn,
        "SELECT o.msg_id,o.payload,o.attempts FROM delivery_outbox o "
        "JOIN message m ON m.session_id=o.session_id AND m.msg_seq=o.msg_seq "
        "WHERE o.completed_at IS NULL AND o.next_attempt_at<=NOW(3) "
        "AND (o.lease_until IS NULL OR o.lease_until<=NOW(3)) "
        "AND NOT EXISTS(SELECT 1 FROM delivery_outbox earlier "
        "JOIN message prior ON prior.session_id=earlier.session_id AND prior.msg_seq=earlier.msg_seq "
        "WHERE earlier.session_id=o.session_id AND earlier.completed_at IS NULL AND earlier.msg_seq<o.msg_seq) "
        "ORDER BY o.next_attempt_at,o.created_at LIMIT 64", error)) return false;
    std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> result(mysql_store_result(conn), mysql_free_result);
    if (!result) return false;
    static std::atomic<uint64_t> sequence{0};
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result.get()))) {
        const auto* lengths = mysql_fetch_lengths(result.get());
        Task candidate;
        candidate.msg_id.assign(row[0], lengths[0]);
        if (!candidate.request.ParseFromArray(row[1], lengths[1])) {
            if (error) *error = "invalid persisted delivery payload";
            return false;
        }
        candidate.attempts = std::stoi(row[2]) + 1;
        candidate.lease_token = std::to_string(getpid()) + "-" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" +
            std::to_string(++sequence);
        if (!Query(conn, "UPDATE delivery_outbox SET lease_token=" + Quote(conn, candidate.lease_token) +
            ",lease_until=" + LeaseTime(lease_ms) + ",attempts=attempts+1 WHERE msg_id=" +
            Quote(conn, candidate.msg_id) + " AND completed_at IS NULL AND next_attempt_at<=NOW(3) "
            "AND (lease_until IS NULL OR lease_until<=NOW(3))", error)) return false;
        if (mysql_affected_rows(conn) == 1) {
            *task = std::move(candidate);
            *found = true;
            return true;
        }
    }
    return true;
}

bool DeliveryOutbox::Renew(const Task& task, int lease_ms, std::string* error) {
    // A renewal in the same millisecond must still affect one row.
    return Mutate(pool_, task, "UPDATE delivery_outbox SET lease_until=GREATEST(" + LeaseTime(lease_ms) +
        ",DATE_ADD(lease_until,INTERVAL 1000 MICROSECOND))", error);
}
bool DeliveryOutbox::Complete(const Task& task, std::string* error) {
    return Mutate(pool_, task, "UPDATE delivery_outbox SET completed_at=NOW(3),lease_until=NULL,lease_token=''", error);
}
bool DeliveryOutbox::Retry(const Task& task, int delay_ms, std::string* error) {
    return Mutate(pool_, task, "UPDATE delivery_outbox SET next_attempt_at=" + LeaseTime(delay_ms) +
        ",lease_until=NULL,lease_token=''", error);
}
bool DeliveryOutbox::Backlog(int64_t* count, std::string* error) {
    if (!pool_ || !count) return false;
    auto guard = pool_->Acquire(2000);
    auto* conn = guard.get();
    if (!Query(conn, "SELECT COUNT(*) FROM delivery_outbox WHERE completed_at IS NULL", error)) return false;
    std::unique_ptr<MYSQL_RES, decltype(&mysql_free_result)> result(mysql_store_result(conn), mysql_free_result);
    if (!result) return false;
    const auto row = mysql_fetch_row(result.get());
    if (!row) return false;
    *count = std::stoll(row[0]);
    return true;
}
}  // namespace sparkpush
