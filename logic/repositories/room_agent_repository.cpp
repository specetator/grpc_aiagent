#include "repositories/room_agent_repository.h"
namespace sparkpush {
namespace {
using Json = nlohmann::json;
std::string Quote(MYSQL* conn, const std::string& value) {
    std::string escaped(value.size() * 2 + 1, '\0');
    escaped.resize(mysql_real_escape_string(conn, &escaped[0], value.data(), value.size()));
    return "'" + escaped + "'";
}
bool Exec(MYSQL* conn, const std::string& sql, std::string* error) {
    if (!conn || mysql_query(conn, sql.c_str())) {
        // Do not put SQL, prompts, or credentials into public errors.
        if (error) *error = "room Agent database operation failed";
        return false;
    }
    return true;
}
bool One(MYSQL* conn, const std::string& sql, Json* out, std::string* error) {
    *out = nullptr;
    if (!Exec(conn, sql, error)) return false;
    MYSQL_RES* result = mysql_store_result(conn);
    if (!result) return false;
    MYSQL_ROW row = mysql_fetch_row(result);
    if (row && row[0]) *out = Json::parse(row[0], nullptr, false);
    mysql_free_result(result);
    if (out->is_discarded()) { *error = "invalid persisted room Agent state"; return false; }
    return true;
}
}
bool MySqlRoomAgentRepository::EnsureSchema(std::string* error) {
    auto guard = pool_->Acquire();
    for (const auto* sql : {
        "CREATE TABLE IF NOT EXISTS room_agent_task (room_id BIGINT PRIMARY KEY, revision BIGINT NOT NULL, payload JSON NOT NULL) ENGINE=InnoDB",
        "CREATE TABLE IF NOT EXISTS room_agent_checkpoint (room_id BIGINT NOT NULL, revision BIGINT NOT NULL, payload JSON NOT NULL, PRIMARY KEY(room_id,revision)) ENGINE=InnoDB",
        "CREATE TABLE IF NOT EXISTS room_agent_request (request_id VARCHAR(121) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, room_id BIGINT NOT NULL, message_seq BIGINT NOT NULL, payload JSON NOT NULL, result JSON NULL, INDEX room_requests(room_id,message_seq)) ENGINE=InnoDB"})
        if (!Exec(guard.get(), sql, error)) return false;
    return true;
}
bool MySqlRoomAgentRepository::Access(int64_t room, int64_t user, int64_t* owner,
                                     bool* member, std::string* error) {
    ImGroup group;
    if (!groups_->GetGroup(room, &group, error) || group.group_type != 1) return false;
    *owner = group.owner_id;
    return members_->IsMember(room, user, member, error);
}
bool MySqlRoomAgentRepository::Load(int64_t room, Json* task, std::string* error) {
    auto guard = pool_->Acquire();
    return One(guard.get(), "SELECT payload FROM room_agent_task WHERE room_id=" + std::to_string(room), task, error);
}
bool MySqlRoomAgentRepository::Save(int64_t room, int64_t revision, const Json& task,
                                   std::string* error) {
    auto guard = pool_->Acquire();
    auto* conn = guard.get();
    if (!Exec(conn, "START TRANSACTION", error)) return false;
    const auto payload = Quote(conn, task.dump());
    std::string sql = revision == 0
        ? "INSERT IGNORE INTO room_agent_task VALUES(" + std::to_string(room) + ",1," + payload + ")"
        : "UPDATE room_agent_task SET revision=revision+1,payload=" + payload +
            " WHERE room_id=" + std::to_string(room) + " AND revision=" + std::to_string(revision);
    const bool saved = Exec(conn, sql, error) && mysql_affected_rows(conn) == 1 &&
        Exec(conn, "INSERT INTO room_agent_checkpoint VALUES(" + std::to_string(room) + "," +
             std::to_string(revision + 1) + "," + payload + ")", error);
    if (!saved) { Exec(conn, "ROLLBACK", error); *error = "task revision conflict or storage failure"; return false; }
    return Exec(conn, "COMMIT", error);
}
bool MySqlRoomAgentRepository::Request(const std::string& id, Json* request, std::string* error) {
    auto guard = pool_->Acquire();
    if (!guard.get()) return false;
    return One(guard.get(), "SELECT payload FROM room_agent_request WHERE request_id=" + Quote(guard.get(), id), request, error);
}
bool MySqlRoomAgentRepository::PutRequest(const Json& request, std::string* error) {
    auto guard = pool_->Acquire();
    auto* conn = guard.get();
    if (!conn) return false;
    const auto id = Quote(conn, request.at("request_id"));
    if (!Exec(conn, "INSERT IGNORE INTO room_agent_request(request_id,room_id,message_seq,payload) VALUES(" +
        id + "," + std::to_string(request.at("collaboration").at("room_id").get<int64_t>()) + "," +
        std::to_string(request.at("message_seq").get<int64_t>()) + "," + Quote(conn, request.dump()) + ")", error)) return false;
    Json stored;
    return One(conn, "SELECT payload FROM room_agent_request WHERE request_id=" + id, &stored, error) && stored == request;
}
bool MySqlRoomAgentRepository::Finish(const std::string& id, const Json& result, std::string* error) {
    auto guard = pool_->Acquire();
    auto* conn = guard.get();
    if (!conn) return false;
    const auto quoted = Quote(conn, id);
    if (!Exec(conn, "UPDATE room_agent_request SET result=" + Quote(conn, result.dump()) +
        " WHERE request_id=" + quoted + " AND result IS NULL", error)) return false;
    Json stored;
    return One(conn, "SELECT result FROM room_agent_request WHERE request_id=" + quoted, &stored, error) && stored == result;
}
bool MySqlRoomAgentRepository::Requests(int64_t room, Json* requests, std::string* error) {
    auto guard = pool_->Acquire();
    if (!Exec(guard.get(), "SELECT payload,result FROM room_agent_request WHERE room_id=" +
              std::to_string(room) + " ORDER BY message_seq DESC LIMIT 20", error)) return false;
    MYSQL_RES* result = mysql_store_result(guard.get());
    if (!result) return false;
    *requests = Json::array();
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(result))) {
        const auto request = Json::parse(row[0], nullptr, false);
        const auto completed = row[1] ? Json::parse(row[1], nullptr, false) : Json(nullptr);
        if (!request.is_object() || completed.is_discarded()) { mysql_free_result(result); return false; }
        requests->push_back({{"request_id", request.at("request_id")},
                             {"actor_user_id", request.at("user_id")},
                             {"message_seq", request.at("message_seq")},
                             {"status", completed.is_null() ? "submitted" : completed.value("ok", false) ? "completed" : "failed"},
                             {"result", completed}});
    }
    mysql_free_result(result);
    return true;
}
}  // namespace sparkpush
