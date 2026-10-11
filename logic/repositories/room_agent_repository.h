#pragma once
#include "mysql_pool.h"
#include "group_dao.h"
#include "services/room_agent_service.h"
namespace sparkpush {
class MySqlRoomAgentRepository : public RoomAgentRepository {
 public:
    MySqlRoomAgentRepository(MySqlConnectionPool* pool, GroupDao* groups,
                            GroupMemberDao* members) : pool_(pool), groups_(groups), members_(members) {}
    bool EnsureSchema(std::string* error);
    bool Access(int64_t, int64_t, int64_t*, bool*, std::string*) override;
    bool Load(int64_t, nlohmann::json*, std::string*) override;
    bool Save(int64_t, int64_t, const nlohmann::json&, std::string*) override;
    bool Request(const std::string&, nlohmann::json*, std::string*) override;
    bool PutRequest(const nlohmann::json&, std::string*) override;
    bool Finish(const std::string&, const nlohmann::json&, std::string*) override;
    bool Requests(int64_t, nlohmann::json*, std::string*) override;
 private:
    MySqlConnectionPool* pool_;
    GroupDao* groups_;
    GroupMemberDao* members_;
};
}  // namespace sparkpush
