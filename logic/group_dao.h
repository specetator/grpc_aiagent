#pragma once
#include "model.h"
#include "mysql_pool.h"
namespace sparkpush {
// Compatibility repository for legacy public chatrooms. All values are bound.
class GroupDao {
 public:
    explicit GroupDao(MySqlConnectionPool* pool) : pool_(pool) {}
    bool CreateChatroom(const std::string& name, int64_t owner, int64_t* room, std::string* error);
    bool GetGroup(int64_t room, ImGroup* group, std::string* error);
    bool ListChatrooms(int offset, int limit, std::vector<ImGroup>* groups, std::string* error);
 private:
    MySqlConnectionPool* pool_;
};
// Authoritative durable membership. Presence and route caches never grant access.
class GroupMemberDao {
 public:
    explicit GroupMemberDao(MySqlConnectionPool* pool) : pool_(pool) {}
    bool AddOrUpdateMember(int64_t room, int64_t user, int role, std::string* error);
    bool RemoveMember(int64_t room, int64_t user, std::string* error);
    bool ListUserChatrooms(int64_t user, std::vector<int64_t>* rooms, std::string* error);
    bool IsMember(int64_t room, int64_t user, bool* member, std::string* error);
    bool GetRole(int64_t room, int64_t user, int* role, std::string* error);
    bool ListRoomMembers(int64_t room, std::vector<int64_t>* users, std::string* error);
 private:
    MySqlConnectionPool* pool_;
};
}
