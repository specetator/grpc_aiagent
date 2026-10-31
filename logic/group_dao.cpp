#include "group_dao.h"
#include <algorithm>
#include "repositories/room_sql.h"
#include "repositories/space_repository.h"

namespace sparkpush {
namespace {
using room_sql::Query;
std::string Id(int64_t id) { return std::to_string(id); }
template<class F> bool Run(MySqlConnectionPool* pool, std::string* error, F fn) {
    try {
        if (!pool) throw std::runtime_error("mysql pool not initialized");
        auto guard=pool->Acquire();
        if (!guard.get()) throw std::runtime_error("no mysql connection");
        fn(guard.get()); return true;
    } catch (const std::exception& e) { if(error) *error=e.what(); return false; }
}
ImGroup Decode(const std::vector<std::string>& row) {
    ImGroup g; g.id=std::stoll(row[0]); g.name=row[1]; g.owner_id=std::stoll(row[2]);
    g.group_type=std::stoi(row[3]); g.description=row[4]; g.visibility=row[5]; return g;
}
}
bool GroupDao::CreateChatroom(const std::string& name, int64_t owner, int64_t* room, std::string* error) {
    MySqlSpaceRepository repo(pool_);
    auto status=repo.Create(owner,name,"","public",room);
    if (!status.ok() && error) *error=status.message;
    return status.ok();
}
bool GroupDao::GetGroup(int64_t room, ImGroup* group, std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) {
        auto rows=Query(conn,"SELECT id,name,owner_id,group_type,description,visibility FROM im_group WHERE id=?",{Id(room)});
        if (rows.empty()) throw std::runtime_error("group not found");
        if (group) *group=Decode(rows[0]);
    });
}
bool GroupDao::ListChatrooms(int offset, int limit, std::vector<ImGroup>* groups, std::string* error) {
    if (!groups) return false; groups->clear();
    return Run(pool_,error,[&](MYSQL* conn) {
        for (const auto& row : Query(conn,"SELECT id,name,owner_id,group_type,description,visibility FROM im_group WHERE group_type=1 ORDER BY id DESC LIMIT ? OFFSET ?",{Id(std::clamp(limit,1,200)),Id(std::max(offset,0))})) groups->push_back(Decode(row));
    });
}
bool GroupMemberDao::AddOrUpdateMember(int64_t room,int64_t user,int role,std::string* error) {
    if (room<=0 || user<=0 || role<0 || role>2) { if(error)*error="invalid membership"; return false; }
    return Run(pool_,error,[&](MYSQL* conn) {
        Query(conn,"INSERT INTO group_member(group_id,user_id,role) VALUES(?,?,?) ON DUPLICATE KEY UPDATE role=VALUES(role)",{Id(room),Id(user),Id(role)});
    });
}
bool GroupMemberDao::RemoveMember(int64_t room,int64_t user,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) { Query(conn,"DELETE FROM group_member WHERE group_id=? AND user_id=?",{Id(room),Id(user)}); });
}
bool GroupMemberDao::ListUserChatrooms(int64_t user,std::vector<int64_t>* rooms,std::string* error) {
    if (!rooms) return false; rooms->clear();
    return Run(pool_,error,[&](MYSQL* conn) {
        for(const auto& row:Query(conn,"SELECT gm.group_id FROM group_member gm JOIN im_group g ON gm.group_id=g.id WHERE gm.user_id=? AND g.group_type=1 AND gm.role IN (0,1,2)",{Id(user)})) rooms->push_back(std::stoll(row[0]));
    });
}
bool GroupMemberDao::GetRole(int64_t room,int64_t user,int* role,std::string* error) {
    if (!role) return false; *role=-1;
    return Run(pool_,error,[&](MYSQL* conn) {
        auto rows=Query(conn,"SELECT gm.role FROM group_member gm JOIN im_group g ON g.id=gm.group_id WHERE gm.group_id=? AND gm.user_id=? AND gm.role IN (0,1,2)",{Id(room),Id(user)});
        if(!rows.empty()) *role=std::stoi(rows[0][0]);
    });
}
bool GroupMemberDao::IsMember(int64_t room,int64_t user,bool* member,std::string* error) {
    if (!member) return false; *member=false;
    int role=-1; const bool ok=GetRole(room,user,&role,error); *member=ok && role>=0; return ok;
}
bool GroupMemberDao::ListRoomMembers(int64_t room,std::vector<int64_t>* users,std::string* error) {
    if(!users) return false; users->clear();
    return Run(pool_,error,[&](MYSQL* conn) {
        for(const auto& row:Query(conn,"SELECT gm.user_id FROM group_member gm JOIN im_group g ON g.id=gm.group_id WHERE gm.group_id=? AND gm.role IN (0,1,2)",{Id(room)})) users->push_back(std::stoll(row[0]));
    });
}
}
