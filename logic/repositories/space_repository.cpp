#include "repositories/space_repository.h"
#include "repositories/room_sql.h"

namespace sparkpush {
namespace {
using room_sql::Query;
std::string Id(int64_t id) { return std::to_string(id); }
ImGroup Group(const std::vector<std::string>& row) {
    ImGroup group;
    group.id = std::stoll(row[0]); group.name = row[1]; group.owner_id = std::stoll(row[2]);
    group.group_type = std::stoi(row[3]); group.description = row[4]; group.visibility = row[5];
    return group;
}
class SqlSpaceTransaction final : public SpaceTransaction {
 public:
    SqlSpaceTransaction(MYSQL* conn, ImGroup group) : conn_(conn), group_(std::move(group)) {}
    const ImGroup& Group() const override { return group_; }
    int Role(int64_t user) override {
        const auto rows = Query(conn_, "SELECT role FROM group_member WHERE group_id=? AND user_id=? FOR UPDATE", {Id(group_.id), Id(user)});
        const int role=rows.empty() ? -1 : std::stoi(rows[0][0]);
        return role>=0 && role<=2 ? role : -1;
    }
    bool ActiveUser(int64_t user) override {
        return !Query(conn_, "SELECT id FROM user WHERE id=? AND status=1 AND deleted_at IS NULL", {Id(user)}).empty();
    }
    std::string Invitation(int64_t user) override {
        const auto rows = Query(conn_, "SELECT status FROM room_invitation WHERE room_id=? AND user_id=? FOR UPDATE", {Id(group_.id), Id(user)});
        return rows.empty() ? "" : rows[0][0];
    }
    void SetInvitation(int64_t user, int64_t inviter, const std::string& status) override {
        Query(conn_, "INSERT INTO room_invitation(room_id,user_id,invited_by,status) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE invited_by=IF(VALUES(status) IN ('pending','revoked'),VALUES(invited_by),invited_by),status=VALUES(status),updated_at=CURRENT_TIMESTAMP(3)", {Id(group_.id),Id(user),Id(inviter),status});
    }
    void SetMember(int64_t user, int role) override {
        Query(conn_, "INSERT INTO group_member(group_id,user_id,role) VALUES(?,?,?) ON DUPLICATE KEY UPDATE role=VALUES(role)", {Id(group_.id),Id(user),Id(role)});
    }
    void RemoveMember(int64_t user) override {
        Query(conn_, "DELETE FROM group_member WHERE group_id=? AND user_id=?", {Id(group_.id),Id(user)});
    }
    void Update(const std::string& name, const std::string& description, const std::string& visibility) override {
        Query(conn_, "UPDATE im_group SET name=?,description=?,visibility=? WHERE id=?", {name,description,visibility,Id(group_.id)});
    }
    std::vector<SpaceMember> Members() override {
        std::vector<SpaceMember> members;
        for (const auto& row : Query(conn_, "SELECT gm.user_id,gm.role,COALESCE(u.account,''),COALESCE(u.name,'') FROM group_member gm LEFT JOIN user u ON u.id=gm.user_id WHERE gm.group_id=? ORDER BY gm.role DESC,gm.user_id", {Id(group_.id)}))
            members.push_back({std::stoll(row[0]),std::stoi(row[1]),row[2],row[3]});
        return members;
    }
 private:
    MYSQL* conn_;
    ImGroup group_;
};
}
bool MySqlSpaceRepository::CheckSchema(std::string* error) {
    try {
        auto guard = pool_->Acquire();
        Query(guard.get(), "SELECT description,visibility FROM im_group LIMIT 0");
        Query(guard.get(), "SELECT room_id,user_id,invited_by,status FROM room_invitation LIMIT 0");
        return true;
    } catch (const std::exception& e) { if(error) *error=e.what(); return false; }
}
ServiceStatus MySqlSpaceRepository::Create(int64_t owner, const std::string& name,
    const std::string& description, const std::string& visibility, int64_t* room) {
    if (!pool_ || !room) return {500,"space repository unavailable"};
    try {
        auto guard = pool_->Acquire(); auto* conn = guard.get();
        room_sql::Transaction transaction(conn);
        if (Query(conn,"SELECT id FROM user WHERE id=? AND status=1 AND deleted_at IS NULL",{Id(owner)}).empty())
            return {404,"owner must be an existing active user"};
        Query(conn,"INSERT INTO im_group(name,owner_id,group_type,description,visibility) VALUES(?,?,1,?,?)",{name,Id(owner),description,visibility});
        const auto id = static_cast<int64_t>(mysql_insert_id(conn));
        Query(conn,"INSERT INTO group_member(group_id,user_id,role) VALUES(?,?,2)",{Id(id),Id(owner)});
        Query(conn,"INSERT INTO session(session_id,type,group_id) VALUES(?,2,?)",{"r_"+Id(id),Id(id)});
        transaction.Commit(); *room=id;
        return {};
    } catch (const std::exception& e) { return {500,e.what()}; }
}
ServiceStatus MySqlSpaceRepository::WithRoom(int64_t room, const std::function<ServiceStatus(SpaceTransaction&)>& operation) {
    if (!pool_) return {500,"space repository unavailable"};
    try {
        auto guard=pool_->Acquire(); auto* conn=guard.get();
        room_sql::Transaction transaction(conn);
        auto rows=Query(conn,"SELECT id,name,owner_id,group_type,description,visibility FROM im_group WHERE id=? FOR UPDATE",{Id(room)});
        if (rows.empty() || rows[0][3]!="1") return {404,"space not found"};
        SqlSpaceTransaction space(conn,Group(rows[0]));
        auto status=operation(space);
        if (status.ok()) transaction.Commit();
        return status;
    } catch (const std::exception& e) { return {500,e.what()}; }
}
ServiceStatus MySqlSpaceRepository::List(int64_t user, int offset, int limit, std::vector<SpaceSummary>* rooms) {
    if (!pool_ || !rooms) return {500,"space repository unavailable"};
    rooms->clear();
    try {
        auto guard=pool_->Acquire();
        for (const auto& row : Query(guard.get(),"SELECT g.id,g.name,g.owner_id,g.group_type,g.description,g.visibility,m.role FROM im_group g JOIN group_member m ON m.group_id=g.id WHERE m.user_id=? AND g.group_type=1 ORDER BY g.id DESC LIMIT ? OFFSET ?",{Id(user),Id(limit),Id(offset)}))
            rooms->push_back({Group(row),std::stoi(row[6])});
        return {};
    } catch (const std::exception& e) { return {500,e.what()}; }
}
ServiceStatus MySqlSpaceRepository::Invitations(int64_t user, std::vector<SpaceInvitation>* invitations) {
    if (!pool_ || !invitations) return {500,"space repository unavailable"};
    invitations->clear();
    try {
        auto guard=pool_->Acquire();
        for (const auto& row : Query(guard.get(),"SELECT g.id,g.name,g.owner_id,g.group_type,g.description,g.visibility,i.invited_by FROM im_group g JOIN room_invitation i ON i.room_id=g.id WHERE i.user_id=? AND i.status='pending' AND g.group_type=1 ORDER BY i.updated_at DESC LIMIT 200",{Id(user)}))
            invitations->push_back({Group(row),std::stoll(row[6])});
        return {};
    } catch (const std::exception& e) { return {500,e.what()}; }
}
ServiceStatus MySqlSpaceRepository::Discover(int offset,int limit,std::vector<ImGroup>* rooms) {
    if(!pool_ || !rooms)return {503,"public directory unavailable"};
    rooms->clear();
    try {
        auto guard=pool_->Acquire();
        for(const auto& row:Query(guard.get(),"SELECT id,name,owner_id,group_type,description,visibility FROM im_group WHERE group_type=1 AND visibility='public' ORDER BY id DESC LIMIT ? OFFSET ?",{Id(limit),Id(offset)}))
            rooms->push_back(Group(row));
        return {};
    } catch(const std::exception& e){return {500,e.what()};}
}
}
