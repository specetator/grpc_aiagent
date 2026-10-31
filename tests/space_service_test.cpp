#include <iostream>
#include <map>
#include <set>
#include <stdexcept>
#include "services/room_service.h"
using namespace sparkpush;
void Require(bool ok,const char* reason) { if(!ok) throw std::runtime_error(reason); }
class MemorySpace final : public SpaceTransaction {
 public:
    ImGroup group;
    std::map<int64_t,int> members;
    std::map<int64_t,std::string> invitations;
    const ImGroup& Group() const override { return group; }
    int Role(int64_t user) override { auto it=members.find(user);return it==members.end()?-1:it->second; }
    bool ActiveUser(int64_t user) override { return user>0 && user<=5; }
    std::string Invitation(int64_t user) override { return invitations[user]; }
    void SetInvitation(int64_t user,int64_t,const std::string& status) override { invitations[user]=status; }
    void SetMember(int64_t user,int role) override { members[user]=role; }
    void RemoveMember(int64_t user) override { members.erase(user); }
    void Update(const std::string& name,const std::string& description,const std::string& visibility) override {
        group.name=name;group.description=description;group.visibility=visibility;
    }
    std::vector<SpaceMember> Members() override { std::vector<SpaceMember> result;for(auto m:members) result.push_back({m.first,m.second,{},{}});return result; }
};
class MemoryRepository final : public SpaceRepository {
 public:
    MemorySpace state;
    ServiceStatus Create(int64_t owner,const std::string& name,const std::string& description,const std::string& visibility,int64_t* room) override {
        state.group.id=*room=1;state.group.owner_id=owner;state.group.group_type=1;state.Update(name,description,visibility);state.members[owner]=2;return {};
    }
    ServiceStatus WithRoom(int64_t room,const std::function<ServiceStatus(SpaceTransaction&)>& fn) override {
        if(room!=state.group.id) return {404,"not found"};
        auto before=state;auto status=fn(state);if(!status.ok()) state=before;return status;
    }
    ServiceStatus List(int64_t,int,int,std::vector<SpaceSummary>*) override { return {}; }
    ServiceStatus Invitations(int64_t,std::vector<SpaceInvitation>*) override { return {}; }
};
int main() {
    MemoryRepository repo;RoomService service(nullptr,nullptr,nullptr,nullptr,&repo);int64_t room=0;
    Require(service.CreateSpace(1,"O'Reilly 空间","description","private",&room).ok(),"ordinary user create");
    Require(repo.state.Role(1)==2 && repo.state.group.visibility=="private","owner private membership");
    Require(service.Join(2,2,room).code==403,"private ID guessing denied");
    Require(service.Invite(2,room,3).code==403,"member management denied");
    Require(service.Invite(1,room,99).code==404,"invite only existing users");
    Require(service.Invite(1,room,2).ok() && repo.state.Role(2)==-1,"invitation is not membership");
    Require(service.Accept(3,room).code==403,"another user cannot accept");
    Require(service.Accept(2,room).ok() && service.Accept(2,room).ok(),"accept idempotent");
    Require(service.ChangeRole(2,room,2,1).code==403,"no self promotion");
    Require(service.ChangeRole(1,room,2,1).ok(),"owner promotes admin");
    Require(service.Join(2,2,room).ok() && repo.state.Role(2)==1,"join preserves admin");
    Require(service.Invite(2,room,3).ok() && service.Accept(3,room).ok(),"admin invite");
    Require(service.ChangeRole(2,room,3,1).code==403,"admin cannot grant admin");
    Require(service.Remove(2,room,1).code==403,"owner cannot be removed");
    Require(service.Unsubscribe(1,1,room).code==409,"owner cannot leave");
    Require(service.ChangeRole(1,room,4,1).code==404,"cannot assign absent member");
    Require(service.ChangeRole(1,room,3,2).code==400,"cannot assign owner");
    Require(service.ChangeRole(1,room,1,0).code==409,"owner cannot demote self");
    Require(service.UpdateSpace(2,room,"bad","","public").code==403,"only owner updates space");
    Require(service.Remove(2,room,3).ok(),"admin removes ordinary member");
    Require(service.Accept(3,room).code==403,"old accepted invitation cannot restore access");
    Require(service.UpdateSpace(1,room,"public","","public").ok(),"owner updates visibility");
    Require(service.Join(3,3,room).code==403,"removed public member cannot rejoin");
    Require(service.Unsubscribe(3,3,room).ok() && service.Join(3,3,room).code==403,"leave cannot clear revocation");
    Require(service.Invite(1,room,4).ok(),"public invite before join");
    Require(service.Join(4,4,room).ok(),"legacy public join works");
    Require(repo.state.Invitation(4)=="accepted" && service.Accept(4,room).ok(),"public join consumes pending invite");
    Require(service.Unsubscribe(4,4,room).ok() && service.Join(4,4,room).ok(),"voluntary public leave can rejoin");
    Require(service.Invite(1,room,3).ok() && service.Accept(3,room).ok(),"explicit reinvite restores access");
    std::vector<SpaceMember> members;
    Require(service.Members(5,room,&members).code==403,"nonmember cannot list members");
    Require(service.Members(3,room,&members).ok() && members.size()==4,"member roster");
    Require(service.CreateSpace(1," ","","private",&room).code==400,"blank name rejected");
    std::cout<<"space service lifecycle and role policies passed\n";
}
