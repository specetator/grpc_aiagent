#pragma once
#include <functional>
#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
constexpr int kRoomMember = 0, kRoomAdmin = 1, kRoomOwner = 2;
struct SpaceMember { int64_t user_id{0}; int role{0}; std::string account, name; };
struct SpaceSummary { ImGroup group; int role{0}; };
struct SpaceInvitation { ImGroup group; int64_t invited_by{0}; };
// Storage operations in one locked-room transaction. Permission decisions are
// made by RoomService inside the callback, before any write is performed.
class SpaceTransaction {
 public:
    virtual ~SpaceTransaction() = default;
    virtual const ImGroup& Group() const = 0;
    virtual int Role(int64_t user) = 0; // -1: not a member
    virtual bool ActiveUser(int64_t user) = 0;
    virtual std::string Invitation(int64_t user) = 0;
    virtual void SetInvitation(int64_t user, int64_t inviter, const std::string& status) = 0;
    virtual void SetMember(int64_t user, int role) = 0;
    virtual void RemoveMember(int64_t user) = 0;
    virtual void Update(const std::string& name, const std::string& description, const std::string& visibility) = 0;
    virtual std::vector<SpaceMember> Members() = 0;
};
class SpaceRepository {
 public:
    virtual ~SpaceRepository() = default;
    virtual ServiceStatus Create(int64_t owner, const std::string& name, const std::string& description,
                                 const std::string& visibility, int64_t* room) = 0;
    virtual ServiceStatus WithRoom(int64_t room, const std::function<ServiceStatus(SpaceTransaction&)>& operation) = 0;
    virtual ServiceStatus List(int64_t user, int offset, int limit, std::vector<SpaceSummary>* rooms) = 0;
    virtual ServiceStatus Discover(int,int,std::vector<ImGroup>*) { return {503,"public directory unavailable"}; }
    virtual ServiceStatus Invitations(int64_t user, std::vector<SpaceInvitation>* invitations) = 0;
};
}
