#include "services/room_service.h"
#include <algorithm>
#include <unordered_set>

#include "conversation_store.h"
#include "group_dao.h"
#include "logging.h"
#include "redis_store.h"
#include "services/business_rules.h"

namespace sparkpush {
namespace {
ServiceStatus RoomActor(int64_t actor, int64_t user, int64_t room) {
    if (user != actor) return {403, "user_id does not match bearer token"};
    if (room <= 0 || user <= 0) return {400, "room_id/user_id required"};
    return {};
}
}  // namespace
ServiceStatus RoomService::ListSubscriptions(int64_t actor, int64_t user,
                                             std::vector<RoomSummary>* rooms) {
    if (user != actor) return {403, "user_id does not match bearer token"};
    if (user <= 0) return {400, "user_id required"};
    if (!members_) return {500, "group member dao not initialized"};
    if (!groups_) return {500, "group dao not initialized"};
    std::vector<int64_t> ids;
    std::string error;
    if (!members_->ListUserChatrooms(user, &ids, &error))
        return {500, "list user chatrooms failed: " + error};
    if (!store_) return {500, "conversation store not initialized"};
    if (!rooms) return {500, "room output is null"};
    rooms->clear();
    for (int64_t id : ids) {
        Session session;
        // 单个房间失败不影响整份列表：成员表里可能残留已删除房间。
        // 名字查不到时用 "Unknown"，而不是把这一项丢掉——
        // 丢掉会让用户以为自己没订阅过。
        if (!store_->GetOrCreateRoomSession(id, &session, &error)) {
            LOG_ERROR << "GetOrCreateRoomSession failed: " << error;
            continue;
        }
        ImGroup group;
        std::string name = "Unknown";
        if (groups_->GetGroup(id, &group, &error)) name = group.name;
        rooms->push_back({std::move(session), std::move(name)});
    }
    return {};
}
ServiceStatus RoomService::Join(int64_t actor, int64_t user, int64_t room) {
    auto status = RoomActor(actor, user, room);
    if (!status.ok()) return status;
    if (!spaces_) return {500, "space repository unavailable"};
    return spaces_->WithRoom(room, [&](SpaceTransaction& tx) -> ServiceStatus {
        if (tx.Role(user) >= 0) {
            if(tx.Invitation(user)=="pending") tx.SetInvitation(user,user,"accepted");
            return {}; // Never downgrade an existing role.
        }
        if (tx.Group().visibility != "public" || tx.Invitation(user) == "revoked")
            return {403, "accept a space invitation before joining"};
        if (!tx.ActiveUser(user)) return {403, "active user required"};
        tx.SetMember(user, kRoomMember);
        if(tx.Invitation(user)=="pending") tx.SetInvitation(user,user,"accepted");
        return {};
    });
}
ServiceStatus RoomService::Leave(int64_t actor, int64_t user, int64_t room) {
    // Leaving the HTTP page is not unsubscribing or changing WS presence.
    // 译：离开 HTTP 页面既不是退订，也不改变 WS 在线状态。
    // 故意只校验参数然后返回成功。若在这里调 RemoveMember 或
    // ReportRoomLeave，用户刷新页面就会丢掉订阅，或把在线数减成负数。
    return RoomActor(actor, user, room);
}
ServiceStatus RoomService::Unsubscribe(int64_t actor, int64_t user, int64_t room) {
    auto status = RoomActor(actor, user, room);
    if (!status.ok()) return status;
    if (!spaces_) return {500, "space repository unavailable"};
    return spaces_->WithRoom(room, [&](SpaceTransaction& tx) -> ServiceStatus {
        if (tx.Group().owner_id == actor) return {409, "owner cannot leave the space"};
        const auto invitation = tx.Invitation(actor);
        tx.RemoveMember(actor);
        // An administrative revocation cannot be cleared by a later leave call.
        if (invitation != "revoked") tx.SetInvitation(actor, actor, "left");
        return {};
    });
}
ServiceStatus RoomService::Create(int64_t actor, const std::string& name, int64_t owner,
                                  int64_t* room) {
    const auto access = RequireAdministrator(actor);
    if (!access.ok()) return access;
    if (name.empty() || owner <= 0) return {400, "name/owner_id required"};
    if (!groups_) return {500, "group dao not initialized"};
    std::string error;
    if (!room || !groups_->CreateChatroom(name, owner, room, &error))
        return {500, "create chatroom failed: " + error};
    return {};
}
ServiceStatus RoomService::List(int64_t actor, int offset, int limit,
                                std::vector<ImGroup>* rooms) {
    const auto access = RequireAdministrator(actor);
    if (!access.ok()) return access;
    if (!groups_) return {500, "group dao not initialized"};
    std::string error;
    if (!rooms || !groups_->ListChatrooms(offset, limit, rooms, &error))
        return {500, "list chatrooms failed: " + error};
    return {};
}
ServiceStatus RoomService::MemberOnlineCount(int64_t actor, int64_t room, int64_t* count) {
    auto access=CheckMember(actor,room);
    return access.ok() ? OnlineCount(room,count) : access;
}
ServiceStatus RoomService::OnlineCount(int64_t room, int64_t* count) {
    if (room <= 0) return {400, "room_id required"};
    if (!count) return {500, "count output is null"};
    *count = 0;
    // Redis 没有计数时退化成 MySQL 成员总数。两者语义不同：
    // 前者是当前 WS 在线人数，后者包含离线订阅者，所以兜底值偏大。
    // 这是展示数字。兜底查询失败只记日志，count 保持 0，仍然返回成功：
    // 一次成员查询失败不该让整个在线人数接口 500。
    const bool found = redis_ && redis_->GetRoomOnlineCount(room, count);
    if (!found && members_) {
        std::vector<int64_t> members;
        std::string error;
        if (members_->ListRoomMembers(room, &members, &error))
            *count = members.size();
        else
            LOG_ERROR << "ListRoomMembers failed when fallback online count: " << error;
    }
    return {};
}
ServiceStatus RoomService::ReportRoomJoin(int64_t room_id, const std::string& comet_id,
                                          int64_t user_id) {
    const auto access = CheckMember(user_id, room_id);
    if (!access.ok()) return access;
    if (!redis_) {
        return ServiceStatus{500, "redis store not initialized"};
    }
    if (room_id <= 0 || comet_id.empty()) {
        return ServiceStatus{400, "room_id/comet_id required"};
    }

    // user_id<=0 只维护分 comet 路由计数，不加全局在线人数。
    // 全局人数与分 comet 计数是两次独立 INCRBY：前一步成功、后一步失败时，
    // 人数已经加上，调用方重试会再加一次。这是展示计数，允许漂移；
    // 下面的路由集合才影响投递，不能用同样的态度对待。
    if (user_id > 0) {
        if (!redis_->IncrRoomOnlineCount(room_id, 1)) {
            return ServiceStatus{500, "IncrRoomOnlineCount(+1) failed"};
        }
    }

    // 按 (room_id, comet_id) 维度维护计数，驱动 room:comets:{room_id}。
    // 同一个 Comet 上可以有多个用户在同一房间，只有 0->1 才加入路由集。
    // 计数和 SADD 不是一个原子操作：INCR 已经到 1 但 AddRoomComet 失败时，
    // 重试会把计数加到 2，new_count==1 不再成立，路由集合补不回去。
    // 改这个 ==1 判断之前，要先想清楚失败之后如何再走一次加入。
    int64_t new_count = 0;
    if (!redis_->IncrRoomCometCount(room_id, comet_id, 1, &new_count)) {
        return ServiceStatus{500, "IncrRoomCometCount(+1) failed"};
    }
    if (new_count == 1) {
        if (!redis_->AddRoomComet(room_id, comet_id)) {
            return ServiceStatus{500, "AddRoomComet failed"};
        }
    }

    return ServiceStatus{0, "ok"};
}

ServiceStatus RoomService::ReportRoomLeave(int64_t room_id, const std::string& comet_id,
                                           int64_t user_id) {
    if (!redis_) {
        return ServiceStatus{500, "redis store not initialized"};
    }
    if (room_id <= 0 || comet_id.empty()) {
        return ServiceStatus{400, "room_id/comet_id required"};
    }

    if (user_id > 0) {
        if (!redis_->IncrRoomOnlineCount(room_id, -1)) {
            return ServiceStatus{500, "IncrRoomOnlineCount(-1) failed"};
        }
    }

    int64_t new_count = 0;
    if (!redis_->IncrRoomCometCount(room_id, comet_id, -1, &new_count)) {
        return ServiceStatus{500, "IncrRoomCometCount(-1) failed"};
    }
    // 用 <=0 而不是 ==0：计数漂移成负数时也要把 comet 摘掉，
    // 否则会一直往一个没人的节点扇出。
    // 与 Join 对称：RemoveRoomComet 失败时计数已经 <=0，
    // 重试会再减一次，更到不了「恰好这次移除」的条件。
    if (new_count <= 0) {
        if (!redis_->RemoveRoomComet(room_id, comet_id)) {
            return ServiceStatus{500, "RemoveRoomComet failed"};
        }
    }

    return ServiceStatus{0, "ok"};
}


namespace {
ServiceStatus SpaceFields(const std::string& name, const std::string& description, const std::string& visibility) {
    if (name.empty() || name.size() > 128 || name.find_first_not_of(" \t\r\n") == std::string::npos ||
        description.size() > 2048 || name.find('\0') != std::string::npos || description.find('\0') != std::string::npos)
        return {400, "name must contain 1..128 bytes; description at most 2048 bytes"};
    if (visibility != "private" && visibility != "public") return {400, "visibility must be private or public"};
    return {};
}
}
ServiceStatus RoomService::CheckMember(int64_t actor, int64_t room) {
    if (actor <= 0 || room <= 0) return {400,"user_id/room_id required"};
    if (!members_) return {500,"group member dao not initialized"};
    bool member=false; std::string error;
    if (!members_->IsMember(room,actor,&member,&error)) return {500,error};
    return member ? ServiceStatus{} : ServiceStatus{403,"room membership required"};
}
ServiceStatus RoomService::FilterMembers(int64_t room, const std::vector<int64_t>& candidates, std::vector<int64_t>* allowed) {
    if (!allowed || room<=0 || candidates.size()>1000) return {400,"invalid room access request"};
    allowed->clear();
    if (!members_) return {500,"group member dao not initialized"};
    std::vector<int64_t> users; std::string error;
    if (!members_->ListRoomMembers(room,&users,&error)) return {500,error};
    std::unordered_set<int64_t> membership(users.begin(),users.end());
    for(auto user:candidates) if(membership.count(user)) allowed->push_back(user);
    return {};
}
ServiceStatus RoomService::CreateSpace(int64_t actor, const std::string& name, const std::string& description,
    const std::string& visibility, int64_t* room) {
    if(actor<=0) return {403,"user required"};
    auto status=SpaceFields(name,description,visibility); if(!status.ok()) return status;
    if(!spaces_) return {500,"space repository unavailable"};
    return spaces_->Create(actor,name,description,visibility,room);
}
ServiceStatus RoomService::ListSpaces(int64_t actor,int offset,int limit,std::vector<SpaceSummary>* rooms) {
    if(actor<=0) return {403,"user required"};
    if(!spaces_) return {500,"space repository unavailable"};
    return spaces_->List(actor,std::max(0,offset),std::clamp(limit,1,200),rooms);
}
ServiceStatus RoomService::DiscoverSpaces(int64_t actor,int offset,int limit,std::vector<ImGroup>* rooms) {
    if(actor<=0 || actor==kAdminUserId)return {403,"user account required"};
    if(!spaces_)return {503,"space repository unavailable"};
    return spaces_->Discover(std::max(0,offset),std::clamp(limit,1,100),rooms);
}
ServiceStatus RoomService::UpdateSpace(int64_t actor,int64_t room,const std::string& name,const std::string& description,const std::string& visibility) {
    auto status=SpaceFields(name,description,visibility); if(!status.ok()) return status;
    if(!spaces_) return {500,"space repository unavailable"};
    return spaces_->WithRoom(room,[&](SpaceTransaction& tx)->ServiceStatus {
        if(tx.Role(actor)!=kRoomOwner || tx.Group().owner_id!=actor) return {403,"only owner can update the space"};
        tx.Update(name,description,visibility); return {};
    });
}
ServiceStatus RoomService::Members(int64_t actor,int64_t room,std::vector<SpaceMember>* members) {
    if(!spaces_ || !members) return {500,"space repository unavailable"};
    return spaces_->WithRoom(room,[&](SpaceTransaction& tx)->ServiceStatus {
        if(tx.Role(actor)<0) return {403,"room membership required"};
        *members=tx.Members(); return {};
    });
}
ServiceStatus RoomService::Invite(int64_t actor,int64_t room,int64_t target) {
    if(target<=0) return {400,"user_id required"};
    if(!spaces_) return {500,"space repository unavailable"};
    return spaces_->WithRoom(room,[&](SpaceTransaction& tx)->ServiceStatus {
        if(tx.Role(actor)<kRoomAdmin) return {403,"owner/admin required"};
        if(!tx.ActiveUser(target)) return {404,"active user not found"};
        if(tx.Role(target)>=0) return {409,"user is already a member"};
        tx.SetInvitation(target,actor,"pending"); return {};
    });
}
ServiceStatus RoomService::Invitations(int64_t actor,std::vector<SpaceInvitation>* invitations) {
    if(actor<=0) return {403,"user required"};
    if(!spaces_) return {500,"space repository unavailable"};
    return spaces_->Invitations(actor,invitations);
}
ServiceStatus RoomService::Accept(int64_t actor,int64_t room) {
    if(actor<=0) return {403,"user required"};
    if(!spaces_) return {500,"space repository unavailable"};
    return spaces_->WithRoom(room,[&](SpaceTransaction& tx)->ServiceStatus {
        if(!tx.ActiveUser(actor)) return {403,"active user required"};
        if(tx.Role(actor)>=0) {
            if(tx.Invitation(actor)=="pending") tx.SetInvitation(actor,actor,"accepted");
            return {};
        }
        if(tx.Invitation(actor)!="pending") return {403,"pending invitation required"};
        tx.SetMember(actor,kRoomMember); tx.SetInvitation(actor,actor,"accepted"); return {};
    });
}
ServiceStatus RoomService::ChangeRole(int64_t actor,int64_t room,int64_t target,int role) {
    if(role!=kRoomMember && role!=kRoomAdmin) return {400,"role must be member or admin"};
    if(!spaces_) return {500,"space repository unavailable"};
    return spaces_->WithRoom(room,[&](SpaceTransaction& tx)->ServiceStatus {
        if(tx.Role(actor)!=kRoomOwner || tx.Group().owner_id!=actor) return {403,"only owner can change roles"};
        if(target==tx.Group().owner_id) return {409,"owner role cannot be changed"};
        if(tx.Role(target)<0) return {404,"member not found"};
        tx.SetMember(target,role); return {};
    });
}
ServiceStatus RoomService::Remove(int64_t actor,int64_t room,int64_t target) {
    if(target<=0) return {400,"user_id required"};
    if(!spaces_) return {500,"space repository unavailable"};
    return spaces_->WithRoom(room,[&](SpaceTransaction& tx)->ServiceStatus {
        const auto role=tx.Role(actor);
        if(role<kRoomAdmin) return {403,"owner/admin required"};
        if(target==tx.Group().owner_id || actor==target) return {403,"use leave; owner cannot be removed"};
        if(role==kRoomAdmin && tx.Role(target)>=kRoomAdmin) return {403,"admin can only remove ordinary members"};
        tx.RemoveMember(target); tx.SetInvitation(target,actor,"revoked"); return {};
    });
}

}  // namespace sparkpush
