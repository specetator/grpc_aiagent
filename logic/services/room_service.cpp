#include "services/room_service.h"

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
    if (!groups_ || !members_) return {500, "group dao not initialized"};
    ImGroup group;
    std::string error;
    if (!groups_->GetGroup(room, &group, &error))
        return {404, "chatroom not found: " + error};
    if (group.group_type != 1) return {400, "group is not chatroom"};
    if (!members_->AddOrUpdateMember(room, user, 0, &error))
        return {500, "add member failed: " + error};
    return {};
}
ServiceStatus RoomService::Leave(int64_t actor, int64_t user, int64_t room) {
    // Leaving the HTTP page is not unsubscribing or changing WS presence.
    return RoomActor(actor, user, room);
}
ServiceStatus RoomService::Unsubscribe(int64_t actor, int64_t user, int64_t room) {
    auto status = RoomActor(actor, user, room);
    if (!status.ok()) return status;
    if (!members_) return {500, "group member dao not initialized"};
    std::string error;
    if (!members_->RemoveMember(room, user, &error))
        return {500, "remove member failed: " + error};
    return {};
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
ServiceStatus RoomService::OnlineCount(int64_t room, int64_t* count) {
    if (room <= 0) return {400, "room_id required"};
    if (!count) return {500, "count output is null"};
    *count = 0;
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
    if (!redis_) {
        return ServiceStatus{500, "redis store not initialized"};
    }
    if (room_id <= 0 || comet_id.empty()) {
        return ServiceStatus{400, "room_id/comet_id required"};
    }

    // 1) 基于 WebSocket 控制消息，使用 INCRBY 维护全局聊天室在线人数
    if (user_id > 0) {
        if (!redis_->IncrRoomOnlineCount(room_id, 1)) {
            return ServiceStatus{500, "IncrRoomOnlineCount(+1) failed"};
        }
    }

    // 2) 按 (room_id, comet_id) 维度维护计数，用于精确维护
    // room:comets:{room_id}
    int64_t new_count = 0;
    if (!redis_->IncrRoomCometCount(room_id, comet_id, 1, &new_count)) {
        return ServiceStatus{500, "IncrRoomCometCount(+1) failed"};
    }
    if (new_count == 1) {
        // 该 room 在该 comet 上从 0 -> 1，加入 room:comets 路由集合
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
    if (new_count <= 0) {
        // 该 room 在该 comet 上已无在线用户，移除路由集合
        if (!redis_->RemoveRoomComet(room_id, comet_id)) {
            return ServiceStatus{500, "RemoveRoomComet failed"};
        }
    }

    return ServiceStatus{0, "ok"};
}

}  // namespace sparkpush
