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
    if (!groups_ || !members_) return {500, "group dao not initialized"};
    ImGroup group;
    std::string error;
    if (!groups_->GetGroup(room, &group, &error))
        return {404, "chatroom not found: " + error};
    // group_type != 1 是普通群，不能走聊天室订阅。
    // 重复 Join 由 DAO 的 INSERT IGNORE 保证幂等，这里不预查成员。
    // 本方法不改 Redis 在线计数，那是 ReportRoomJoin 的事。
    if (group.group_type != 1) return {400, "group is not chatroom"};
    if (!members_->AddOrUpdateMember(room, user, 0, &error))
        return {500, "add member failed: " + error};
    return {};
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

}  // namespace sparkpush
