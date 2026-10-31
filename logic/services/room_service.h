#pragma once
#include "model.h"
#include "services/service_status.h"
#include "services/space_repository.h"

namespace sparkpush {
class ConversationStore;
class GroupDao;
class GroupMemberDao;
class RedisStore;
struct RoomSummary {
    Session session;
    std::string name;
};

// 聊天室的订阅、在线人数与路由维护。
//
// 读本类之前先分清四个**极易混淆**的概念，它们分别存在不同地方：
//
//   概念            方法                    存储          生命周期
//   持久订阅        Join / Unsubscribe      MySQL         直到显式退订
//   页面离开        Leave                   无（空操作）  ——
//   WS 在线存在     ReportRoomJoin/Leave    Redis 计数    连接断开就消失
//   在线人数展示    OnlineCount             Redis 优先    即时
//
// 混淆它们会造成很难查的 bug：例如把 Leave 写成退订，用户刷新一下
// 页面就再也收不到房间消息；或者把 ReportRoomLeave 写成 RemoveMember，
// 一次断线重连就会把用户的订阅清掉。
class RoomService {
   public:
    RoomService(ConversationStore* store, GroupDao* groups, GroupMemberDao* members,
                RedisStore* redis, SpaceRepository* spaces = nullptr)
        : store_(store), groups_(groups), members_(members), redis_(redis), spaces_(spaces) {}

    ServiceStatus CheckMember(int64_t actor, int64_t room);
    ServiceStatus FilterMembers(int64_t room, const std::vector<int64_t>& candidates,
                                std::vector<int64_t>* allowed);
    ServiceStatus CreateSpace(int64_t actor, const std::string& name, const std::string& description,
                               const std::string& visibility, int64_t* room);
    ServiceStatus ListSpaces(int64_t actor, int offset, int limit, std::vector<SpaceSummary>* rooms);
    ServiceStatus DiscoverSpaces(int64_t actor,int offset,int limit,std::vector<ImGroup>* rooms);
    ServiceStatus UpdateSpace(int64_t actor, int64_t room, const std::string& name,
                               const std::string& description, const std::string& visibility);
    ServiceStatus Members(int64_t actor, int64_t room, std::vector<SpaceMember>* members);
    ServiceStatus Invite(int64_t actor, int64_t room, int64_t target);
    ServiceStatus Invitations(int64_t actor, std::vector<SpaceInvitation>* invitations);
    ServiceStatus Accept(int64_t actor, int64_t room);
    ServiceStatus ChangeRole(int64_t actor, int64_t room, int64_t target, int role);
    ServiceStatus Remove(int64_t actor, int64_t room, int64_t target);

    // 列出用户已订阅的聊天室，同时补全房间名。
    // 单个房间查不到只记日志并 continue，不让整个列表失败：
    // 成员表里可能有已删除房间的残留行。
    ServiceStatus ListSubscriptions(int64_t actor, int64_t user,
                                    std::vector<RoomSummary>* rooms);

    // 公开空间主动加入；私有空间必须先接受邀请。重复 Join 保留角色。
    // 注意：本方法**不**改 Redis 在线计数，那是 ReportRoomJoin 的职责。
    ServiceStatus Join(int64_t actor, int64_t user, int64_t room);

    // 空操作，只校验参数与身份。
    // Leaving the HTTP page is not unsubscribing or changing WS presence.
    // 译：离开 HTTP 页面既不是退订，也不改变 WS 在线状态。
    // 保留这个接口只是为了兼容前端已有的调用；若将来真要让它做事，
    // 必须先想清楚它应该动哪一层（几乎肯定不是 MySQL 订阅）。
    ServiceStatus Leave(int64_t actor, int64_t user, int64_t room);

    // 正式退出：删 group_member，保留邀请/撤销状态；owner 不能退出。
    ServiceStatus Unsubscribe(int64_t actor, int64_t user, int64_t room);

    // 以下两个是管理员接口，先过 RequireAdministrator。
    ServiceStatus Create(int64_t actor, const std::string& name, int64_t owner,
                         int64_t* room);
    ServiceStatus List(int64_t actor, int offset, int limit,
                       std::vector<ImGroup>* rooms);

    // 在线人数。Redis 计数优先，拿不到时退化到 MySQL 成员总数。
    // 注意两者语义不同：前者是「当前连着 WS 的人数」，
    // 后者是「订阅过的人数」（包括离线的），因此兜底值会偏大。
    // 这是一个展示用数字，偏大可接受；若将来要用它做权限或计费判定，
    // 必须先分清这两个语义。
    ServiceStatus MemberOnlineCount(int64_t actor, int64_t room, int64_t* count);
    ServiceStatus OnlineCount(int64_t room, int64_t* count);

    // Durable membership and online presence are DIFFERENT use cases.
    // 译：持久订阅与在线存在是两个不同的用例。
    //
    // 下面两个 Report* 由 Comet 在 WS 控制消息（room_join/room_leave）时调，
    // Join 先查 MySQL 成员资格，再维护 Redis；Leave 只释放在线计数。
    //   全局在线数   room 维度的 INCRBY，供 OnlineCount 读
    //   分 comet 计数 (room_id, comet_id) 维度，驱动 room:comets:{room_id} 路由集
    // 第二层才是关键：广播时靠这个集合知道「该往哪几个 Comet 推」。
    // 0->1 时加入集合、降到 <=0 时移除，避开把无人房间也扇出一遍。
    ServiceStatus ReportRoomJoin(int64_t room_id, const std::string& comet_id,
                                 int64_t user_id);
    ServiceStatus ReportRoomLeave(int64_t room_id, const std::string& comet_id,
                                  int64_t user_id);

   private:
    ConversationStore* store_;
    GroupDao* groups_;
    GroupMemberDao* members_;
    RedisStore* redis_;
    SpaceRepository* spaces_;
};
}  // namespace sparkpush
