#pragma once
#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
class ConversationStore;
class GroupDao;
class GroupMemberDao;
class RedisStore;
struct RoomSummary {
    Session session;
    std::string name;
};
class RoomService {
   public:
    RoomService(ConversationStore* store, GroupDao* groups, GroupMemberDao* members,
                RedisStore* redis)
        : store_(store), groups_(groups), members_(members), redis_(redis) {}
    ServiceStatus ListSubscriptions(int64_t actor, int64_t user,
                                    std::vector<RoomSummary>* rooms);
    ServiceStatus Join(int64_t actor, int64_t user, int64_t room);
    ServiceStatus Leave(int64_t actor, int64_t user, int64_t room);
    ServiceStatus Unsubscribe(int64_t actor, int64_t user, int64_t room);
    ServiceStatus Create(int64_t actor, const std::string& name, int64_t owner,
                         int64_t* room);
    ServiceStatus List(int64_t actor, int offset, int limit,
                       std::vector<ImGroup>* rooms);
    ServiceStatus OnlineCount(int64_t room, int64_t* count);
    // Durable subscription and online presence are DIFFERENT use cases.
    ServiceStatus ReportRoomJoin(int64_t room_id, const std::string& comet_id,
                                 int64_t user_id);
    ServiceStatus ReportRoomLeave(int64_t room_id, const std::string& comet_id,
                                  int64_t user_id);

   private:
    ConversationStore* store_;
    GroupDao* groups_;
    GroupMemberDao* members_;
    RedisStore* redis_;
};
}  // namespace sparkpush
