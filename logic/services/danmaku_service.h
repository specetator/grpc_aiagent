#pragma once
#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
class ConversationStore;
class DanmakuDao;
class GroupMemberDao;
class KafkaProducer;
class RedisStore;
struct DanmakuResult {
    ServiceStatus status;
    Message message;
    bool pushed{false};
};
class DanmakuService {
   public:
    DanmakuService(ConversationStore* store, DanmakuDao* dao, GroupMemberDao* members,
                   KafkaProducer* producer, RedisStore* redis)
        : store_(store),
          dao_(dao),
          members_(members),
          producer_(producer),
          redis_(redis) {}
    ServiceStatus SendAvailable() const;
    DanmakuResult Send(const std::string& token, const std::string& video,
                       int64_t timeline_ms, const std::string& text);
    ServiceStatus List(const std::string& video, int64_t from, int64_t to, int limit,
                       std::vector<DanmakuItem>* result);

   private:
    ConversationStore* store_;
    DanmakuDao* dao_;
    GroupMemberDao* members_;
    KafkaProducer* producer_;
    RedisStore* redis_;
};
}  // namespace sparkpush
