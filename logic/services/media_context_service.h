#pragma once
#include "services/media_processing_service.h"
#include "services/room_agent_service.h"

namespace sparkpush {
// Populated only from an authenticated, persisted generic Room Agent request.
struct MediaContextScope {
    std::string request_id;
    nlohmann::json collaboration;
    int64_t message_ceiling{0};
    std::vector<int64_t> bot_users;
};
class MediaContextRepository {
 public:
    virtual ~MediaContextRepository()=default;
    virtual ServiceStatus Messages(const MediaContextScope&,const std::string& query,
        int64_t before,int64_t sequence,uint64_t offset,uint64_t length,uint64_t limit,nlohmann::json*)=0;
    virtual ServiceStatus Media(const MediaContextScope&,const std::string& query,uint64_t before,uint64_t limit,nlohmann::json*)=0;
    // Atomic, idempotent receipts for server-observed ranges, never model claims.
    virtual ServiceStatus Record(const MediaContextScope&,const nlohmann::json& references,nlohmann::json*)=0;
};
class MediaContextService {
 public:
    MediaContextService(RoomAgentService* agents,MediaProcessingService* processing,
        MediaRepository* files,MediaContextRepository* repository)
        : agents_(agents),processing_(processing),files_(files),repository_(repository) {}
    ServiceStatus Execute(const std::string& connector_key,const std::string& operation,
        const nlohmann::json& input,nlohmann::json* output);
 private:
    ServiceStatus Authorize(const std::string&,const std::string&,MediaContextScope*);
    RoomAgentService* agents_;
    MediaProcessingService* processing_;
    MediaRepository* files_;
    MediaContextRepository* repository_;
};
}  // namespace sparkpush
