#pragma once
#include "services/media_repository.h"
#include "services/conversation_service.h"

namespace sparkpush {
constexpr uint64_t kMediaTextBytes = 16 * 1024 * 1024;
constexpr uint64_t kMediaFragmentBytes = 16 * 1024;
constexpr uint64_t kMediaFragmentCount = 2048;
struct MediaLease {
    std::string asset, revision, token;
    uint64_t attempt{0};
};
class MediaArtifactRepository {
 public:
    virtual ~MediaArtifactRepository()=default;
    virtual ServiceStatus Reserve(const MediaLease&)=0;
    virtual ServiceStatus Stage(const MediaLease&,uint64_t,const nlohmann::json&,const std::string&,nlohmann::json*)=0;
    virtual ServiceStatus Complete(const MediaLease&,const nlohmann::json&,nlohmann::json*)=0;
    virtual ServiceStatus Artifacts(const std::string&,const std::string&,uint64_t,uint64_t,nlohmann::json*)=0;
    virtual ServiceStatus Read(const std::string&,const std::string&,uint64_t,nlohmann::json*)=0;
    virtual ServiceStatus Reclaim(const std::string&)=0;
    virtual ServiceStatus ReclaimCandidates(const std::string&,uint64_t,nlohmann::json*)=0;
};
class MediaProcessingRepository {
 public:
    virtual ~MediaProcessingRepository() = default;
    virtual ServiceStatus Claim(const std::string& key, const std::string& pipeline, bool allocate, nlohmann::json*, const std::string& parser_sha="") = 0;
    virtual ServiceStatus Check(const MediaLease&, nlohmann::json*, bool require_live=true) = 0;
    virtual ServiceStatus Current(const std::string&,const std::string&)=0;
    virtual ServiceStatus Heartbeat(const MediaLease&) = 0;
    virtual ServiceStatus Stage(const MediaLease&, uint64_t, const nlohmann::json&, nlohmann::json*) = 0;
    virtual ServiceStatus Complete(const MediaLease&, uint64_t, const std::string&, nlohmann::json*) = 0;
    virtual ServiceStatus Fail(const MediaLease&, const std::string&) = 0;
    virtual ServiceStatus Info(const std::string&, nlohmann::json*) = 0;
    virtual ServiceStatus Fragments(const std::string&, const std::string&, uint64_t, uint64_t, nlohmann::json*) = 0;
    virtual ServiceStatus Search(const std::string&, const std::string&, uint64_t, nlohmann::json*) = 0;
};
// Worker identity is a separate operator credential. User requests always use
// current membership; model text cannot provide a room, worker key or lease.
class MediaProcessingService {
 public:
    MediaProcessingService(MediaProcessingRepository* jobs, MediaRepository* files,
        ConversationService* conversations, std::string worker_key, std::string pipeline_sha,
        MediaArtifactRepository* artifacts=nullptr, std::string media_pipeline="", std::string parser_sha="")
        : jobs_(jobs), files_(files), conversations_(conversations),
          key_(std::move(worker_key)), pipeline_(std::move(pipeline_sha)), artifacts_(artifacts),
          media_pipeline_(std::move(media_pipeline)), parser_(std::move(parser_sha)) {}
    ServiceStatus Worker(const std::string&, const std::string&, const nlohmann::json&, nlohmann::json*);
    ServiceStatus Member(int64_t, const std::string&, const nlohmann::json&, nlohmann::json*);
 private:
    ServiceStatus Access(int64_t, const std::string&);
    MediaProcessingRepository* jobs_;
    MediaRepository* files_;
    ConversationService* conversations_;
    std::string key_, pipeline_;
    MediaArtifactRepository* artifacts_;
    std::string media_pipeline_, parser_;
};
}  // namespace sparkpush
