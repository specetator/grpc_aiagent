#pragma once
#include "repositories/media_blob_store.h"
#include "services/media_processing_service.h"
#include "mysql_pool.h"

namespace sparkpush {
// Internal processing storage. No user/Agent may call this without the Service
// credential/lease policy. Public reads must additionally authorize membership
// and current publication; Repository reads alone do not grant membership.
class MySqlMediaArtifactRepository final : public MediaArtifactRepository {
 public:
    MySqlMediaArtifactRepository(MySqlConnectionPool* pool, std::string root)
        : pool_(pool), blobs_(std::move(root)) {}
    bool CheckSchema();
    ServiceStatus Reserve(const MediaLease&) override;
    // Metadata is immutable BEFORE any file IO. Retrying after connection loss
    // uses the same revision/index/bytes and cannot mint more uncharged files.
    ServiceStatus Stage(const MediaLease&, uint64_t index, const nlohmann::json& metadata,
                        const std::string& jpeg, nlohmann::json*) override;
    ServiceStatus Complete(const MediaLease&,const nlohmann::json&,nlohmann::json*) override;
    ServiceStatus Artifacts(const std::string&,const std::string&,uint64_t,uint64_t,nlohmann::json*) override;
    ServiceStatus Read(const std::string&,const std::string&,uint64_t,nlohmann::json*) override;
    // Terminal or expired, unpublished revisions only. A live/public revision
    // cannot be collected. Safe to repeat after any intermediate failure.
    ServiceStatus Reclaim(const std::string& revision) override;
    ServiceStatus ReclaimCandidates(const std::string& after,uint64_t limit,nlohmann::json*) override;
    static std::string ArtifactId(const std::string& revision, uint64_t index);
 private:
    MySqlConnectionPool* pool_;
    MediaBlobStore blobs_;
};
}  // namespace sparkpush
