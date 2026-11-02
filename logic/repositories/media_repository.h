#pragma once
#include "mysql_pool.h"
#include "repositories/media_blob_store.h"

namespace sparkpush {
class MySqlMediaRepository final : public MediaRepository {
 public:
    MySqlMediaRepository(MySqlConnectionPool* pool, std::string root)
        : pool_(pool), blobs_(std::move(root)) {}
    bool CheckSchema();
    ServiceStatus Create(const MediaUpload&, RoomMedia*) override;
    ServiceStatus Find(const std::string&, RoomMedia*) override;
    ServiceStatus PutChunk(const std::string&, uint64_t, const std::string&, RoomMedia*) override;
    ServiceStatus Finalize(const std::string&, RoomMedia*) override;
    ServiceStatus Abort(const std::string&, RoomMedia*) override;
    ServiceStatus List(const std::string&, uint64_t, int, std::vector<RoomMedia>*) override;
    ServiceStatus Uploads(const std::string&, int64_t, std::vector<RoomMedia>*) override;
    ServiceStatus Read(const RoomMedia&, uint64_t, uint64_t, std::string*) override;
 private:
    MySqlConnectionPool* pool_;
    MediaBlobStore blobs_;
};
}  // namespace sparkpush
