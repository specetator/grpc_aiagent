#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "image_attachment.h"
#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
constexpr uint64_t kMediaChunkBytes = 1024 * 1024;
constexpr uint64_t kMediaMaxBytes = 1024ULL * 1024 * 1024;
constexpr uint64_t kMediaRoomBytes = 10ULL * 1024 * 1024 * 1024;

struct MediaChunk {
    uint64_t index{0}, bytes{0};
    std::string sha256;
};
struct RoomMedia {
    StoredAttachment file;
    uint64_t ordinal{0};
    std::string kind;
    std::vector<MediaChunk> chunks;
};
struct MediaUpload {
    int64_t actor{0}, room{0};
    std::string session, client_id, name, mime, kind, sha256;
    uint64_t bytes{0};
};

// The attachment row is the sole authority for owner, room, original bytes and
// digest. Media tables add upload manifests and processing state, not copies.
class MediaRepository {
 public:
    virtual ~MediaRepository() = default;
    virtual ServiceStatus Create(const MediaUpload&, RoomMedia*) = 0;
    virtual ServiceStatus Find(const std::string&, RoomMedia*) = 0;
    virtual ServiceStatus PutChunk(const std::string&, uint64_t, const std::string&, RoomMedia*) = 0;
    virtual ServiceStatus Finalize(const std::string&, RoomMedia*) = 0;
    virtual ServiceStatus Abort(const std::string&, RoomMedia*) = 0;
    virtual ServiceStatus List(const std::string&, uint64_t, int, std::vector<RoomMedia>*) = 0;
    virtual ServiceStatus Uploads(const std::string&, int64_t, std::vector<RoomMedia>*) = 0;
    virtual ServiceStatus Read(const RoomMedia&, uint64_t, uint64_t, std::string*) = 0;
};
}  // namespace sparkpush
