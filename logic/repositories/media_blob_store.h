#pragma once
#include "services/media_repository.h"
#include <memory>

namespace sparkpush {
class MediaFileLock {
 public:
    explicit MediaFileLock(int fd) : fd_(fd) {}
    ~MediaFileLock();
    MediaFileLock(const MediaFileLock&) = delete;
    MediaFileLock& operator=(const MediaFileLock&) = delete;
 private:
    int fd_;
};
// All names are server-generated attachment IDs. No user filename or model path
// reaches openat. Parser workers will receive authorized bytes, not this root.
class MediaBlobStore {
 public:
    explicit MediaBlobStore(std::string root) : root_(std::move(root)) {}
    // Acquire BEFORE any SQL asset lock. Locks persist as zero-byte files and
    // are never unlinked: all processes sharing the volume lock the same inode.
    std::unique_ptr<MediaFileLock> Lock(const std::string&) const;
    void WriteChunk(const std::string&, uint64_t, const std::string&) const;
    void Assemble(const RoomMedia&) const;
    std::string Read(const RoomMedia&, uint64_t offset, uint64_t length) const;
    void RemoveChunks(const RoomMedia&) const;
    // Under the caller's fixed file lock, remove chunks and assembly temporaries
    // while preserving the immutable final. Used before advancing a derived
    // revision to another index, so crashed stages cannot accumulate duplicates.
    void RemoveStaging(const RoomMedia&) const;
    // Caller holds the asset file lock and has durably marked it cancelling
    // (or previously aborted). Includes publication left by SQL rollback.
    void DiscardUpload(const RoomMedia&) const;
 private:
    std::string root_;
};
}  // namespace sparkpush
