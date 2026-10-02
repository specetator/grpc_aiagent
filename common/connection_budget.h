#pragma once
#include <cstddef>
#include <mutex>
#include <string>
namespace sparkpush {
// Counts EventLoop-queued frames and the socket output buffer separately.
// Buffer sizes are published only by the owning EventLoop.
class ConnectionBudget {
public:
    explicit ConnectionBudget(size_t bytes = 8 * 1024 * 1024, size_t frames = 1024)
        : max_bytes_(bytes), max_frames_(frames) {}
    bool Reserve(size_t bytes) {
        std::lock_guard<std::mutex> lock(mu_);
        if (closed_ || frames_ >= max_frames_ || bytes > max_bytes_ ||
            queued_ > max_bytes_ - bytes || buffered_ > max_bytes_ - bytes - queued_) return false;
        queued_ += bytes; ++frames_; return true;
    }
    void Complete(size_t bytes, size_t buffered) {
        std::lock_guard<std::mutex> lock(mu_);
        queued_ -= bytes; --frames_; buffered_ = buffered;
    }
    void BufferDrained() { std::lock_guard<std::mutex> lock(mu_); buffered_ = 0; }
    void PublishBuffered(size_t bytes) { std::lock_guard<std::mutex> lock(mu_); buffered_ = bytes; }
    void Close() { std::lock_guard<std::mutex> lock(mu_); closed_ = true; }
    size_t limit() const { return max_bytes_; }
private:
    std::mutex mu_;
    size_t max_bytes_, max_frames_, queued_{0}, buffered_{0}, frames_{0};
    bool closed_{false};
};
inline bool ValidDeviceId(const std::string& id) {
    if (id.empty() || id.size() > 64) return false;
    for (unsigned char c : id)
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-')) return false;
    return true;
}
} // namespace sparkpush
