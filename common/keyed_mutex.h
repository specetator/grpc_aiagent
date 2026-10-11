#pragma once
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
namespace sparkpush {
class KeyedMutex {
 public:
    std::shared_ptr<std::mutex> Get(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (++requests_ % 128 == 0) {
            for (auto it = entries_.begin(); it != entries_.end(); ) {
                if (it->second.expired()) it = entries_.erase(it); else ++it;
            }
        }
        auto& weak = entries_[key];
        auto result = weak.lock();
        if (!result) { result = std::make_shared<std::mutex>(); weak = result; }
        return result;
    }
 private:
    std::mutex mutex_;
    std::unordered_map<std::string, std::weak_ptr<std::mutex>> entries_;
    size_t requests_{0};
};
}  // namespace sparkpush
