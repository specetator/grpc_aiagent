#pragma once
#include "services/media_repository.h"
#include "services/conversation_service.h"

namespace sparkpush {
class RoomMediaService {
 public:
    RoomMediaService(MediaRepository* repository, ConversationService* conversations)
        : repository_(repository), conversations_(conversations) {}
    ServiceStatus Execute(int64_t actor, const std::string& operation,
                          const nlohmann::json& input, nlohmann::json* output);
 private:
    ServiceStatus Access(int64_t, const std::string&);
    MediaRepository* repository_;
    ConversationService* conversations_;
};
}  // namespace sparkpush
