#pragma once

#include "image_attachment.h"
#include "services/conversation_service.h"

namespace sparkpush {
struct AttachmentContents {
    StoredAttachment metadata;
    std::string bytes;
};
class AttachmentService {
   public:
    AttachmentService(AttachmentRepository* repository,
                      ConversationService* conversations)
        : repository_(repository), conversations_(conversations) {}
    bool Available() const;
    ServiceStatus Upload(int64_t user, const std::string& session,
                         const std::string& name, const std::string& base64_data,
                         StoredAttachment* attachment);
    ServiceStatus Read(int64_t user, const std::string& id, AttachmentContents* result);
    // Shared by all message transports. Checks ownership/readiness/session
    // binding before accepting any attachment reference into the message.
    ServiceStatus AuthorizeMessageImages(int64_t sender, const std::string& session,
                                         const std::string& content_json);

   private:
    AttachmentRepository* repository_;
    ConversationService* conversations_;
};
}  // namespace sparkpush
