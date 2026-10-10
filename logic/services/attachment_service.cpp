#include "services/attachment_service.h"

namespace sparkpush {
bool AttachmentService::Available() const {
    return repository_ && repository_->Available();
}
ServiceStatus AttachmentService::Upload(int64_t user, const std::string& session,
                                        const std::string& name,
                                        const std::string& data,
                                        StoredAttachment* attachment) {
    if (!Available()) return {503, "image attachments are not enabled"};
    if (session.empty() || data.empty())
        return {400, "session_id and data are required"};
    if (!conversations_) return {500, "conversation store not initialized"};
    Session value;
    auto status = conversations_->CheckAccess(user, session, &value, true);
    if (!status.ok()) {
        if (status.reason == ServiceReason::kMembershipLookupFailed)
            status.message = "check session membership failed: " + status.message;
        return status;
    }
    std::string bytes;
    if (!DecodeBase64(data, &bytes)) return {400, "invalid base64 image data"};
    std::string error;
    if (!attachment ||
        !repository_->Create(user, session, name, bytes, attachment, &error))
        return {400, error};
    return {};
}
ServiceStatus AttachmentService::Read(int64_t user, const std::string& id,
                                      AttachmentContents* result) {
    if (!Available()) return {503, "image attachments are not enabled"};
    if (!result) return {500, "attachment output is null"};
    *result = {};
    std::string error;
    if (!repository_->Find(id, &result->metadata, &error)) return {404, error};
    const auto& stored = result->metadata;
    if (stored.owner_user_id != user) {
        Session session;
        if (stored.session_id.empty() || !conversations_ ||
            !conversations_->CheckAccess(user, stored.session_id, &session).ok())
            return {403, "attachment is not visible"};
    }
    if (!repository_->Read(stored, &result->bytes, &error)) return {404, error};
    return {};
}
ServiceStatus AttachmentService::AuthorizeMessageImages(int64_t sender,
                                                        const std::string& session,
                                                        const std::string& content) {
    const auto refs = ExtractImageRefsFromContentJson(content);
    if (refs.empty()) return {};
    if (!Available()) return {400, "image attachments are not enabled"};
    for (const auto& ref : refs) {
        StoredAttachment stored;
        std::string error;
        if (!repository_->Find(ref.id, &stored, &error)) return {400, error};
        if (stored.owner_user_id != sender)
            return {400, "attachment does not belong to the sender"};
        if (stored.status != "ready") return {400, "attachment is not ready"};
        if (!stored.session_id.empty() && stored.session_id != session)
            return {400, "attachment belongs to another conversation"};
        if (stored.session_id.empty() && !repository_->Bind(ref.id, session, &error))
            return {400, error};
    }
    return {};
}
}  // namespace sparkpush
