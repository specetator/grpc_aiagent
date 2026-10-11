#pragma once

#include "services/repositories.h"
#include "services/service_status.h"

namespace sparkpush {
struct MessagePage {
    std::vector<Message> messages;
    bool has_more{false};
    int64_t next_seq{0};
    int64_t safe_seq{0};
    std::string next_session_id;
};
struct DeliveryCursor {
    int64_t user_id{0};
    std::string session_id;
    int64_t msg_seq{0};
};

class ConversationService {
   public:
    explicit ConversationService(ConversationRepository* repository)
        : repository_(repository) {}
    bool Available() const;
    // Shared object-level authorization for history, sync and attachments.
    // Only image upload may lazily create a canonical single-chat session.
    ServiceStatus CheckAccess(int64_t user_id, const std::string& session_id,
                              Session* session, bool create_single = false);
    ServiceStatus History(int64_t user_id, const std::string& session_id,
                          int64_t anchor, int limit, std::vector<Message>* messages);
    ServiceStatus SyncAfter(int64_t user_id, const std::string& session_id,
                            int64_t after, int limit, MessagePage* page);
    ServiceStatus SyncOffline(int64_t user_id, int limit, MessagePage* page,
                              const std::string& after_session = "", int64_t after_seq = 0);
    ServiceStatus MarkDelivered(int64_t user_id, bool has_user_cursors,
                                const std::vector<DeliveryCursor>& cursors);
    // Preserve the existing identity-only read/unread contract in this
    // structural refactor; do not silently change authorization semantics.
    ServiceStatus MarkRead(int64_t actor, int64_t user_id, const std::string& session,
                           int64_t sequence);
    ServiceStatus Unread(int64_t actor, int64_t user_id, const std::string& session,
                         int64_t* count);
    ServiceStatus ListSingles(int64_t actor, int64_t user_id,
                              std::vector<Session>* sessions);

   private:
    bool MembersAvailable() const;
    ConversationRepository* repository_;
};
}  // namespace sparkpush
