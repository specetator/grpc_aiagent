#include "services/conversation_service.h"

#include <algorithm>

#include "logging.h"
#include "metrics.h"

namespace sparkpush {
namespace {
int SyncLimit(int limit) {
    return std::max(1, std::min(limit > 0 ? limit : 100, 1000));
}
bool ParseSingleParticipants(const std::string& id, int64_t* a, int64_t* b) {
    if (id.rfind("s_", 0) != 0) return false;
    const auto split = id.find('_', 2);
    if (split == std::string::npos) return false;
    try {
        *a = std::stoll(id.substr(2, split - 2));
        *b = std::stoll(id.substr(split + 1));
    } catch (...) {
        return false;
    }
    // A noncanonical spelling must not create a side-effecting DB row.
    return *a > 0 && *b > 0 && *a <= *b &&
           id == "s_" + std::to_string(*a) + "_" + std::to_string(*b);
}
}  // namespace

bool ConversationService::Available() const {
    return repository_ && repository_->Available();
}
bool ConversationService::MembersAvailable() const {
    return repository_ && repository_->MembersAvailable();
}

ServiceStatus ConversationService::CheckAccess(int64_t user_id,
                                               const std::string& session_id,
                                               Session* session, bool create_single) {
    if (!Available()) return {500, "conversation store not initialized"};
    if (!session) return {500, "session output is null"};
    std::string error;
    if (!repository_->FindSession(session_id, session, &error)) {
        int64_t a = 0, b = 0;
        if (!create_single || !ParseSingleParticipants(session_id, &a, &b) ||
            (user_id != a && user_id != b) ||
            !repository_->EnsureSingle(a, b, session, &error) ||
            session->id != session_id)
            return {404, "session not found"};
    }
    bool authorized = false;
    if (session->type == SessionType::kSingle) {
        authorized = session->user1_id == user_id || session->user2_id == user_id;
    } else if (session->group_id > 0 && MembersAvailable()) {
        if (!repository_->IsMember(session->group_id, user_id, &authorized, &error))
            return {500, error, ServiceReason::kMembershipLookupFailed};
    }
    if (!authorized) return {403, "user is not a member of this session"};
    return {};
}

ServiceStatus ConversationService::History(int64_t user, const std::string& session,
                                           int64_t anchor, int limit,
                                           std::vector<Message>* messages) {
    Session value;
    auto status = CheckAccess(user, session, &value);
    if (!status.ok()) {
        if (status.reason == ServiceReason::kMembershipLookupFailed)
            status.message = "check session membership failed: " + status.message;
        return status;
    }
    std::string error;
    if (!messages || !repository_->History(session, anchor, limit, messages, &error))
        return {500, "query history failed: " + error};
    return {};
}

ServiceStatus ConversationService::SyncAfter(int64_t user, const std::string& session,
                                             int64_t after, int limit,
                                             MessagePage* page) {
    if (page) *page = {};
    if (!Available() || !MembersAvailable() || !page || user <= 0 || session.empty() ||
        after < 0)
        return {400, "user_id/session_id are required"};
    Session value;
    auto status = CheckAccess(user, session, &value);
    if (!status.ok()) return status;
    const int bounded_limit = SyncLimit(limit);
    std::string error;
    if (!repository_->After(session, after, bounded_limit, &page->messages, &error))
        return {500, error};
    page->has_more = static_cast<int>(page->messages.size()) == bounded_limit;
    page->next_seq = page->messages.empty() ? after : page->messages.back().msg_seq;
    MetricsRegistry::Instance().Increment("spark_push_cursor_sync_total");
    return {};
}

ServiceStatus ConversationService::SyncOffline(int64_t user, int limit,
                                               MessagePage* page) {
    if (page) *page = {};
    if (!Available() || !MembersAvailable() || !page || user <= 0)
        return {400, "offline sync dependencies/user_id are missing"};
    std::vector<Session> sessions;
    std::string error;
    if (!repository_->Singles(user, &sessions, &error)) return {500, error};
    std::vector<int64_t> rooms;
    if (!repository_->Rooms(user, &rooms, &error))
        LOG_WARN << "ListUserChatrooms for offline sync failed: " << error;
    for (int64_t room_id : rooms) {
        Session room;
        if (repository_->FindSession("r_" + std::to_string(room_id), &room, &error))
            sessions.push_back(std::move(room));
    }
    int remaining = SyncLimit(limit);
    for (const auto& session : sessions) {
        if (remaining <= 0) break;
        int64_t delivered = 0;
        if (!repository_->Delivered(user, session.id, &delivered, &error)) {
            LOG_WARN << "GetDeliveredSeq failed for session " << session.id << ": "
                     << error;
            continue;
        }
        std::vector<Message> messages;
        if (!repository_->After(session.id, delivered, remaining, &messages, &error)) {
            LOG_WARN << "offline message query failed for session " << session.id
                     << ": " << error;
            continue;
        }
        page->messages.insert(page->messages.end(), messages.begin(), messages.end());
        remaining -= static_cast<int>(messages.size());
    }
    page->has_more = remaining == 0 && !sessions.empty();
    MetricsRegistry::Instance().Increment("spark_push_offline_sync_total");
    return {};
}

ServiceStatus ConversationService::MarkDelivered(
    int64_t user, bool has_user_cursors, const std::vector<DeliveryCursor>& cursors) {
    if (!Available() || !MembersAvailable() || (user <= 0 && !has_user_cursors))
        return {400, "user_id or user_cursors is required"};
    for (const auto& cursor : cursors) {
        if (cursor.user_id <= 0 || cursor.session_id.empty() || cursor.msg_seq < 0)
            continue;
        Session session;
        auto status = CheckAccess(cursor.user_id, cursor.session_id, &session);
        if (!status.ok()) return status;
        std::string error;
        if (!repository_->MarkDelivered(cursor.user_id, cursor.session_id,
                                        cursor.msg_seq, &error))
            return {500, error};
    }
    MetricsRegistry::Instance().Increment("spark_push_delivered_cursor_total");
    return {};
}

ServiceStatus ConversationService::MarkRead(int64_t actor, int64_t user,
                                            const std::string& session,
                                            int64_t sequence) {
    if (actor != user) return {403, "user_id does not match bearer token"};
    if (user <= 0 || session.empty() || sequence <= 0)
        return {400, "user_id/session_id/read_seq required"};
    if (!Available()) return {500, "conversation store not initialized"};
    std::string error;
    if (!repository_->MarkRead(user, session, sequence, &error))
        return {500, "mark read failed: " + error};
    return {};
}
ServiceStatus ConversationService::Unread(int64_t actor, int64_t user,
                                          const std::string& session, int64_t* count) {
    if (actor != user) return {403, "user_id does not match bearer token"};
    if (user <= 0 || session.empty()) return {400, "user_id/session_id required"};
    if (!Available()) return {500, "conversation store not initialized"};
    std::string error;
    if (!count || !repository_->Unread(user, session, count, &error))
        return {500, "get unread failed: " + error};
    return {};
}
ServiceStatus ConversationService::ListSingles(int64_t actor, int64_t user,
                                               std::vector<Session>* sessions) {
    if (actor != user) return {403, "user_id does not match bearer token"};
    if (user <= 0) return {400, "user_id required"};
    if (!Available()) return {500, "conversation store not initialized"};
    std::string error;
    if (!sessions || !repository_->Singles(user, sessions, &error))
        return {500, "list sessions failed: " + error};
    return {};
}
}  // namespace sparkpush
