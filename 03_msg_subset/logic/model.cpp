#include "model.h"

#include <algorithm>
#include <chrono>

namespace sparkpush {

User InMemoryModel::RegisterOrGetUser(const std::string& account,
                                      const std::string& password) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& kv : users_) {
        if (kv.second.account == account) {
            return kv.second;
        }
    }
    User u;
    u.id = static_cast<int64_t>(users_.size() + 1);
    u.account = account;
    u.name = account;
    u.password_hash = password;
    users_[u.id] = u;
    return u;
}

User InMemoryModel::GetUserById(int64_t user_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(user_id);
    if (it != users_.end()) {
        return it->second;
    }
    return {};
}

Session InMemoryModel::GetOrCreateSingleSession(int64_t user1, int64_t user2) {
    if (user1 > user2) std::swap(user1, user2);
    std::string session_id = "s_" + std::to_string(user1) + "_" + std::to_string(user2);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = sessions_.find(session_id);
    if (it != sessions_.end()) return it->second;
    Session s;
    s.id = session_id;
    s.type = SessionType::kSingle;
    s.user1_id = user1;
    s.user2_id = user2;
    sessions_[s.id] = s;
    return s;
}

Message InMemoryModel::AppendMessage(const Session& session, int64_t sender_id,
                                     const std::string& msg_type,
                                     const std::string& content_json,
                                     int64_t now_ms,
                                     const std::string& client_msg_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto& msgs = messages_[session.id];
    int64_t seq = static_cast<int64_t>(msgs.size()) + 1;
    Message m;
    m.session_id = session.id;
    m.msg_seq = seq;
    m.msg_id = session.id + "-" + std::to_string(seq);
    m.sender_id = sender_id;
    m.msg_type = msg_type;
    m.content_json = content_json;
    m.timestamp_ms = now_ms;
    m.client_msg_id = client_msg_id;
    msgs.push_back(m);
    sessions_[session.id].last_msg_seq = seq;
    return m;
}

std::vector<Session> InMemoryModel::ListUserSingleSessions(int64_t user_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Session> res;
    for (const auto& kv : sessions_) {
        const Session& s = kv.second;
        if (s.type == SessionType::kSingle &&
            (s.user1_id == user_id || s.user2_id == user_id)) {
            res.push_back(s);
        }
    }
    return res;
}

void InMemoryModel::MarkRead(int64_t user_id, const std::string& session_id,
                             int64_t read_seq) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_session_state_[makeUserSessionKey(user_id, session_id)].read_seq =
        read_seq;
}

int64_t InMemoryModel::GetUnread(int64_t user_id,
                                 const std::string& session_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto sit = sessions_.find(session_id);
    if (sit == sessions_.end()) return 0;
    auto uit =
        user_session_state_.find(makeUserSessionKey(user_id, session_id));
    int64_t read_seq = (uit == user_session_state_.end())
                           ? 0
                           : uit->second.read_seq;
    return sit->second.last_msg_seq > read_seq
               ? (sit->second.last_msg_seq - read_seq)
               : 0;
}

std::string InMemoryModel::makeUserSessionKey(
    int64_t user_id, const std::string& session_id) const {
    return std::to_string(user_id) + ":" + session_id;
}

}  // namespace sparkpush


