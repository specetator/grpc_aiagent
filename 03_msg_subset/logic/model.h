#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace sparkpush {

struct User {
    int64_t id{0};
    std::string account;
    std::string name;
    std::string password_hash;
};

enum class SessionType {
    kSingle = 0,
};

struct Session {
    std::string id;  // session_id
    SessionType type{SessionType::kSingle};
    int64_t user1_id{0};
    int64_t user2_id{0};
    int64_t last_msg_seq{0};
};

struct Message {
    std::string msg_id;
    std::string session_id;
    int64_t msg_seq{0};
    int64_t sender_id{0};
    int64_t timestamp_ms{0};
    std::string msg_type;
    std::string content_json;
    std::string client_msg_id;
};

struct UserSessionState {
    int64_t read_seq{0};
};

// 简单内存模型（测试用），真实逻辑使用 DAO/Store
class InMemoryModel {
   public:
    InMemoryModel() = default;

    User RegisterOrGetUser(const std::string& account,
                           const std::string& password);
    User GetUserById(int64_t user_id);

    Session GetOrCreateSingleSession(int64_t user1, int64_t user2);
    Message AppendMessage(const Session& session, int64_t sender_id,
                          const std::string& msg_type,
                          const std::string& content_json, int64_t now_ms,
                          const std::string& client_msg_id);

    std::vector<Session> ListUserSingleSessions(int64_t user_id);

    void MarkRead(int64_t user_id, const std::string& session_id,
                  int64_t read_seq);
    int64_t GetUnread(int64_t user_id, const std::string& session_id);

   private:
    std::string makeUserSessionKey(int64_t user_id,
                                   const std::string& session_id) const;

    std::mutex mutex_;
    std::unordered_map<int64_t, User> users_;
    std::unordered_map<std::string, Session> sessions_;
    std::unordered_map<std::string, std::vector<Message>> messages_;
    std::unordered_map<std::string, UserSessionState> user_session_state_;
};

}  // namespace sparkpush


