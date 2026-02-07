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
  kGroup = 1,
  kChatroom = 2,
};

struct Session {
  std::string id;   // session_id
  SessionType type{SessionType::kSingle};
  int64_t user1_id{0};
  int64_t user2_id{0};
  int64_t group_id{0};
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

// 简单内存模型，后续可以替换为 MySQL/Redis 实现
class InMemoryModel {
 public:
  InMemoryModel() = default;

  // 用户相关
  User RegisterOrGetUser(const std::string& account, const std::string& password);
  User GetUserById(int64_t user_id);

  // 会话与消息
  Session GetOrCreateSingleSession(int64_t user1, int64_t user2);
  Session GetOrCreateRoomSession(int64_t room_id);
  Message AppendMessage(const Session& session,
                        int64_t sender_id,
                        const std::string& msg_type,
                        const std::string& content_json,
                        int64_t now_ms,
                        const std::string& client_msg_id);

  std::vector<Message> GetHistory(const std::string& session_id,
                                  int64_t anchor_seq,
                                  int limit);

  // 会话列表（给 Web / App 拉取最近会话列表使用，简单内存实现）
  // 一对一会话列表：当前用户参与的所有单聊会话
  std::vector<Session> ListUserSingleSessions(int64_t user_id);
  // 聊天室会话列表：当前用户加入过的所有聊天室会话
  std::vector<Session> ListUserRoomSessions(int64_t user_id);

  // 聊天室成员管理（最小实现）
  void JoinRoom(int64_t room_id, int64_t user_id);
  void LeaveRoom(int64_t room_id, int64_t user_id);
  std::vector<int64_t> GetRoomMembers(int64_t room_id);

  // 未读与已读
  void MarkRead(int64_t user_id, const std::string& session_id, int64_t read_seq);
  int64_t GetUnread(int64_t user_id, const std::string& session_id);

 private:
  std::string makeUserSessionKey(int64_t user_id, const std::string& session_id) const;

  std::mutex mutex_;
  std::unordered_map<int64_t, User> users_;
  std::unordered_map<std::string, Session> sessions_;
  std::unordered_map<std::string, std::vector<Message>> messages_;  // session_id -> msgs
  std::unordered_map<std::string, UserSessionState> user_session_state_;
  std::unordered_map<int64_t, std::vector<int64_t>> room_members_;  // room_id -> user_ids
};

}  // namespace sparkpush


