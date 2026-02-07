#pragma once

#include "message_dao.h"
#include "redis_store.h"
#include "session_dao.h"
#include "user_session_state_dao.h"

#include <memory>
#include <string>
#include <vector>

namespace sparkpush {

// 统一封装“会话 + 消息 + 未读”数据访问
class ConversationStore {
 public:
  ConversationStore(SessionDao* session_dao,
                    MessageDao* message_dao,
                    UserSessionStateDao* state_dao,
                    RedisStore* redis_store);

  bool GetOrCreateSingleSession(int64_t user1,
                                int64_t user2,
                                Session* session,
                                std::string* err_msg);

  bool GetOrCreateRoomSession(int64_t room_id,
                              Session* session,
                              std::string* err_msg);

  bool AppendMessage(const Session& session,
                     int64_t sender_id,
                     const std::string& msg_type,
                     const std::string& content_json,
                     int64_t timestamp_ms,
                     const std::string& client_msg_id,
                     Message* message,
                     std::string* err_msg);

  bool GetHistory(const std::string& session_id,
                  int64_t anchor_seq,
                  int limit,
                  std::vector<Message>* messages,
                  std::string* err_msg);

  bool MarkRead(int64_t user_id,
                const std::string& session_id,
                int64_t read_seq,
                std::string* err_msg);

  bool GetUnread(int64_t user_id,
                 const std::string& session_id,
                 int64_t* unread,
                 std::string* err_msg);

  bool ListUserSingleSessions(int64_t user_id,
                              std::vector<Session>* sessions,
                              std::string* err_msg);

  bool GetSessionById(const std::string& session_id,
                      Session* session,
                      std::string* err_msg);

 private:
  SessionDao* session_dao_{nullptr};
  MessageDao* message_dao_{nullptr};
  UserSessionStateDao* state_dao_{nullptr};
  RedisStore* redis_store_{nullptr};
};

}  // namespace sparkpush


