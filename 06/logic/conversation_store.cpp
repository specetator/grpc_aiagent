#include "conversation_store.h"

#include "logging.h"

#include <algorithm>

namespace sparkpush {

ConversationStore::ConversationStore(SessionDao* session_dao,
                                     MessageDao* message_dao,
                                     UserSessionStateDao* state_dao,
                                     RedisStore* redis_store)
    : session_dao_(session_dao),
      message_dao_(message_dao),
      state_dao_(state_dao),
      redis_store_(redis_store) {}

bool ConversationStore::GetOrCreateSingleSession(int64_t user1,
                                                 int64_t user2,
                                                 Session* session,
                                                 std::string* err_msg) {
  if (!session_dao_) {
    if (err_msg) *err_msg = "session dao not initialized";
    return false;
  }
  return session_dao_->GetOrCreateSingleSession(user1, user2, session, err_msg);
}

bool ConversationStore::GetOrCreateRoomSession(int64_t room_id,
                                               Session* session,
                                               std::string* err_msg) {
  if (!session_dao_) {
    if (err_msg) *err_msg = "session dao not initialized";
    return false;
  }
  return session_dao_->GetOrCreateRoomSession(room_id, session, err_msg);
}

bool ConversationStore::AppendMessage(const Session& session,
                                      int64_t sender_id,
                                      const std::string& msg_type,
                                      const std::string& content_json,
                                      int64_t timestamp_ms,
                                      const std::string& client_msg_id,
                                      Message* message,
                                      std::string* err_msg) {
  if (!session_dao_ || !message_dao_) {
    if (err_msg) *err_msg = "conversation store not initialized";
    return false;
  }
  int64_t seq = 0;
  if (!session_dao_->AllocateMessageSeq(session.id, &seq, err_msg)) {
    return false;
  }
  Message msg;
  msg.session_id = session.id;
  msg.msg_seq = seq;
  msg.sender_id = sender_id;
  msg.msg_type = msg_type;
  msg.content_json = content_json;
  msg.timestamp_ms = timestamp_ms;
  msg.client_msg_id = client_msg_id;
  msg.msg_id = msg.session_id + "-" + std::to_string(seq);

  if (!message_dao_->InsertMessage(msg, err_msg)) {
    return false;
  }

  if (redis_store_) {
    if (!redis_store_->SetSessionLastSeq(session.id, seq)) {
      LogError("SetSessionLastSeq failed for session " + session.id);
    }
  }

  if (message) {
    *message = msg;
  }
  return true;
}

bool ConversationStore::GetHistory(const std::string& session_id,
                                   int64_t anchor_seq,
                                   int limit,
                                   std::vector<Message>* messages,
                                   std::string* err_msg) {
  if (!message_dao_) {
    if (err_msg) *err_msg = "message dao not initialized";
    return false;
  }
  return message_dao_->ListMessages(session_id, anchor_seq, limit, messages,
                                    err_msg);
}

bool ConversationStore::MarkRead(int64_t user_id,
                                 const std::string& session_id,
                                 int64_t read_seq,
                                 std::string* err_msg) {
  if (!state_dao_) {
    if (err_msg) *err_msg = "state dao not initialized";
    return false;
  }
  if (!state_dao_->UpsertReadSeq(user_id, session_id, read_seq, err_msg)) {
    return false;
  }
  if (redis_store_) {
    if (!redis_store_->SetUserReadSeq(user_id, session_id, read_seq)) {
      LogError("SetUserReadSeq failed for user " + std::to_string(user_id));
    }
  }
  return true;
}

bool ConversationStore::GetUnread(int64_t user_id,
                                  const std::string& session_id,
                                  int64_t* unread,
                                  std::string* err_msg) {
  if (!state_dao_ || !session_dao_) {
    if (err_msg) *err_msg = "store not initialized";
    return false;
  }
  if (!unread) {
    if (err_msg) *err_msg = "unread pointer is null";
    return false;
  }

  int64_t read_seq = 0;
  bool read_seq_ok = false;
  if (redis_store_) {
    if (redis_store_->GetUserReadSeq(user_id, session_id, &read_seq)) {
      read_seq_ok = true;
    }
  }
  if (!read_seq_ok) {
    if (!state_dao_->GetReadSeq(user_id, session_id, &read_seq, err_msg)) {
      return false;
    }
    if (redis_store_) {
      redis_store_->SetUserReadSeq(user_id, session_id, read_seq);
    }
  }

  int64_t last_seq = 0;
  bool last_seq_ok = false;
  if (redis_store_) {
    if (redis_store_->GetSessionLastSeq(session_id, &last_seq)) {
      last_seq_ok = true;
    }
  }
  if (!last_seq_ok) {
    Session session;
    if (!session_dao_->GetSessionById(session_id, &session, err_msg)) {
      return false;
    }
    last_seq = session.last_msg_seq;
    if (redis_store_) {
      redis_store_->SetSessionLastSeq(session_id, last_seq);
    }
  }

  *unread = last_seq > read_seq ? (last_seq - read_seq) : 0;
  return true;
}

bool ConversationStore::ListUserSingleSessions(int64_t user_id,
                                               std::vector<Session>* sessions,
                                               std::string* err_msg) {
  if (!session_dao_) {
    if (err_msg) *err_msg = "session dao not initialized";
    return false;
  }
  return session_dao_->ListUserSingleSessions(user_id, sessions, err_msg);
}

bool ConversationStore::GetSessionById(const std::string& session_id,
                                       Session* session,
                                       std::string* err_msg) {
  if (!session_dao_) {
    if (err_msg) *err_msg = "session dao not initialized";
    return false;
  }
  return session_dao_->GetSessionById(session_id, session, err_msg);
}

}  // namespace sparkpush


