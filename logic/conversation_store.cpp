#include "conversation_store.h"

#include <algorithm>

#include "logging.h"

namespace sparkpush {

// 功能：构造封装对象，保存各类 DAO/Redis 句柄
// 参数：session_dao 会话 DAO；message_dao 消息 DAO；state_dao 已读
// DAO；redis_store Redis 封装
ConversationStore::ConversationStore(SessionDao* session_dao,
                                     MessageDao* message_dao,
                                     UserSessionStateDao* state_dao,
                                     RedisStore* redis_store)
    : session_dao_(session_dao),
      message_dao_(message_dao),
      state_dao_(state_dao),
      redis_store_(redis_store) {}

// 功能：获取或创建单聊会话，直接委托 SessionDao
// 返回：true 表示成功，false 写入 err_msg
bool ConversationStore::GetOrCreateSingleSession(int64_t user1, int64_t user2,
                                                 Session* session,
                                                 std::string* err_msg) {
    if (!session_dao_) {
        if (err_msg) *err_msg = "session dao not initialized";
        return false;
    }
    return session_dao_->GetOrCreateSingleSession(user1, user2, session,
                                                  err_msg);
}

// 功能：获取或创建聊天室会话
bool ConversationStore::GetOrCreateRoomSession(int64_t room_id,
                                               Session* session,
                                               std::string* err_msg) {
    if (!session_dao_) {
        if (err_msg) *err_msg = "session dao not initialized";
        return false;
    }
    return session_dao_->GetOrCreateRoomSession(room_id, session, err_msg);
}

// 功能：为会话追加消息，分配 seq 并写入 MySQL/Redis
bool ConversationStore::AppendMessage(const Session& session, int64_t sender_id,
                                      const std::string& msg_type,
                                      const std::string& content_json,
                                      int64_t timestamp_ms,
                                      const std::string& client_msg_id,
                                      Message* message, std::string* err_msg) {
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
        // 将最新 seq 写入 Redis，便于未读计算
        if (!redis_store_->SetSessionLastSeq(session.id, seq)) {
            LogError("SetSessionLastSeq failed for session " + session.id);
        }
    }

    if (message) {
        *message = msg;
    }
    return true;
}

// 功能：读取会话历史消息
bool ConversationStore::GetHistory(const std::string& session_id,
                                   int64_t anchor_seq, int limit,
                                   std::vector<Message>* messages,
                                   std::string* err_msg) {
    if (!message_dao_) {
        if (err_msg) *err_msg = "message dao not initialized";
        return false;
    }
    return message_dao_->ListMessages(session_id, anchor_seq, limit, messages,
                                      err_msg);
}

// 功能：标记用户已读序列并缓存到 Redis
bool ConversationStore::MarkRead(int64_t user_id, const std::string& session_id,
                                 int64_t read_seq, std::string* err_msg) {
    if (!state_dao_) {
        if (err_msg) *err_msg = "state dao not initialized";
        return false;
    }
    if (!state_dao_->UpsertReadSeq(user_id, session_id, read_seq, err_msg)) {
        return false;
    }
    if (redis_store_) {
        // 写入缓存，快速读取未读
        if (!redis_store_->SetUserReadSeq(user_id, session_id, read_seq)) {
            LogError("SetUserReadSeq failed for user " +
                     std::to_string(user_id));
        }
    }
    return true;
}

// 功能：计算用户未读数，优先读取 Redis 缓存
bool ConversationStore::GetUnread(int64_t user_id,
                                  const std::string& session_id,
                                  int64_t* unread, std::string* err_msg) {
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
        // 缓存未命中，回源 MySQL
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
        // 回源 session 表，确保 last_seq 最新
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

// 功能：列出用户参与的单聊会话
bool ConversationStore::ListUserSingleSessions(int64_t user_id,
                                               std::vector<Session>* sessions,
                                               std::string* err_msg) {
    if (!session_dao_) {
        if (err_msg) *err_msg = "session dao not initialized";
        return false;
    }
    return session_dao_->ListUserSingleSessions(user_id, sessions, err_msg);
}

// 功能：按 session_id 查询会话
bool ConversationStore::GetSessionById(const std::string& session_id,
                                       Session* session, std::string* err_msg) {
    if (!session_dao_) {
        if (err_msg) *err_msg = "session dao not initialized";
        return false;
    }
    return session_dao_->GetSessionById(session_id, session, err_msg);
}

}  // namespace sparkpush
