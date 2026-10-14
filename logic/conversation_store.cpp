#include "conversation_store.h"

#include <algorithm>
#include <chrono>
#include <functional>
#include <thread>

#include "logging.h"

namespace sparkpush {

// 功能：构造封装对象，保存各类 DAO/Redis 句柄
// 参数：session_dao 会话 DAO；message_dao 消息 DAO；state_dao 已读
// DAO；redis_store Redis 封装
ConversationStore::ConversationStore(SessionDao* session_dao,
                                     MessageDao* message_dao,
                                     UserSessionStateDao* state_dao,
                                     RedisStore* redis_store,
                                     MessageReservationRepository* reservations)
    : session_dao_(session_dao),
      message_dao_(message_dao),
      state_dao_(state_dao),
      redis_store_(redis_store),
      reservations_(reservations ? reservations : (message_dao ? &message_dao->reservations() : nullptr)) {}

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
    // 顺序固定：持久预约序号 -> INSERT message -> 标 Persisted -> 推进 last_msg_seq。
    // 任一步失败都直接返回，**不回滚已经写下的预约**。
    // 同一个 client_msg_id 重试会命中旧预约，序号不会再涨。
    // 若在这里删掉预约，重试会分配一个新序号，而客户端可能已经拿着旧 msg_id。
    Message msg;
    bool fresh = false;
    if (!AppendMessageHotPath(session.id, sender_id, msg_type, content_json, timestamp_ms,
                              client_msg_id, &msg, &fresh, err_msg)) return false;

    if (!message_dao_->InsertMessage(msg, err_msg) ||
        !message_dao_->reservations().Persisted(msg, err_msg) ||
        !session_dao_->UpdateLastMessageSeqAtLeast(session.id, msg.msg_seq, err_msg)) {
        return false;
    }

    if (redis_store_) {
        // Redis 是未读计算的缓存。写失败只记日志，GetUnread miss 时会回源 session 表。
        // 不能因为缓存失败把已经落库的消息报成失败，否则调用方重试会以为没写上。
        if (!redis_store_->SetSessionLastSeq(session.id, msg.msg_seq)) {
            LOG_ERROR << "SetSessionLastSeq failed for session " << session.id;
        }
    }

    if (message) {
        *message = msg;
    }
    return true;
}

// The allocation authority is durable SQL; Redis is a cache/routing service.
// 译：分配权在持久 SQL；Redis 只是缓存和路由。本方法不写 message 行。
bool ConversationStore::AppendMessageHotPath(
    const std::string& session_id, int64_t sender_id,
    const std::string& msg_type, const std::string& content_json,
    int64_t timestamp_ms, const std::string& client_msg_id, Message* message,
    bool* is_new, std::string* err_msg) {
    if (!reservations_ || !message || !is_new) {
        if (err_msg) *err_msg = "message reservations unavailable";
        return false;
    }
    Message proposed;
    proposed.session_id = session_id;
    proposed.sender_id = sender_id;
    proposed.msg_type = msg_type;
    proposed.content_json = content_json;
    proposed.timestamp_ms = timestamp_ms;
    proposed.client_msg_id = client_msg_id;
    return reservations_->Reserve(proposed, message, is_new, err_msg);
}

bool ConversationStore::PrepareOutbox(const Message& message, const std::string& payload, std::string* error) {
    return reservations_ && reservations_->PrepareOutbox(message, payload, error);
}
bool ConversationStore::Published(const Message& message, std::string* error) {
    return reservations_ && reservations_->Published(message, error);
}
bool ConversationStore::SafeSyncPrefix(const std::string& session, int64_t* seq, std::string* error) {
    return message_dao_ && message_dao_->SafeSyncPrefix(session, seq, error);
}

// 异步历史落盘：补 session + 写 message（序号已由 SQL 预留）
bool ConversationStore::PersistMessage(const Message& message,
                                       const std::string& scene, int64_t user1,
                                       int64_t user2, int64_t room_id,
                                       std::string* err_msg) {
    if (!session_dao_ || !message_dao_) {
        if (err_msg) *err_msg = "conversation store not initialized";
        return false;
    }
    // Job 消费 persist_message 时调用。序号在更早的预约里已经定了，
    // 这里只补 session 行、INSERT、标 Persisted、推进 last_msg_seq。
    // 最多 4 次，退避 50/100/200ms。序号冲突不重试：内容与已有序号对不上，
    // 再试结果一样，继续睡只会拖住消费线程。锁等待、瞬时断连才退避。
    // scene 不是 single/chatroom 时按 message.session_id 取已有会话，不新建。
    constexpr int kMaxAttempts = 4;
    std::string last_error;
    for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
        Session session;
        std::string err;
        bool session_ok = false;
        if (scene == "single") {
            session_ok = session_dao_->GetOrCreateSingleSession(
                user1, user2, &session, &err);
        } else if (scene == "chatroom") {
            session_ok = session_dao_->GetOrCreateRoomSession(room_id, &session,
                                                               &err);
        } else {
            session_ok = session_dao_->GetSessionById(message.session_id,
                                                       &session, &err);
        }

        if (session_ok && message_dao_->InsertMessage(message, &err) &&
            message_dao_->reservations().Persisted(message, &err) &&
            session_dao_->UpdateLastMessageSeqAtLeast(
                message.session_id, message.msg_seq, &err)) {
            return true;
        }

        last_error = err.empty() ? "unknown persistence error" : err;
        // rfind(..., 0) 是「前缀匹配」。冲突文本由 message_dao 产生，改措辞会让这里重新重试。
        if (last_error.rfind("message sequence collision", 0) == 0) break;
        if (attempt + 1 < kMaxAttempts) {
            const int backoff_ms = 50 * (1 << attempt);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(backoff_ms));
        }
    }
    if (err_msg) *err_msg = last_error;
    return false;
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

bool ConversationStore::GetMessagesAfter(const std::string& session_id,
                                         int64_t after_seq, int limit,
                                         std::vector<Message>* messages,
                                         std::string* err_msg) {
    if (!message_dao_) {
        if (err_msg) *err_msg = "message dao not initialized";
        return false;
    }
    return message_dao_->ListMessagesAfter(session_id, after_seq, limit,
                                           messages, err_msg);
}

// 功能：标记用户已读序列并缓存到 Redis
bool ConversationStore::MarkRead(int64_t user_id, const std::string& session_id,
                                 int64_t read_seq, std::string* err_msg) {
    if (!state_dao_) {
        if (err_msg) *err_msg = "state dao not initialized";
        return false;
    }
    // MySQL 成功才算成功。先写库再写缓存：若先写 Redis 而 MySQL 失败，
    // 缓存会超前于库，未读会被算少，直到有人把缓存覆盖掉。
    if (!state_dao_->UpsertReadSeq(user_id, session_id, read_seq, err_msg)) {
        return false;
    }
    if (redis_store_) {
        // 缓存写失败只记日志。下次 GetUnread miss 会从 MySQL 回源。
        if (!redis_store_->SetUserReadSeq(user_id, session_id, read_seq)) {
            LOG_ERROR << "SetUserReadSeq failed for user "
                      << std::to_string(user_id);
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

    // 未读 = last_seq - read_seq。两个数都是 Redis 优先，miss 回源并回填。
    // 回填失败不影响本次结果，权威值已经拿到。
    // 这不是无空洞计数：中间缺的序号也算未读。送达水位才做无空洞，
    // 见 UserSessionStateDao::AdvanceDelivered。这个数字只用于角标。
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

bool ConversationStore::MarkDelivered(int64_t user_id,
                                      const std::string& session_id,
                                      int64_t delivered_seq,
                                      std::string* err_msg) {
    if (!state_dao_) {
        if (err_msg) *err_msg = "state dao not initialized";
        return false;
    }
    return state_dao_->UpsertDeliveredSeq(user_id, session_id, delivered_seq,
                                          err_msg);
}

bool ConversationStore::GetDeliveredSeq(int64_t user_id,
                                        const std::string& session_id,
                                        int64_t* delivered_seq,
                                        std::string* err_msg) {
    if (!state_dao_) {
        if (err_msg) *err_msg = "state dao not initialized";
        return false;
    }
    return state_dao_->GetDeliveredSeq(user_id, session_id, delivered_seq,
                                       err_msg);
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
