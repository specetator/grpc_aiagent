#pragma once

#include <array>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "message_dao.h"
#include "redis_store.h"
#include "session_dao.h"
#include "user_session_state_dao.h"

namespace sparkpush {

// 统一封装“会话 + 消息 + 未读”数据访问
class ConversationStore {
   public:
    // 功能：构造封装对象，注入 Session/Message/State/Redis 依赖
    // 参数：对应的 DAO/Store 指针，均为外部生命周期管理
    // 返回：无
    ConversationStore(SessionDao* session_dao, MessageDao* message_dao,
                      UserSessionStateDao* state_dao, RedisStore* redis_store);

    // 功能：获取或创建单聊会话，确保数据库存在对应 session
    // 参数：user1/user2 单聊双方；session 输出会话信息；err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool GetOrCreateSingleSession(int64_t user1, int64_t user2,
                                  Session* session, std::string* err_msg);

    // 功能：获取或创建聊天室会话，按 room_id 生成 session
    // 参数：room_id 聊天室 id；session 输出会话；err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool GetOrCreateRoomSession(int64_t room_id, Session* session,
                                std::string* err_msg);

    // 功能：追加一条消息，分配顺序号并写入存储
    // 参数：session 目标会话；sender_id 发送者；msg_type/ content_json
    // 消息内容；
    //       timestamp_ms 服务器时间；client_msg_id 客户端 id；message
    //       输出写入结果； err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool AppendMessage(const Session& session, int64_t sender_id,
                       const std::string& msg_type,
                       const std::string& content_json, int64_t timestamp_ms,
                       const std::string& client_msg_id, Message* message,
                       std::string* err_msg);

    // 热路径：仅 Redis INCR 分配序号并构造 Message，不写 MySQL
    bool AppendMessageHotPath(const std::string& session_id, int64_t sender_id,
                              const std::string& msg_type,
                              const std::string& content_json,
                              int64_t timestamp_ms,
                              const std::string& client_msg_id, Message* message,
                              bool* is_new, std::string* err_msg);

    // 异步落盘：确保 session 行存在并 INSERT message（使用已分配的 msg_seq）
    // scene: "single" | "chatroom"；single 时用 user1/user2，chatroom 用 room_id
    bool PersistMessage(const Message& message, const std::string& scene,
                        int64_t user1, int64_t user2, int64_t room_id,
                        std::string* err_msg);

    // 功能：查询会话历史消息
    // 参数：session_id 会话；anchor_seq 游标（不包含）；limit 条数；messages
    // 输出结果；
    //       err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool GetHistory(const std::string& session_id, int64_t anchor_seq,
                    int limit, std::vector<Message>* messages,
                    std::string* err_msg);

    bool GetMessagesAfter(const std::string& session_id, int64_t after_seq,
                          int limit, std::vector<Message>* messages,
                          std::string* err_msg);

    // 功能：标记用户已读序列并写回 Redis/MySQL
    // 参数：user_id 用户；session_id 会话；read_seq 已读序号；err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool MarkRead(int64_t user_id, const std::string& session_id,
                  int64_t read_seq, std::string* err_msg);

    // 功能：计算某用户在指定会话的未读数
    // 参数：user_id 用户；session_id 会话；unread 输出未读数；err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool GetUnread(int64_t user_id, const std::string& session_id,
                   int64_t* unread, std::string* err_msg);

    bool MarkDelivered(int64_t user_id, const std::string& session_id,
                       int64_t delivered_seq, std::string* err_msg);

    bool GetDeliveredSeq(int64_t user_id, const std::string& session_id,
                         int64_t* delivered_seq, std::string* err_msg);

    // 功能：列出用户参与的所有单聊会话
    // 参数：user_id 用户；sessions 输出会话列表；err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool ListUserSingleSessions(int64_t user_id, std::vector<Session>* sessions,
                                std::string* err_msg);

    // 功能：按 session_id 查询会话详情
    // 参数：session_id 会话；session 输出详情；err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool GetSessionById(const std::string& session_id, Session* session,
                        std::string* err_msg);

   private:
    SessionDao* session_dao_{nullptr};
    MessageDao* message_dao_{nullptr};
    UserSessionStateDao* state_dao_{nullptr};
    RedisStore* redis_store_{nullptr};
    // 不同会话并行校准 MySQL floor；同一会话仍由固定 shard 串行初始化。
    static constexpr size_t kSeqSeedShardCount = 64;
    std::array<std::mutex, kSeqSeedShardCount> seq_seed_mutexes_;
    std::array<std::unordered_set<std::string>, kSeqSeedShardCount>
        seq_seeded_sessions_;
};

}  // namespace sparkpush
