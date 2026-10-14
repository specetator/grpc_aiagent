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

// 统一封装“会话 + 消息 + 未读”数据访问。
//
// 本类是 DAO 之上的一层**编排**，不包含业务规则（那些在 services/ 里）。
// 它存在的价值是把四个存储（SessionDao / MessageDao / UserSessionStateDao /
// RedisStore）的协作顺序固定下来，避开每个调用方自己拼。
//
// 两个 Append 方法的分工是本文件最容易看错的地方，先记住：
//   AppendMessageHotPath  **只分配序号**（持久预约），不写 message 行
//   AppendMessage         HotPath + 同步写 message + 推进游标 + 刷 Redis
// 名字里的 "HotPath" 有误导性：它并不是「走 Redis 的快路径」，
// 而是走 MySQL 事务的持久分配器。真正的 Redis 快路径在
// RedisStore::AllocateSessionMsgSeq。
class ConversationStore {
   public:
    // 功能：构造封装对象，注入 Session/Message/State/Redis 依赖
    // 参数：对应的 DAO/Store 指针，均为外部生命周期管理
    // 返回：无
    // reservations 为空时自动用 message_dao->reservations()。
    // 留这个参数只是为了让单测能注入内存假实现（见
    // tests/message_acceptance_test.cpp），生产路径不会传。
    ConversationStore(SessionDao* session_dao, MessageDao* message_dao,
                      UserSessionStateDao* state_dao, RedisStore* redis_store,
                      MessageReservationRepository* reservations = nullptr);

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
    //
    // 这是**同步落库**路径（遗留 HTTP API 与弹幕在用）：
    // 分配序号 -> INSERT message -> 标 Persisted -> 推进 last_msg_seq -> 刷 Redis。
    // 与 WS 路径的关键区别：WS 走 outbox（先发 Kafka，由 Job 异步落库），
    // 本方法直接在调用线程里写完。因此它慢，但返回时消息已在库里。
    // 新增实时消息路径不要用它：同步写库会把 MySQL 延迟直接加到发送 RTT 上。
    bool AppendMessage(const Session& session, int64_t sender_id,
                       const std::string& msg_type,
                       const std::string& content_json, int64_t timestamp_ms,
                       const std::string& client_msg_id, Message* message,
                       std::string* err_msg);

    // Durable reservation: canonical payload and sequence survive cache loss.
    // 译：持久预约——规范载荷与序号在缓存丢失后仍然存在。
    //
    // **只分配序号，不写 message 行**。返回的 message 已带上权威的
    // msg_seq/msg_id，is_new=false 表示命中同 client_msg_id 的旧预约（幂等重放）。
    //
    // 名字里的 "HotPath" 很容易误解：它不是 Redis 快路径，而是走
    // MySqlMessageReservations::Reserve 的**单事务持久分配**。
    // The allocation authority is durable SQL; Redis is a cache/routing service.
    // 之所以把分配权放在 SQL 而不是 Redis，是因为 Redis 丢数据时
    // 序号必须能重建；若序号只存在于 Redis，丢一次就会造成消息撞主键。
    bool AppendMessageHotPath(const std::string& session_id, int64_t sender_id,
                              const std::string& msg_type,
                              const std::string& content_json,
                              int64_t timestamp_ms,
                              const std::string& client_msg_id, Message* message,
                              bool* is_new, std::string* err_msg);

    // 异步落盘：确保 session 行存在并 INSERT message（使用已分配的 msg_seq）
    // scene: "single" | "chatroom"；single 时用 user1/user2，chatroom 用 room_id
    // 由 Job 消费 persist_message 后调用。幂等靠 InsertMessage 里的
    // 唯一索引 + 内容比对（见 message_dao.h），因此 Kafka 重放安全。
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
    // 下面三个是 outbox 状态机的转发（详见 message_reservation.h）：
    //   PrepareOutbox  写 payload，stage 0 -> 1
    //   Published      Kafka 已确认，stage -> 2
    //   SafeSyncPrefix 算安全同步水位（客户端只能同步到这里）
    // 它们只是转发到 reservations_ / message_dao_，存在的价值是让
    // 调用方只需要持有 ConversationStore 一个依赖。
    bool PrepareOutbox(const Message&, const std::string& payload, std::string* error);
    bool Published(const Message&, std::string* error);
    bool SafeSyncPrefix(const std::string& session, int64_t* seq, std::string* error);

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

    // 登记送达水位。注意这是**无空洞水位**而不是「收到的最大 seq」，
    // 具体算法见 UserSessionStateDao::AdvanceDelivered。
    bool MarkDelivered(int64_t user_id, const std::string& session_id,
                       int64_t delivered_seq, std::string* err_msg);

    // 读送达水位。与 MarkDelivered 走同一个重算逻辑，因此**不是只读**：
    // 它也会回写水位并清理已被覆盖的回执行。
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
    // 全部是**不拥有**的借用指针，生命周期由 logic/main.cpp 管。
    // 构造顺序上它们必须先于本对象创建、后于本对象销毁。
    SessionDao* session_dao_{nullptr};
    MessageDao* message_dao_{nullptr};
    UserSessionStateDao* state_dao_{nullptr};
    RedisStore* redis_store_{nullptr};
    // 为空时构造函数会回退到 message_dao_->reservations()。
    // 可注入只是为了单测能用内存假实现跑完整验收流程。
    MessageReservationRepository* reservations_{nullptr};
};

}  // namespace sparkpush
