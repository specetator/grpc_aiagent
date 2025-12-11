#pragma once

#include <memory>
#include <string>
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

    // 功能：追加一条消息并写入存储
    // 参数：session 目标会话；msg_id 服务端生成的消息ID；msg_seq 客户端序列号；
    //       sender_id 发送者；msg_type/ content_json 消息内容；
    //       timestamp_ms 服务器时间；client_msg_id 客户端 id；message 输出写入结果； 
    //       err_msg 记录错误
    // 返回：成功返回 true，失败返回 false
    bool AppendMessage(const Session& session, const std::string& msg_id,
                       int64_t msg_seq, int64_t sender_id,
                       const std::string& msg_type,
                       const std::string& content_json, int64_t timestamp_ms,
                       const std::string& client_msg_id, Message* message,
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
};

}  // namespace sparkpush
