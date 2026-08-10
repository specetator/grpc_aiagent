#pragma once

#include <string>
#include <vector>

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 负责读取/写入 `session` 表，提供会话的持久化能力
class SessionDao {
   public:
    explicit SessionDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 按 user1/user2（升序）获取或创建单聊会话
    // 功能：获取或创建单聊会话（user1/user2 升序）
    // 参数：user1/user2 用户；session 输出；err_msg 错误
    // 返回：成功 true，失败 false
    bool GetOrCreateSingleSession(int64_t user1, int64_t user2,
                                  Session* session, std::string* err_msg);

    // 按 room_id 获取或创建聊天室会话
    // 功能：获取或创建聊天室会话
    bool GetOrCreateRoomSession(int64_t room_id, Session* session,
                                std::string* err_msg);

    // 直接根据 session_id 查询
    // 功能：按 session_id 查询会话
    bool GetSessionById(const std::string& session_id, Session* session,
                        std::string* err_msg);

    // 将 last_msg_seq +1，并返回新的 seq
    // 功能：为会话分配下一个消息序列号
    bool AllocateMessageSeq(const std::string& session_id, int64_t* next_seq,
                            std::string* err_msg);

    // 将 session.last_msg_seq 单调推进到至少 last_seq，乱序异步落盘不会回退。
    bool UpdateLastMessageSeqAtLeast(const std::string& session_id,
                                     int64_t last_seq,
                                     std::string* err_msg);

    // 列出某个用户参与的所有单聊会话
    // 功能：列出用户参与的单聊会话
    bool ListUserSingleSessions(int64_t user_id, std::vector<Session>* sessions,
                                std::string* err_msg);

   private:
    bool EnsureSingleSession(MYSQL* conn, const std::string& session_id,
                             int64_t user1, int64_t user2,
                             std::string* err_msg);
    bool EnsureRoomSession(MYSQL* conn, const std::string& session_id,
                           int64_t room_id, std::string* err_msg);
    bool FillSessionFromStmt(MYSQL_STMT* stmt, Session* session,
                             std::string* err_msg);

    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
