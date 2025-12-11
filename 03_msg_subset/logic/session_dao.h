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

    // 直接根据 session_id 查询
    // 功能：按 session_id 查询会话
    bool GetSessionById(const std::string& session_id, Session* session,
                        std::string* err_msg);

    // 功能：更新会话的 last_msg_seq（基于 msg_id 的序号部分）
    bool UpdateLastMsgSeq(const std::string& session_id, int64_t msg_id_seq,
                          std::string* err_msg);

    // 列出某个用户参与的所有单聊会话
    // 功能：列出用户参与的单聊会话
    bool ListUserSingleSessions(int64_t user_id, std::vector<Session>* sessions,
                                std::string* err_msg);

   private:
    bool EnsureSingleSession(MYSQL* conn, const std::string& session_id,
                             int64_t user1, int64_t user2,
                             std::string* err_msg);
    bool FillSessionFromStmt(MYSQL_STMT* stmt, Session* session,
                             std::string* err_msg);

    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
