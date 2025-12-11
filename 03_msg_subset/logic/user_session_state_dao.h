#pragma once

#include <string>

#include "mysql_pool.h"

namespace sparkpush {

// DAO：管理 user_session_state 表（用户已读位置）
// 用于未读计数：unread = session.last_msg_seq - user_read_seq
class UserSessionStateDao {
   public:
    explicit UserSessionStateDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 更新或插入用户已读位置（只有当新的 read_seq 更大时才更新）
    bool UpsertReadSeq(int64_t user_id, const std::string& session_id,
                       int64_t read_seq, std::string* err_msg);
    
    // 查询用户已读位置
    bool GetReadSeq(int64_t user_id, const std::string& session_id,
                    int64_t* read_seq, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush


