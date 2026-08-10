#pragma once

#include <string>
#include <vector>

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

class MessageDao {
   public:
    explicit MessageDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 功能：插入一条消息记录
    // 参数：message 消息体；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool InsertMessage(const Message& message, std::string* err_msg);

    // 功能：按会话分页查询历史消息
    // 参数：session_id 会话 id；anchor_seq 游标（小于该 seq）；limit 数量；
    //       messages 输出结果；err_msg 错误信息
    // 返回：成功 true，失败 false
    bool ListMessages(const std::string& session_id, int64_t anchor_seq,
                      int limit, std::vector<Message>* messages,
                      std::string* err_msg);

    // 按 msg_seq 正序读取 after_seq 之后的消息，用于游标补偿和离线补推。
    bool ListMessagesAfter(const std::string& session_id, int64_t after_seq,
                           int limit, std::vector<Message>* messages,
                           std::string* err_msg);

    // 查询会话当前最大 msg_seq（无消息时 *max_seq=0）
    bool GetMaxMsgSeq(const std::string& session_id, int64_t* max_seq,
                      std::string* err_msg);

   private:
    bool IsSameStoredMessage(MYSQL* conn, const Message& message, bool* same,
                             std::string* err_msg);

    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
