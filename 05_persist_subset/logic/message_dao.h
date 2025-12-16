#pragma once

#include <string>
#include <vector>

#include "mysql_pool.h"

namespace sparkpush {

// MySQL 持久化：消息表访问
struct PersistMessage {
    std::string msg_id;
    std::string session_id;
    int64_t msg_seq{0};
    int64_t sender_id{0};
    int64_t timestamp_ms{0};
    std::string msg_type;
    std::string content_json;
    std::string client_msg_id;
};

class MessageDao {
   public:
    explicit MessageDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 插入一条消息（幂等性由表的唯一键保障）
    bool Insert(const PersistMessage& message, std::string* err_msg);

    // 按会话分页查询历史消息（按 msg_seq 倒序，anchor_seq>0 时取小于 anchor
    // 的）
    bool List(const std::string& session_id, int64_t anchor_seq, int limit,
              std::vector<PersistMessage>* messages, std::string* err_msg);

   private:
    MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush
