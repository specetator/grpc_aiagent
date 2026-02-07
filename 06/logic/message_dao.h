#pragma once

#include "model.h"
#include "mysql_pool.h"

#include <string>
#include <vector>

namespace sparkpush {

class MessageDao {
 public:
  explicit MessageDao(MySqlConnectionPool* pool) : pool_(pool) {}

  bool InsertMessage(const Message& message, std::string* err_msg);

  bool ListMessages(const std::string& session_id,
                    int64_t anchor_seq,
                    int limit,
                    std::vector<Message>* messages,
                    std::string* err_msg);

 private:
  MySqlConnectionPool* pool_{nullptr};
};

}  // namespace sparkpush


