#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "mysql_pool.h"

namespace sparkpush {

struct AuditLogRecord {
    int64_t id{0};
    int64_t actor_user_id{0};
    int64_t target_user_id{0};
    std::string action;
    std::string reason;
    std::string metadata_json;
    std::string created_at;
};

// 用户中心审计日志 DAO。日志只追加，不提供业务层 UPDATE/DELETE。
class AuditLogDao {
   public:
    explicit AuditLogDao(MySqlConnectionPool* pool) : pool_(pool) {}

    bool EnsureSchema(std::string* err_msg);

    bool Append(int64_t actor_user_id, int64_t target_user_id,
                const std::string& action, const std::string& reason,
                const std::string& metadata_json, std::string* err_msg);

    bool List(int offset, int limit, int64_t target_user_id,
              std::vector<AuditLogRecord>* records, int* total,
              std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
