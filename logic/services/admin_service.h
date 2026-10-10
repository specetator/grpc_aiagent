#pragma once
#include <nlohmann/json.hpp>

#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
class UserDao;
class RedisStore;
class AuditLogDao;
class AdminService {
   public:
    AdminService(UserDao* users, RedisStore* redis, AuditLogDao* audit)
        : users_(users), redis_(redis), audit_(audit) {}
    bool UsersAvailable() const { return users_ != nullptr; }
    bool CenterAvailable() const { return users_ && redis_; }
    bool AuditAvailable() const { return audit_ != nullptr; }
    ServiceStatus ListUsers(int64_t actor, int offset, int limit, int status,
                            std::vector<User>* users, int* total);
    ServiceStatus SetStatus(int64_t actor, int64_t user, int status,
                            const std::string& reason, nlohmann::json* result);
    ServiceStatus UpdateName(int64_t actor, int64_t user, const std::string& name,
                             nlohmann::json* result);
    ServiceStatus RevokeTokens(int64_t actor, int64_t user, nlohmann::json* result);
    ServiceStatus ListAudit(int64_t actor, int offset, int limit, int64_t target,
                            std::vector<AuditLogRecord>* records, int* total);

   private:
    UserDao* users_;
    RedisStore* redis_;
    AuditLogDao* audit_;
};
}  // namespace sparkpush
