#pragma once
#include <nlohmann/json.hpp>

#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
class UserDao;
class RedisStore;
class AuditLogDao;

// 用户中心的管理操作：查/改用户、封禁解禁、吊销凭证、查审计。
//
// 三个不变量贯穿所有方法：
//   1. 每个方法第一行都是 RequireAdministrator(actor)，没有例外。
//      actor 由适配层在 requireAdmin 验过 Bearer token 之后代入常量，
//      绝不来自请求体（详见 business_rules.h）。
//   2. 不允许对 kAdminUserId 自己动手：封禁/删除/改名/吊销都先把
//      user_id == kAdminUserId 挡成 400。否则一个误操作就能把自己锁在门外，
//      而管理员账号是内置的，没法从数据库以外恢复。
//   3. 审计写失败不阻断动作，但会把 audit_recorded=false 放进响应，
//      让调用方知道「事成了但没记上」。
class AdminService {
   public:
    AdminService(UserDao* users, RedisStore* redis, AuditLogDao* audit)
        : users_(users), redis_(redis), audit_(audit) {}
    bool UsersAvailable() const { return users_ != nullptr; }
    // 封禁类操作需要 Redis：因为要先吊销 token，而 token 存在 Redis 里。
    // 只查/改用户表的接口用 UsersAvailable() 就够了。
    bool CenterAvailable() const { return users_ && redis_; }
    bool AuditAvailable() const { return audit_ != nullptr; }

    // 分页查用户。limit 在本层与 UserDao 各钳一次 [1,100]（双层防御）。
    ServiceStatus ListUsers(int64_t actor, int offset, int limit, int status,
                            std::vector<User>* users, int* total);

    // 改用户状态（active / disabled / deleted）。
    //
    // **顺序不可颠倒**：先 RevokeUserTokens 再 UpdateStatus。
    // 反过来的话存在一个窗口：状态已经是 disabled，但用户手里已签发的
    // token 在 Redis 里仍然有效。新握手会被 Authenticate 的 IsActive 检查挡下，
    // 但**已建立的连接**走的是 GetActiveUser（2s 缓存），因此那段窗口里
    // 它还能继续发消息。先吊销 token 则最坏情况是「token 没了但状态还没改」，
    // 用户只需要重新登录，不构成安全缺口。
    // 吊销失败直接 503 并**不推进状态**：宁可让管理员重试，
    // 也不能留下一个「看似封了实则还能用」的账号。
    //
    // reason 上限 512 与 audit_log.reason 列宽一致。
    // result 里会返回 old_status/new_status/revoked_tokens/audit_recorded。
    ServiceStatus SetStatus(int64_t actor, int64_t user, int status,
                            const std::string& reason, nlohmann::json* result);

    // 管理员改昵称。name 上限 64 与 user.name VARCHAR(64) 一致。
    // 不需要 CenterAvailable()：本操作不碰 token。
    ServiceStatus UpdateName(int64_t actor, int64_t user, const std::string& name,
                             nlohmann::json* result);

    // 只吊销 token 而不改状态。用于「强制重新登录」这类场景：
    // 怀疑凭证泄露时踢掉所有会话，但账号本身仍然可用。
    ServiceStatus RevokeTokens(int64_t actor, int64_t user, nlohmann::json* result);

    // 分页查审计日志。target=0 表示不按对象过滤。
    ServiceStatus ListAudit(int64_t actor, int offset, int limit, int64_t target,
                            std::vector<AuditLogRecord>* records, int* total);

   private:
    UserDao* users_;
    RedisStore* redis_;
    AuditLogDao* audit_;
};
}  // namespace sparkpush
