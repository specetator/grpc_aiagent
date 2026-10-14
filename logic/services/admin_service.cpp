#include "services/admin_service.h"

#include "audit_log_dao.h"
#include "logging.h"
#include "redis_store.h"
#include "services/business_rules.h"
#include "user_dao.h"

namespace sparkpush {
ServiceStatus AdminService::ListUsers(int64_t actor, int offset, int limit, int status,
                                      std::vector<User>* users, int* total) {
    auto access = RequireAdministrator(actor);
    if (!access.ok()) return access;
    if (!users_) return {500, "user dao not initialized"};
    if (offset < 0 || limit <= 0 || limit > 100)
        return {400, "offset/limit out of range"};
    std::string error;
    if (!users || !total ||
        !users_->ListUsers(offset, limit, status, users, total, &error))
        return {500, "list users failed: " + error};
    return {};
}
ServiceStatus AdminService::SetStatus(int64_t actor, int64_t user_id, int status,
                                      const std::string& reason,
                                      nlohmann::json* result) {
    auto access = RequireAdministrator(actor);
    if (!access.ok()) return access;
    if (!CenterAvailable()) return {500, "user center dependencies not initialized"};
    if (!result) return {500, "user output is null"};
    if (status < kUserStatusActive || status > kUserStatusDeleted)
        return {400, "status must be active/disabled/deleted"};
    if (user_id <= 0 || user_id == kAdminUserId || reason.size() > 512) {
        return {400, "invalid user_id or reason"};
    }

    User before;
    std::string err;
    if (!users_->GetUserById(user_id, &before, &err)) {
        return {404, "user not found"};
    }

    int revoked_tokens = 0;
    // 先撤销凭证再改变状态，避免状态已经 disabled 但旧 token 仍可用。
    // 吊销失败直接 503，不推进状态：宁可让管理员重试，
    // 也不能留下一个「看似封了、token 还能用」的账号。
    // 恢复成 active 不走这一支：解禁不应该把用户现有登录也清掉。
    if (status != kUserStatusActive &&
        !redis_->RevokeUserTokens(user_id, &revoked_tokens)) {
        return {503, "revoke user tokens failed"};
    }
    if (!users_->UpdateStatus(user_id, status, &err)) {
        return {500, "update user status failed: " + err};
    }

    nlohmann::json metadata = {
        {"old_status", UserStatusName(before.status)},
        {"new_status", UserStatusName(status)},
        {"revoked_tokens", revoked_tokens},
    };
    const std::string action =
        status == kUserStatusDisabled
            ? "user.disable"
            : (status == kUserStatusDeleted ? "user.delete" : "user.restore");
    bool audit_recorded = false;
    if (audit_) {
        std::string audit_error;
        audit_recorded = audit_->Append(kAdminUserId, user_id, action, reason,
                                        metadata.dump(), &audit_error);
        // 审计失败不回滚状态。动作已经生效，回滚会变成
        // 「日志说没改、库里已经改了」。调用方看 audit_recorded。
        if (!audit_recorded) {
            LOG_ERROR << "User status audit failed: " << audit_error;
        }
    }

    // 回读失败就用改之前的快照。更新本身已经成功，不能因为这次 SELECT
    // 失败把整个操作报成失败（管理员会重试，而状态其实已经是目标值）。
    // 代价是响应里的 status 仍是旧值。new_status 只写进审计 metadata，
    // 不在返回给调用方的这份 JSON 里。
    User after;
    if (!users_->GetUserById(user_id, &after, &err)) after = before;
    nlohmann::json data = {
        {"user_id", after.id},
        {"account", after.account},
        {"status", UserStatusName(after.status)},
        {"status_code", after.status},
        {"revoked_tokens", revoked_tokens},
        {"audit_recorded", audit_recorded},
    };
    if (after.deleted_at.empty()) {
        data["deleted_at"] = nullptr;
    } else {
        data["deleted_at"] = after.deleted_at;
    }
    *result = std::move(data);
    return {};
}
ServiceStatus AdminService::UpdateName(int64_t actor, int64_t user_id,
                                       const std::string& name,
                                       nlohmann::json* result) {
    auto access = RequireAdministrator(actor);
    if (!access.ok()) return access;
    if (!users_) return {500, "user dao not initialized"};
    if (!result) return {500, "user output is null"};
    if (user_id <= 0 || user_id == kAdminUserId || name.empty() || name.size() > 64) {
        return {400, "invalid user_id/name"};
    }
    User before;
    std::string err;
    if (!users_->GetUserById(user_id, &before, &err)) {
        return {404, "user not found"};
    }
    if (!users_->UpdateName(user_id, name, &err)) {
        return {500, "update user failed: " + err};
    }
    bool audit_recorded = false;
    if (audit_) {
        std::string audit_error;
        nlohmann::json metadata = {{"old_name", before.name}, {"new_name", name}};
        audit_recorded = audit_->Append(kAdminUserId, user_id, "user.update_profile",
                                        "", metadata.dump(), &audit_error);
        if (!audit_recorded) LOG_ERROR << "User profile audit failed: " << audit_error;
    }
    nlohmann::json data = {
        {"user_id", user_id}, {"name", name}, {"audit_recorded", audit_recorded}};
    *result = std::move(data);
    return {};
}
ServiceStatus AdminService::RevokeTokens(int64_t actor, int64_t user_id,
                                         nlohmann::json* result) {
    auto access = RequireAdministrator(actor);
    if (!access.ok()) return access;
    if (!CenterAvailable()) return {500, "user center dependencies not initialized"};
    if (!result) return {500, "user output is null"};
    if (user_id <= 0 || user_id == kAdminUserId) {
        return {400, "invalid user_id"};
    }
    User target;
    std::string err;
    if (!users_->GetUserById(user_id, &target, &err)) {
        return {404, "user not found"};
    }
    int revoked_tokens = 0;
    if (!redis_->RevokeUserTokens(user_id, &revoked_tokens)) {
        return {503, "revoke user tokens failed"};
    }
    bool audit_recorded = false;
    if (audit_) {
        std::string audit_error;
        nlohmann::json metadata = {{"revoked_tokens", revoked_tokens}};
        audit_recorded = audit_->Append(kAdminUserId, user_id, "user.revoke_tokens", "",
                                        metadata.dump(), &audit_error);
        if (!audit_recorded) LOG_ERROR << "Token revoke audit failed: " << audit_error;
    }
    nlohmann::json data = {{"user_id", user_id},
                           {"revoked_tokens", revoked_tokens},
                           {"audit_recorded", audit_recorded}};
    *result = std::move(data);
    return {};
}
ServiceStatus AdminService::ListAudit(int64_t actor, int offset, int limit,
                                      int64_t target,
                                      std::vector<AuditLogRecord>* records,
                                      int* total) {
    auto access = RequireAdministrator(actor);
    if (!access.ok()) return access;
    if (!audit_) return {500, "audit log dao not initialized"};
    if (offset < 0 || limit <= 0 || limit > 100 || target < 0)
        return {400, "invalid offset/limit/target_user_id"};
    std::string error;
    if (!records || !total ||
        !audit_->List(offset, limit, target, records, total, &error))
        return {500, "list audit logs failed: " + error};
    return {};
}
}  // namespace sparkpush
