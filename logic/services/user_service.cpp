#include "services/user_service.h"

#include <nlohmann/json.hpp>

#include "logging.h"
#include "metrics.h"
#include "security.h"

namespace sparkpush {
namespace {
bool IsActive(const User& user) {
    return user.status == kUserStatusActive && user.deleted_at.empty();
}
}  // namespace

bool UserService::UsersAvailable() const {
    return repository_ && repository_->UsersAvailable();
}
bool UserService::AuthenticationAvailable() const {
    return UsersAvailable() && repository_->TokensAvailable();
}
bool UserService::AdminEnabled() const {
    return repository_ && repository_->TokensAvailable() &&
           !options_.admin_account.empty() && !options_.admin_password.empty();
}

ServiceStatus UserService::IssueToken(int64_t user_id, std::string* token) {
    std::string error;
    if (!GenerateSecureToken(token, &error)) return {500, "generate token failed"};
    if (!repository_ || !repository_->SaveToken(*token, user_id, 24 * 3600))
        return {500, "save token to redis failed"};
    return {};
}

ServiceStatus UserService::Login(const std::string& account,
                                 const std::string& password, LoginResult* result) {
    if (!result) return {500, "login output is null"};
    *result = {};
    if (account == kHermesBotAccount)
        return {403, "Hermes Bot account cannot login directly"};
    if (!options_.admin_account.empty() && account == options_.admin_account) {
        if (options_.admin_password.empty() ||
            !ConstantTimeEquals(password, options_.admin_password))
            return {401, "invalid password"};
        auto status = IssueToken(kAdminUserId, &result->token);
        if (!status.ok()) return status;
        result->user_id = kAdminUserId;
        result->administrator = true;
        repository_->Audit(kAdminUserId, 0, "admin.login", R"({"login":"success"})");
        return {};
    }
    if (account.empty()) return {400, "account is required"};
    if (!UsersAvailable()) return {500, "user dao not initialized"};
    User user;
    std::string error;
    if (!repository_->FindByAccount(account, &user, &error))
        return {404, "user not found, please register first"};
    if (!IsActive(user))
        return {403, "user account is disabled or deleted",
                ServiceReason::kInactiveAccount};
    bool needs_rehash = false;
    if (!VerifyPassword(password, user.password_hash, &needs_rehash, &error))
        return {401, "invalid password"};
    if (needs_rehash) {
        std::string hash;
        if (HashPassword(password, &hash, &error) &&
            !repository_->UpdatePassword(user.id, hash, &error))
            LOG_ERROR << "Password hash upgrade failed for user_id=" << user.id << ": "
                      << error;
    }
    auto status = IssueToken(user.id, &result->token);
    if (!status.ok()) return status;
    result->user_id = user.id;
    result->name = user.name;
    repository_->Audit(user.id, user.id, "user.login",
                       nlohmann::json{{"account", user.account}}.dump());
    return {};
}

ServiceStatus UserService::Register(const std::string& account,
                                    const std::string& password,
                                    const std::string& name, LoginResult* result) {
    if (!result) return {500, "login output is null"};
    *result = {};
    if (account.empty() || password.size() < 6 || password.size() > 128)
        return {400, "account/password are required"};
    if (!options_.admin_account.empty() && account == options_.admin_account)
        return {409, "admin account is reserved, please use login directly"};
    if (account == kHermesBotAccount)
        return {409, "Hermes Bot account is reserved for the system"};
    if (!UsersAvailable()) return {500, "user dao not initialized"};
    User existing;
    std::string error;
    if (repository_->FindByAccount(account, &existing, &error))
        return {409, "account already exists"};
    if (!error.empty() && error != "user not found")
        return {500, "query user failed: " + error};
    std::string hash;
    if (!HashPassword(password, &hash, &error)) return {500, "hash password failed"};
    int64_t user_id = 0;
    if (!repository_->Create(account, name, hash, &user_id, &error))
        return {500, "register user failed: " + error};
    auto status = IssueToken(user_id, &result->token);
    if (!status.ok()) return status;
    result->user_id = user_id;
    result->name = name;
    repository_->Audit(user_id, user_id, "user.register",
                       nlohmann::json{{"account", account}}.dump());
    return {};
}

ServiceStatus UserService::Authenticate(const std::string& token,
                                        TokenAudience audience, int64_t* user_id) {
    if (!user_id) return {500, "user output is null"};
    *user_id = 0;
    const bool realtime = audience == TokenAudience::kRealtimeUser;
    if (!repository_ || !repository_->TokensAvailable())
        return realtime ? ServiceStatus{500, "redis store not initialized"}
                        : ServiceStatus{503, "user authentication is unavailable"};
    if (!realtime && !UsersAvailable())
        return {503, "user authentication is unavailable"};
    int64_t id = 0;
    if (!repository_->ResolveToken(token, &id) || (!realtime && id <= 0))
        return {401, realtime ? "invalid or expired token"
                              : "user token invalid or expired"};
    if (realtime &&
        (!UsersAvailable() || id == kAdminUserId || options_.bot_users.count(id)))
        return {403, "user token is not allowed"};
    User user;
    std::string error;
    if (!repository_->FindById(id, &user, &error) || !IsActive(user))
        return {403, "user account is disabled or deleted",
                ServiceReason::kInactiveAccount};
    *user_id = id;
    return {};
}

ServiceStatus UserService::AuthenticateAdmin(const std::string& token) {
    if (!AdminEnabled()) return {503, "admin login is disabled"};
    int64_t id = 0;
    if (!repository_->ResolveToken(token, &id) || id != kAdminUserId)
        return {403, "admin token invalid or expired"};
    return {};
}

bool UserService::GetActiveUser(int64_t user_id, User* user, std::string* error) {
    if (!user || !UsersAvailable() || user_id <= 0) {
        if (error) *error = "invalid active user lookup arguments";
        return false;
    }
    const auto now = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        auto it = active_users_.find(user_id);
        if (it != active_users_.end() && it->second.expires_at > now) {
            *user = it->second.user;
            MetricsRegistry::Instance().Increment(
                "spark_push_user_active_cache_hit_total");
            return IsActive(*user);
        }
        if (it != active_users_.end()) active_users_.erase(it);
    }
    User loaded;
    const bool found = repository_->FindById(user_id, &loaded, error);
    MetricsRegistry::Instance().Increment("spark_push_user_active_cache_miss_total");
    if (!found || !IsActive(loaded)) return false;
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        active_users_[user_id] = {loaded, now + std::chrono::seconds(2)};
    }
    *user = std::move(loaded);
    return true;
}
}  // namespace sparkpush
