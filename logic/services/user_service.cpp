#include "services/user_service.h"

#include <nlohmann/json.hpp>

#include "logging.h"
#include "metrics.h"
#include "security.h"

namespace sparkpush {
namespace {
// 两个条件必须同时成立。软删除会同时写 status 和 deleted_at，
// 但历史脏数据可能只写了其中一个；只看 status 会把「已删除但状态
// 还停在 active」的行放进来。
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
    // 管理员不查用户表：密码是配置里的明文，用恒时比较。
    // 这一支在 FindByAccount 之前返回，所以管理员账号即使和某个真实用户重名，
    // 也进不了下面的普通登录。没配 admin_account 时整支跳过，
    // AdminEnabled() 会是 false，管理接口另外回 503。
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
    // 摘要升级失败只记日志。密码已经验过，不能因为回写失败把人挡在门外；
    // 下次登录 needs_rehash 仍为真，会再试一次。
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
    // FindByAccount 的三种结果不能混：
    //   返回 true                         账号已存在 -> 409
    //   false 且 error 是 "user not found"  正常的「没有这个人」，继续注册
    //   false 且是别的错误文本               数据库故障 -> 500
    // 第三支漏掉的话，MySQL 宕机会被当成「账号可用」。
    // "user not found" 是 DAO 的契约字符串，见 user_dao.h，不要改措辞。
    // 这仍是 check-then-act：并发注册同名账号时，输的一方会从 Create
    // 拿到 500（DAO 不把 1062 翻译成 409）。
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
    // 「存储不可用」的状态码按受众分开，不要顺手对齐：
    //   realtime -> 500，WS 握手失败，客户端会重连
    //   http     -> 503，HTTP 约定的「鉴权服务不可用」
    const bool realtime = audience == TokenAudience::kRealtimeUser;
    if (!repository_ || !repository_->TokensAvailable())
        return realtime ? ServiceStatus{500, "redis store not initialized"}
                        : ServiceStatus{503, "user authentication is unavailable"};
    if (!realtime && !UsersAvailable())
        return {503, "user authentication is unavailable"};
    int64_t id = 0;
    // id<=0 只在 HTTP 受众这里直接 401。realtime 的非法 id 落到下面，
    // 最终仍是 403，不会写进 *user_id。
    if (!repository_->ResolveToken(token, &id) || (!realtime && id <= 0))
        return {401, realtime ? "invalid or expired token"
                              : "user token invalid or expired"};
    // 管理员和机器人可以持有 HTTP token（登录/内部身份），但不能拿来建 WS。
    // 这一支在 IsActive 之前返回，文案是 "not allowed" 而不是 "disabled"。
    if (realtime &&
        (!UsersAvailable() || id == kAdminUserId || options_.bot_users.count(id)))
        return {403, "user token is not allowed"};
    // 每次真的查库，不走 GetActiveUser 的 2 秒缓存。
    // 因此封禁在下一次握手就生效，不用等热路径缓存过期。
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
    // 只认 kAdminUserId。普通用户的有效 token 解析得出来也是 403，
    // 不能因为 Redis 里有这条 token 就放行。
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
        // 命中且未过期才用。过期项顺手删掉，避免表只增不减。
        // 返回前再过一遍 IsActive：缓存里理论上只有活跃用户，
        // 这一步是防以后有人把否定结果也塞进来。
        if (it != active_users_.end() && it->second.expires_at > now) {
            *user = it->second.user;
            MetricsRegistry::Instance().Increment(
                "spark_push_user_active_cache_hit_total");
            return IsActive(*user);
        }
        if (it != active_users_.end()) active_users_.erase(it);
    }
    // 查库在锁外。同一个 id 并发 miss 会打多次 FindById，结果幂等，
    // 比把慢查询关在锁里堵住整条热路径便宜。
    User loaded;
    const bool found = repository_->FindById(user_id, &loaded, error);
    MetricsRegistry::Instance().Increment("spark_push_user_active_cache_miss_total");
    // 只缓存成功且活跃的用户。查不到或已禁用不写入，
    // 所以解禁、新建账号立即生效，不会被旧的否定结果卡住。
    if (!found || !IsActive(loaded)) return false;
    {
        std::lock_guard<std::mutex> lock(cache_mutex_);
        active_users_[user_id] = {loaded, now + std::chrono::seconds(2)};
    }
    *user = std::move(loaded);
    return true;
}
}  // namespace sparkpush
