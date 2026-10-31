#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "services/repositories.h"
#include "services/service_status.h"

namespace sparkpush {

// 登录/注册的结果。administrator=true 只会在管理员内置账号登录成功时置位，
// 前端靠它决定是否展示管理入口。
struct LoginResult {
    int64_t user_id{0};
    std::string token;
    std::string name;
    bool administrator{false};
};
struct ContactSummary { int64_t user_id{0}; std::string account, name; };

// 账号、密码、token 与活跃状态的业务层。
//
// 存储全部走 UserRepository 端口，本类不直接碰 MySQL/Redis，
// 因此 tests/logic_business_service_test.cpp 能用内存假实现跑完整流程。
class UserService {
   public:
    struct Options {
        // 管理员内置账号。为空则 AdminEnabled() 返回 false，
        // 所有 /api/admin/* 接口会直接回 503。这是「没配就不开」的 fail-closed。
        std::string admin_account;
        std::string admin_password;
        // 机器人账号集合（如 Hermes bot）。它们可以当会话的一端，
        // 但**不允许**用 realtime token 建 WS 连接（见 Authenticate）。
        std::unordered_set<int64_t> bot_users;
    };
    // 同一个 token 在两种场景下规则不同，因此必须区分受众：
    //   kHttpUser      普通 HTTP 接口（http_server.cpp）
    //   kRealtimeUser  WS 握手与消息提交（grpc_service.cpp / message_service.cpp）
    // realtime 额外排除两类身份：管理员与机器人。
    // 管理员不能建 WS 是因为它没有对应的会话端点；
    // 机器人不能建 WS 是因为它们的消息由服务端自己注入，
    // 开一个能主动发消息的 WS 口子等于给 Agent 一个绕过编排的后门。
    enum class TokenAudience { kHttpUser, kRealtimeUser };

    UserService(UserRepository* repository, Options options)
        : repository_(repository), options_(std::move(options)) {}

    // 登录。四条特殊路径：
    //   1. Hermes bot 账号直接 403，不允许登录
    //   2. 管理员内置账号用 ConstantTimeEquals 比对配置里的明文密码，不查用户表
    //      （恒时比对是为了抗 timing attack：普通 != 会在第一个不同字节处
    //       提前返回，攻击者能靠测时间逐字节猜出密码）
    //   3. 普通账号 FindByAccount -> VerifyPassword
    //   4. needs_rehash 为真时透明升级密码摘要（失败只记日志，不阻断登录）
    // 注意密码错与账号不存在返回不同状态码（401 vs 404），
    // 这会泄露「账号是否存在」。若要收紧应统一成 401。
    ServiceStatus Login(const std::string& account, const std::string& password,
                        LoginResult* result);
    // 注册并直接登录（返回可用的 token）。
    // 保留账号（admin_account / kHermesBotAccount）在入口处就被 409 挡掉。
    ServiceStatus Register(const std::string& account, const std::string& password,
                           const std::string& name, LoginResult* result);
    // 验 token 并取出 user_id。按 audience 应用不同规则，详见 TokenAudience。
    ServiceStatus Authenticate(const std::string& token, TokenAudience audience,
                               int64_t* user_id);
    // 管理员 token 验证：解析出的 id 必须等于 kAdminUserId，否则 403。
    // 这是 requireAdmin 门禁的核心，所有 /api/admin/* 接口都先过这一道。
    ServiceStatus AuthenticateAdmin(const std::string& token);
    bool AdminEnabled() const;
    bool AuthenticationAvailable() const;
    bool UsersAvailable() const;
    // Preserve the existing 2s hot-path cache. Authentication ALWAYS bypasses
    // it, so disabling a user immediately prevents a new HTTP/WS handshake.
    //
    // 译：保留现有的 2 秒热路径缓存。鉴权（Authenticate）**永远绕过它**，
    // 因此封禁一个用户会立即阻止新的 HTTP/WS 握手。
    //
    // 两个方法的分工必须看清楚：
    //   Authenticate   握手/登录时调，每次真的去查库，不走缓存
    //   GetActiveUser  发消息热路径调（message_service 每条消息要查发送者、
    //                  收件人、bot 账号），带 2s 缓存避开每条消息三次查库
    // 代价：一个刚被封禁的用户，已建立的连接在最多 2 秒内仍能发消息。
    // 这是有意的权衡：把封禁生效时间从「立即」放宽到 2 秒，
    // 换来热路径上每条消息少三次 MySQL 往返。
    // 另外缓存**只存成功结果**：查不到或已禁用的用户不会被缓存，
    // 因此解禁/新建账号立即生效，不会被旧的否定结果卡住。
    bool GetActiveUser(int64_t user_id, User* user, std::string* error);
    // Exact registered account only; no prefix search or credential fields.
    ServiceStatus LookupContact(int64_t actor, const std::string& account,
                                ContactSummary* contact);

   private:
    // 生成随机 token 并存进 Redis，TTL 24 小时。
    // 先 GenerateSecureToken 再 SaveToken：两步中任一步失败都不返回半成品 token，
    // 避开客户端拿到一个存不进去、下次鉴权必失败的 token。
    ServiceStatus IssueToken(int64_t user_id, std::string* token);
    UserRepository* repository_;
    Options options_;
    struct CacheEntry {
        User user;
        // 用 steady_clock 而不是 system_clock：过期判定只看时间差，
        // 用墙上时钟的话一次 NTP 回拨就能让所有缓存项提前失效或永久不过期。
        std::chrono::steady_clock::time_point expires_at;
    };
    // 一把锁保护整张缓存表。持锁期间只做查找/插入/删除，
    // 查库（FindById）在锁**外**做：否则一次慢查询会把所有线程堵在锁上。
    // 代价是同一个 user_id 可能被并发查库多次（惊群），
    // 但结果幂等且只多几次读，比阻塞整个热路径便宜得多。
    std::mutex cache_mutex_;
    std::unordered_map<int64_t, CacheEntry> active_users_;
};
}  // namespace sparkpush
