#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "services/repositories.h"
#include "services/service_status.h"

namespace sparkpush {

struct LoginResult {
    int64_t user_id{0};
    std::string token;
    std::string name;
    bool administrator{false};
};

class UserService {
   public:
    struct Options {
        std::string admin_account;
        std::string admin_password;
        std::unordered_set<int64_t> bot_users;
    };
    enum class TokenAudience { kHttpUser, kRealtimeUser };

    UserService(UserRepository* repository, Options options)
        : repository_(repository), options_(std::move(options)) {}

    ServiceStatus Login(const std::string& account, const std::string& password,
                        LoginResult* result);
    ServiceStatus Register(const std::string& account, const std::string& password,
                           const std::string& name, LoginResult* result);
    ServiceStatus Authenticate(const std::string& token, TokenAudience audience,
                               int64_t* user_id);
    ServiceStatus AuthenticateAdmin(const std::string& token);
    bool AdminEnabled() const;
    bool AuthenticationAvailable() const;
    bool UsersAvailable() const;
    // Preserve the existing 2s hot-path cache. Authentication ALWAYS bypasses
    // it, so disabling a user immediately prevents a new HTTP/WS handshake.
    bool GetActiveUser(int64_t user_id, User* user, std::string* error);

   private:
    ServiceStatus IssueToken(int64_t user_id, std::string* token);
    UserRepository* repository_;
    Options options_;
    struct CacheEntry {
        User user;
        std::chrono::steady_clock::time_point expires_at;
    };
    std::mutex cache_mutex_;
    std::unordered_map<int64_t, CacheEntry> active_users_;
};
}  // namespace sparkpush
