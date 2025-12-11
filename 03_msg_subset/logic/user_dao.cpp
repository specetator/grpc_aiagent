// ============================================================================
// 用户 DAO 实现
// 
// 使用 MySQL C API 的预编译语句（Prepared Statement）执行数据库操作
// ============================================================================
#include "user_dao.h"

#include <mutex>

namespace sparkpush {

// ============================================================================
// CreateUser: 创建新用户记录
// ============================================================================
bool UserDao::CreateUser(const std::string& account, const std::string& name,
                         const std::string& password, int64_t* user_id,
                         std::string* err_msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (account_to_id_.count(account)) {
        if (err_msg) *err_msg = "account already exists";
        return false;
    }
    int64_t id = next_id_++;
    User u;
    u.id = id;
    u.account = account;
    u.name = name;
    u.password_hash = password;
    users_[id] = u;
    account_to_id_[account] = id;
    if (user_id) *user_id = id;
    return true;
}

// ============================================================================
// GetUserByAccount: 按账号查询用户
// ============================================================================

// 按账号查询用户
bool UserDao::GetUserByAccount(const std::string& account, User* user,
                               std::string* err_msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = account_to_id_.find(account);
    if (it == account_to_id_.end()) {
        if (err_msg) *err_msg = "user not found";
        return false;
    }
    if (user) *user = users_[it->second];
    return true;
}

// 按 id 查询用户
bool UserDao::GetUserById(int64_t user_id, User* user, std::string* err_msg) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = users_.find(user_id);
    if (it == users_.end()) {
        if (err_msg) *err_msg = "user not found";
        return false;
    }
    if (user) *user = it->second;
    return true;
}

}  // namespace sparkpush
