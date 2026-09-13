#pragma once

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

inline constexpr int kUserStatusActive = 1;
inline constexpr int kUserStatusDisabled = 2;
inline constexpr int kUserStatusDeleted = 3;
inline constexpr int64_t kAdminUserId = 900000000000LL;
inline constexpr int64_t kHermesBotUserId = 900000000001LL;
inline constexpr const char* kHermesBotAccount = "hermes_bot";
inline constexpr const char* kHermesBotName = "Pi Agent";

// 简单用户 DAO：目前只操作单库，后续可在这里加入分库分表路由
class UserDao {
   public:
    explicit UserDao(MySqlConnectionPool* pool) : pool_(pool) {}

    // 启动时幂等补齐用户中心字段和索引，兼容已经存在的开发库。
    bool EnsureUserCenterSchema(std::string* err_msg);

    // 启动时幂等创建 Hermes 机器人账号。机器人不通过普通登录接口获取
    // Token，只作为单聊会话的一端参与消息持久化和推送。
    bool EnsureHermesBotUser(int64_t bot_user_id, const std::string& account,
                             const std::string& name, std::string* err_msg);

    // 功能：创建用户
    // 参数：account 账号；name 昵称；password 密码 hash；user_id 输出
    // id；err_msg 错误 返回：成功 true，失败 false
    bool CreateUser(const std::string& account, const std::string& name,
                    const std::string& password, int64_t* user_id,
                    std::string* err_msg);

    // 功能：按账号查询用户
    bool GetUserByAccount(const std::string& account, User* user,
                          std::string* err_msg);

    // 功能：按 id 查询用户
    bool GetUserById(int64_t user_id, User* user, std::string* err_msg);

    // 登录时将旧密码摘要升级为当前 PBKDF2 编码。
    bool UpdatePasswordHash(int64_t user_id, const std::string& password_hash,
                            std::string* err_msg);

    // 管理员分页查询用户；status=0 表示查询全部状态。
    bool ListUsers(int offset, int limit, int status, std::vector<User>* users,
                   int* total, std::string* err_msg);

    // 修改用户状态：active / disabled / deleted。deleted 状态会写 deleted_at。
    bool UpdateStatus(int64_t user_id, int status, std::string* err_msg);

    // 管理员修改昵称。
    bool UpdateName(int64_t user_id, const std::string& name,
                    std::string* err_msg);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
