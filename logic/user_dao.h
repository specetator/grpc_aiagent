#pragma once

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 简单用户 DAO：目前只操作单库，后续可在这里加入分库分表路由
class UserDao {
   public:
    explicit UserDao(MySqlConnectionPool* pool) : pool_(pool) {}

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

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
