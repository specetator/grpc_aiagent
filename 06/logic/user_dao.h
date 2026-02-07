#pragma once

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 简单用户 DAO：目前只操作单库，后续可在这里加入分库分表路由
class UserDao {
 public:
  explicit UserDao(MySqlConnectionPool* pool) : pool_(pool) {}

  bool CreateUser(const std::string& account,
                  const std::string& name,
                  const std::string& password,
                  int64_t* user_id,
                  std::string* err_msg);

  bool GetUserByAccount(const std::string& account,
                        User* user,
                        std::string* err_msg);

  bool GetUserById(int64_t user_id,
                   User* user,
                   std::string* err_msg);

 private:
  MySqlConnectionPool* pool_;
};

}  // namespace sparkpush


