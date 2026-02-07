#include "user_dao.h"

#include "logging.h"

#include <mysql/mysql.h>

namespace sparkpush {

// 预期表结构（示意）:
// CREATE TABLE user (
//   id BIGINT PRIMARY KEY AUTO_INCREMENT,
//   account VARCHAR(64) UNIQUE NOT NULL,
//   password_hash VARCHAR(128) NOT NULL,
//   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
// );

bool UserDao::CreateUser(const std::string& account,
                         const std::string& name,
                         const std::string& password,
                         int64_t* user_id,
                         std::string* err_msg) {
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }

  const char* sql =
      "INSERT INTO user(account, name, password_hash) VALUES(?, ?, ?)";

  MYSQL_STMT* stmt = mysql_stmt_init(conn);
  if (!stmt) {
    std::string e = "mysql_stmt_init failed";
    LogError(e);
    if (err_msg) *err_msg = e;
    return false;
  }

  if (mysql_stmt_prepare(stmt, sql, static_cast<unsigned long>(strlen(sql))) !=
      0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("CreateUser prepare failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  MYSQL_BIND bind[3];
  memset(bind, 0, sizeof(bind));

  unsigned long account_len = static_cast<unsigned long>(account.size());
  unsigned long name_len = static_cast<unsigned long>(name.size());
  unsigned long pwd_len = static_cast<unsigned long>(password.size());

  bind[0].buffer_type = MYSQL_TYPE_STRING;
  bind[0].buffer = const_cast<char*>(account.data());
  bind[0].buffer_length = account_len;
  bind[0].length = &account_len;

  bind[1].buffer_type = MYSQL_TYPE_STRING;
  bind[1].buffer = const_cast<char*>(name.data());
  bind[1].buffer_length = name_len;
  bind[1].length = &name_len;

  bind[2].buffer_type = MYSQL_TYPE_STRING;
  bind[2].buffer = const_cast<char*>(password.data());
  bind[2].buffer_length = pwd_len;
  bind[2].length = &pwd_len;

  if (mysql_stmt_bind_param(stmt, bind) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("CreateUser bind_param failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  if (mysql_stmt_execute(stmt) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("CreateUser execute failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  if (user_id) {
    *user_id = static_cast<int64_t>(mysql_insert_id(conn));
  }

  mysql_stmt_close(stmt);
  return true;
}

bool UserDao::GetUserByAccount(const std::string& account,
                               User* user,
                               std::string* err_msg) {
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }

  const char* sql =
      "SELECT id, account, name, password_hash FROM user "
      "WHERE account = ? LIMIT 1";

  MYSQL_STMT* stmt = mysql_stmt_init(conn);
  if (!stmt) {
    std::string e = "mysql_stmt_init failed";
    LogError(e);
    if (err_msg) *err_msg = e;
    return false;
  }
  
  if (mysql_stmt_prepare(stmt, sql, static_cast<unsigned long>(strlen(sql))) !=
      0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserByAccount prepare failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  MYSQL_BIND param[1];
  memset(param, 0, sizeof(param));
  unsigned long account_len = static_cast<unsigned long>(account.size());
  param[0].buffer_type = MYSQL_TYPE_STRING;
  param[0].buffer = const_cast<char*>(account.data());
  param[0].buffer_length = account_len;
  param[0].length = &account_len;

  if (mysql_stmt_bind_param(stmt, param) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserByAccount bind_param failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  if (mysql_stmt_execute(stmt) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserByAccount execute failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  if (mysql_stmt_store_result(stmt) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserByAccount store_result failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  my_ulonglong row_count = mysql_stmt_num_rows(stmt);
  if (row_count == 0) {
    if (err_msg) *err_msg = "user not found";
    mysql_stmt_close(stmt);
    return false;
  }

  MYSQL_BIND result[4];
  memset(result, 0, sizeof(result));

  long long id_buf = 0;
  char account_buf[128] = {0};
  char name_buf[128] = {0};
  char pwd_buf[256] = {0};
  unsigned long account_out_len = 0;
  unsigned long name_out_len = 0;
  unsigned long pwd_out_len = 0;

  result[0].buffer_type = MYSQL_TYPE_LONGLONG;
  result[0].buffer = &id_buf;
  result[0].is_unsigned = 0;

  result[1].buffer_type = MYSQL_TYPE_STRING;
  result[1].buffer = account_buf;
  result[1].buffer_length = sizeof(account_buf);
  result[1].length = &account_out_len;

  result[2].buffer_type = MYSQL_TYPE_STRING;
  result[2].buffer = name_buf;
  result[2].buffer_length = sizeof(name_buf);
  result[2].length = &name_out_len;

  result[3].buffer_type = MYSQL_TYPE_STRING;
  result[3].buffer = pwd_buf;
  result[3].buffer_length = sizeof(pwd_buf);
  result[3].length = &pwd_out_len;

  if (mysql_stmt_bind_result(stmt, result) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserByAccount bind_result failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  int fetch_ret = mysql_stmt_fetch(stmt);
  if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserByAccount fetch failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  user->id = id_buf;
  user->account.assign(account_buf, account_out_len);
  user->name.assign(name_buf, name_out_len);
  user->password_hash.assign(pwd_buf, pwd_out_len);

  mysql_stmt_close(stmt);
  return true;
}

bool UserDao::GetUserById(int64_t user_id,
                          User* user,
                          std::string* err_msg) {
  if (!pool_) {
    if (err_msg) *err_msg = "mysql pool not initialized";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }

  const char* sql =
      "SELECT id, account, name, password_hash FROM user "
      "WHERE id = ? LIMIT 1";

  MYSQL_STMT* stmt = mysql_stmt_init(conn);
  if (!stmt) {
    std::string e = "mysql_stmt_init failed";
    LogError(e);
    if (err_msg) *err_msg = e;
    return false;
  }

  if (mysql_stmt_prepare(stmt, sql, static_cast<unsigned long>(strlen(sql))) !=
      0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserById prepare failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  MYSQL_BIND param[1];
  memset(param, 0, sizeof(param));

  long long id_param = user_id;
  param[0].buffer_type = MYSQL_TYPE_LONGLONG;
  param[0].buffer = &id_param;
  param[0].is_unsigned = 0;

  if (mysql_stmt_bind_param(stmt, param) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserById bind_param failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  if (mysql_stmt_execute(stmt) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserById execute failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  if (mysql_stmt_store_result(stmt) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserById store_result failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  my_ulonglong row_count = mysql_stmt_num_rows(stmt);
  if (row_count == 0) {
    if (err_msg) *err_msg = "user not found";
    mysql_stmt_close(stmt);
    return false;
  }

  MYSQL_BIND result[4];
  memset(result, 0, sizeof(result));

  long long id_buf = 0;
  char account_buf[128] = {0};
  char name_buf[128] = {0};
  char pwd_buf[256] = {0};
  unsigned long account_out_len = 0;
  unsigned long name_out_len = 0;
  unsigned long pwd_out_len = 0;

  result[0].buffer_type = MYSQL_TYPE_LONGLONG;
  result[0].buffer = &id_buf;
  result[0].is_unsigned = 0;

  result[1].buffer_type = MYSQL_TYPE_STRING;
  result[1].buffer = account_buf;
  result[1].buffer_length = sizeof(account_buf);
  result[1].length = &account_out_len;

  result[2].buffer_type = MYSQL_TYPE_STRING;
  result[2].buffer = name_buf;
  result[2].buffer_length = sizeof(name_buf);
  result[2].length = &name_out_len;

  result[3].buffer_type = MYSQL_TYPE_STRING;
  result[3].buffer = pwd_buf;
  result[3].buffer_length = sizeof(pwd_buf);
  result[3].length = &pwd_out_len;

  if (mysql_stmt_bind_result(stmt, result) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserById bind_result failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  int fetch_ret = mysql_stmt_fetch(stmt);
  if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
    std::string e = mysql_stmt_error(stmt);
    LogError("GetUserById fetch failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  user->id = id_buf;
  user->account.assign(account_buf, account_out_len);
  user->name.assign(name_buf, name_out_len);
  user->password_hash.assign(pwd_buf, pwd_out_len);

  mysql_stmt_close(stmt);
  return true;
}

}  // namespace sparkpush


