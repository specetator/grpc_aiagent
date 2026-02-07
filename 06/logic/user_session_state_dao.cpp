#include "user_session_state_dao.h"

#include "logging.h"

#include <mysql/mysql.h>

#include <cstring>

namespace sparkpush {

bool UserSessionStateDao::UpsertReadSeq(int64_t user_id,
                                        const std::string& session_id,
                                        int64_t read_seq,
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
      "INSERT INTO user_session_state(user_id, session_id, read_seq) "
      "VALUES(?, ?, ?) "
      "ON DUPLICATE KEY UPDATE read_seq=GREATEST(read_seq, VALUES(read_seq)), "
      "last_visit_at=CURRENT_TIMESTAMP";
  MYSQL_STMT* stmt = mysql_stmt_init(conn);
  if (!stmt) {
    if (err_msg) *err_msg = "mysql_stmt_init failed";
    return false;
  }
  if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }
  MYSQL_BIND bind[3];
  memset(bind, 0, sizeof(bind));
  long long uid_buf = user_id;
  bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[0].buffer = &uid_buf;

  unsigned long sid_len = static_cast<unsigned long>(session_id.size());
  bind[1].buffer_type = MYSQL_TYPE_STRING;
  bind[1].buffer = const_cast<char*>(session_id.data());
  bind[1].buffer_length = sid_len;
  bind[1].length = &sid_len;

  long long read_buf = read_seq;
  bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[2].buffer = &read_buf;

  if (mysql_stmt_bind_param(stmt, bind) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }
  if (mysql_stmt_execute(stmt) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }
  mysql_stmt_close(stmt);
  return true;
}

bool UserSessionStateDao::GetReadSeq(int64_t user_id,
                                     const std::string& session_id,
                                     int64_t* read_seq,
                                     std::string* err_msg) {
  if (!pool_ || !read_seq) {
    if (err_msg) *err_msg = "invalid arguments";
    return false;
  }
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }
  const char* sql =
      "SELECT read_seq FROM user_session_state "
      "WHERE user_id=? AND session_id=? LIMIT 1";
  MYSQL_STMT* stmt = mysql_stmt_init(conn);
  if (!stmt) {
    if (err_msg) *err_msg = "mysql_stmt_init failed";
    return false;
  }
  if (mysql_stmt_prepare(stmt, sql, strlen(sql)) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }
  MYSQL_BIND bind[2];
  memset(bind, 0, sizeof(bind));
  long long uid_buf = user_id;
  bind[0].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[0].buffer = &uid_buf;

  unsigned long sid_len = static_cast<unsigned long>(session_id.size());
  bind[1].buffer_type = MYSQL_TYPE_STRING;
  bind[1].buffer = const_cast<char*>(session_id.data());
  bind[1].buffer_length = sid_len;
  bind[1].length = &sid_len;

  if (mysql_stmt_bind_param(stmt, bind) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }
  if (mysql_stmt_execute(stmt) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }

  MYSQL_BIND result[1];
  memset(result, 0, sizeof(result));
  long long read_buf = 0;
  result[0].buffer_type = MYSQL_TYPE_LONGLONG;
  result[0].buffer = &read_buf;

  if (mysql_stmt_bind_result(stmt, result) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }
  int fetch_ret = mysql_stmt_fetch(stmt);
  if (fetch_ret == MYSQL_NO_DATA) {
    *read_seq = 0;
    mysql_stmt_close(stmt);
    return true;
  }
  if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }
  *read_seq = read_buf;
  mysql_stmt_close(stmt);
  return true;
}

}  // namespace sparkpush


