#include "message_dao.h"

#include "logging.h"

#include <mysql/mysql.h>

#include <algorithm>
#include <cstring>

namespace sparkpush {

bool MessageDao::InsertMessage(const Message& message, std::string* err_msg) {
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
      "INSERT INTO message(session_id, msg_seq, sender_id, msg_type, content_json, timestamp_ms, client_msg_id) "
      "VALUES(?, ?, ?, ?, ?, ?, ?)";
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

  MYSQL_BIND bind[7];
  memset(bind, 0, sizeof(bind));
  unsigned long sid_len = static_cast<unsigned long>(message.session_id.size());
  bind[0].buffer_type = MYSQL_TYPE_STRING;
  bind[0].buffer = const_cast<char*>(message.session_id.data());
  bind[0].buffer_length = sid_len;
  bind[0].length = &sid_len;

  long long seq_buf = message.msg_seq;
  bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[1].buffer = &seq_buf;

  long long sender_buf = message.sender_id;
  bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[2].buffer = &sender_buf;

  unsigned long type_len = static_cast<unsigned long>(message.msg_type.size());
  bind[3].buffer_type = MYSQL_TYPE_STRING;
  bind[3].buffer = const_cast<char*>(message.msg_type.data());
  bind[3].buffer_length = type_len;
  bind[3].length = &type_len;

  unsigned long content_len =
      static_cast<unsigned long>(message.content_json.size());
  bind[4].buffer_type = MYSQL_TYPE_STRING;
  bind[4].buffer = const_cast<char*>(message.content_json.data());
  bind[4].buffer_length = content_len;
  bind[4].length = &content_len;

  long long ts_buf = message.timestamp_ms;
  bind[5].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[5].buffer = &ts_buf;

  unsigned long client_msg_id_len =
      static_cast<unsigned long>(message.client_msg_id.size());
  bind[6].buffer_type = MYSQL_TYPE_STRING;
  bind[6].buffer = const_cast<char*>(message.client_msg_id.data());
  bind[6].buffer_length = client_msg_id_len;
  bind[6].length = &client_msg_id_len;

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

bool MessageDao::ListMessages(const std::string& session_id,
                              int64_t anchor_seq,
                              int limit,
                              std::vector<Message>* messages,
                              std::string* err_msg) {
  if (!pool_ || !messages) {
    if (err_msg) *err_msg = "invalid arguments";
    return false;
  }
  messages->clear();
  auto guard = pool_->Acquire();
  MYSQL* conn = guard.get();
  if (!conn) {
    if (err_msg) *err_msg = "no mysql connection";
    return false;
  }

  const char* sql_with_anchor =
      "SELECT session_id, msg_seq, sender_id, msg_type, content_json, timestamp_ms, client_msg_id "
      "FROM message WHERE session_id=? AND msg_seq<? "
      "ORDER BY msg_seq DESC LIMIT ?";
  const char* sql_without_anchor =
      "SELECT session_id, msg_seq, sender_id, msg_type, content_json, timestamp_ms, client_msg_id "
      "FROM message WHERE session_id=? "
      "ORDER BY msg_seq DESC LIMIT ?";

  const char* sql = (anchor_seq > 0) ? sql_with_anchor : sql_without_anchor;

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
  unsigned long sid_len = static_cast<unsigned long>(session_id.size());
  bind[0].buffer_type = MYSQL_TYPE_STRING;
  bind[0].buffer = const_cast<char*>(session_id.data());
  bind[0].buffer_length = sid_len;
  bind[0].length = &sid_len;

  int bind_count = 2;
  int index = 1;
  if (anchor_seq > 0) {
    long long anchor_buf = anchor_seq;
    bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
    bind[1].buffer = &anchor_buf;
    ++index;
    bind_count = 3;
  }
  int limit_buf = limit <= 0 ? 50 : limit;
  bind[index].buffer_type = MYSQL_TYPE_LONG;
  bind[index].buffer = &limit_buf;

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
  if (mysql_stmt_store_result(stmt) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }

  MYSQL_BIND result[7];
  memset(result, 0, sizeof(result));
  char session_buf[256];
  unsigned long session_len = 0;
  long long seq_buf = 0;
  long long sender_buf = 0;
  char type_buf[64];
  unsigned long type_len = 0;
  char content_buf[4096];
  unsigned long content_len = 0;
  long long ts_buf = 0;
  char client_msg_id_buf[128];
  unsigned long client_msg_id_len = 0;

  result[0].buffer_type = MYSQL_TYPE_STRING;
  result[0].buffer = session_buf;
  result[0].buffer_length = sizeof(session_buf);
  result[0].length = &session_len;

  result[1].buffer_type = MYSQL_TYPE_LONGLONG;
  result[1].buffer = &seq_buf;

  result[2].buffer_type = MYSQL_TYPE_LONGLONG;
  result[2].buffer = &sender_buf;

  result[3].buffer_type = MYSQL_TYPE_STRING;
  result[3].buffer = type_buf;
  result[3].buffer_length = sizeof(type_buf);
  result[3].length = &type_len;

  result[4].buffer_type = MYSQL_TYPE_STRING;
  result[4].buffer = content_buf;
  result[4].buffer_length = sizeof(content_buf);
  result[4].length = &content_len;

  result[5].buffer_type = MYSQL_TYPE_LONGLONG;
  result[5].buffer = &ts_buf;

  result[6].buffer_type = MYSQL_TYPE_STRING;
  result[6].buffer = client_msg_id_buf;
  result[6].buffer_length = sizeof(client_msg_id_buf);
  result[6].length = &client_msg_id_len;

  if (mysql_stmt_bind_result(stmt, result) != 0) {
    if (err_msg) *err_msg = mysql_stmt_error(stmt);
    mysql_stmt_close(stmt);
    return false;
  }

  while (true) {
    int fetch_ret = mysql_stmt_fetch(stmt);
    if (fetch_ret == MYSQL_NO_DATA) break;
    if (fetch_ret != 0 && fetch_ret != MYSQL_DATA_TRUNCATED) {
      if (err_msg) *err_msg = mysql_stmt_error(stmt);
      mysql_stmt_close(stmt);
      return false;
    }
    Message msg;
    msg.session_id.assign(session_buf, session_len);
    msg.msg_seq = seq_buf;
    msg.sender_id = sender_buf;
    msg.msg_type.assign(type_buf, type_len);
    msg.content_json.assign(content_buf, content_len);
    msg.timestamp_ms = ts_buf;
    msg.client_msg_id.assign(client_msg_id_buf, client_msg_id_len);
    msg.msg_id = msg.session_id + "-" + std::to_string(msg.msg_seq);
    messages->push_back(std::move(msg));
  }
  mysql_stmt_close(stmt);
  // 目前查询按倒序返回，需翻转成升序
  std::reverse(messages->begin(), messages->end());
  return true;
}

}  // namespace sparkpush


