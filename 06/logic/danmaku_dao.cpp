#include "danmaku_dao.h"

#include "logging.h"

#include <sstream>
#include <mysql/mysql.h>

namespace sparkpush {

// 参考 sql/schema.sql 中的表结构：
// CREATE TABLE IF NOT EXISTS `video_danmaku` (
//   `id` BIGINT NOT NULL AUTO_INCREMENT,
//   `video_id` VARCHAR(128) NOT NULL,
//   `timeline_ms` BIGINT NOT NULL,
//   `sender_id` BIGINT NOT NULL,
//   `content_json` TEXT NOT NULL,
//   `timestamp_ms` BIGINT NOT NULL,
//   PRIMARY KEY (`id`),
//   KEY `idx_video_timeline` (`video_id`, `timeline_ms`),
//   KEY `idx_sender` (`sender_id`)
// );

bool DanmakuDao::InsertDanmaku(const std::string& video_id,
                               int64_t timeline_ms,
                               int64_t sender_id,
                               const std::string& content_json,
                               int64_t* out_id,
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
      "INSERT INTO video_danmaku("
      "video_id, timeline_ms, sender_id, content_json, timestamp_ms"
      ") VALUES(?, ?, ?, ?, ?)";

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
    LogError("InsertDanmaku prepare failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  MYSQL_BIND bind[5];
  memset(bind, 0, sizeof(bind));

  unsigned long video_len = static_cast<unsigned long>(video_id.size());
  unsigned long content_len = static_cast<unsigned long>(content_json.size());
  long long timeline_param = timeline_ms;
  long long sender_param = sender_id;
  long long ts_param = static_cast<long long>(time(nullptr)) * 1000LL;

  bind[0].buffer_type = MYSQL_TYPE_STRING;
  bind[0].buffer = const_cast<char*>(video_id.data());
  bind[0].buffer_length = video_len;
  bind[0].length = &video_len;

  bind[1].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[1].buffer = &timeline_param;
  bind[1].is_unsigned = 0;

  bind[2].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[2].buffer = &sender_param;
  bind[2].is_unsigned = 0;

  bind[3].buffer_type = MYSQL_TYPE_STRING;
  bind[3].buffer = const_cast<char*>(content_json.data());
  bind[3].buffer_length = content_len;
  bind[3].length = &content_len;

  bind[4].buffer_type = MYSQL_TYPE_LONGLONG;
  bind[4].buffer = &ts_param;
  bind[4].is_unsigned = 0;

  if (mysql_stmt_bind_param(stmt, bind) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("InsertDanmaku bind_param failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  if (mysql_stmt_execute(stmt) != 0) {
    std::string e = mysql_stmt_error(stmt);
    LogError("InsertDanmaku execute failed: " + e);
    if (err_msg) *err_msg = e;
    mysql_stmt_close(stmt);
    return false;
  }

  if (out_id) {
    *out_id = static_cast<int64_t>(mysql_insert_id(conn));
  }

  mysql_stmt_close(stmt);
  return true;
}

bool DanmakuDao::ListDanmaku(const std::string& video_id,
                             std::vector<DanmakuItem>* out_list,
                             std::string* err_msg,
                             int64_t from_ms,
                             int64_t to_ms,
                             int limit) {
  if (!out_list) return false;
  out_list->clear();
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

  char escaped_vid[256];
  mysql_real_escape_string(conn, escaped_vid, video_id.c_str(), video_id.length());

  std::ostringstream oss;
  oss << "SELECT id, video_id, timeline_ms, sender_id, content_json, timestamp_ms "
      << "FROM video_danmaku WHERE video_id='" << escaped_vid << "'";

  // 按时间范围过滤：闭开区间 [from_ms, to_ms)
  if (from_ms > 0) {
    oss << " AND timeline_ms>=" << from_ms;
  }
  if (to_ms > 0 && to_ms > from_ms) {
    oss << " AND timeline_ms<" << to_ms;
  }

  if (limit <= 0) {
    limit = 5000;
  }

  oss << " ORDER BY timeline_ms ASC LIMIT " << limit;

  std::string sql = oss.str();
  if (mysql_query(conn, sql.c_str()) != 0) {
    if (err_msg) *err_msg = mysql_error(conn);
    LogError("ListDanmaku query failed: " + std::string(mysql_error(conn)));
    return false;
  }

  MYSQL_RES* res = mysql_store_result(conn);
  if (!res) {
    if (err_msg) *err_msg = "store_result failed";
    return false;
  }

  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    DanmakuItem item;
    item.id = row[0] ? std::stoll(row[0]) : 0;
    item.video_id = row[1] ? row[1] : "";
    item.timeline_ms = row[2] ? std::stoll(row[2]) : 0;
    item.sender_id = row[3] ? std::stoll(row[3]) : 0;
    item.content_json = row[4] ? row[4] : "";
    item.timestamp_ms = row[5] ? std::stoll(row[5]) : 0;
    out_list->push_back(item);
  }
  mysql_free_result(res);
  return true;
}

}  // namespace sparkpush


