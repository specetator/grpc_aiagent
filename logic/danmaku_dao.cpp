#include "danmaku_dao.h"

#include <mysql/mysql.h>

#include <sstream>

#include "logging.h"

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

// 功能：写入一条弹幕记录到 video_danmaku。
// 注意本方法用的是占位符绑定（与下面 ListDanmaku 的字符串拼接不同），
// 因此写入路径没有注入面，也没有缓冲区长度问题。
// timestamp_ms 由本函数自己取当前时间，不接受调用方传入：
// 弹幕的时间轴位置是 timeline_ms（视频内偏移），而 timestamp_ms 是发送时刻，
// 两者语义不同，后者必须由服务端定，否则客户端能伪造发送时间。
bool DanmakuDao::InsertDanmaku(const std::string& video_id, int64_t timeline_ms,
                               int64_t sender_id,
                               const std::string& content_json, int64_t* out_id,
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
        LOG_ERROR << e;
        if (err_msg) *err_msg = e;
        return false;
    }

    if (mysql_stmt_prepare(stmt, sql,
                           static_cast<unsigned long>(strlen(sql))) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "InsertDanmaku prepare failed: " << e;
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
    // 发送时刻由服务端自取，不从入参里拿。time() 只有秒级精度，
    // 乘 1000 是为了对齐其他表的毫秒时间戳；同一秒内的多条弹幕
    // 因此会得到相同的 timestamp_ms，区分先后只能靠自增 id。
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
        LOG_ERROR << "InsertDanmaku bind_param failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (mysql_stmt_execute(stmt) != 0) {
        std::string e = mysql_stmt_error(stmt);
        LOG_ERROR << "InsertDanmaku execute failed: " << e;
        if (err_msg) *err_msg = e;
        mysql_stmt_close(stmt);
        return false;
    }

    if (out_id) {
        // 在 close 之前取：insert_id 属于连接，guard 析构后连接会被复用。
        *out_id = static_cast<int64_t>(mysql_insert_id(conn));
    }

    mysql_stmt_close(stmt);
    return true;
}

// 功能：查询弹幕列表，支持时间范围与数量限制。
//
// 本方法是工程里少数用字符串拼接 SQL 的地方之一（另一个是 group_dao.cpp），
// 与其他 DAO 一律用 mysql_stmt_* 占位符绑定的做法不一致。
bool DanmakuDao::ListDanmaku(const std::string& video_id,
                             std::vector<DanmakuItem>* out_list,
                             std::string* err_msg, int64_t from_ms,
                             int64_t to_ms, int limit) {
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

    // 已知缺陷：固定 256 字节栈缓冲区。mysql_real_escape_string 的输出
    // 最长是 2*length+1，因此 video_id 长度 >= 128 时会写穿这个缓冲区。
    // 调用链上只校验了 video_id.empty()，没有限长（schema 里是 VARCHAR(128)，
    // 但 SELECT 路径不受列宽约束）。正确做法是改用占位符绑定，
    // 或者把缓冲区改成 std::string 并用 mysql_real_escape_string 的返回值扩容。
    char escaped_vid[256];
    mysql_real_escape_string(conn, escaped_vid, video_id.c_str(),
                             video_id.length());

    std::ostringstream oss;
    oss << "SELECT id, video_id, timeline_ms, sender_id, content_json, "
           "timestamp_ms "
        << "FROM video_danmaku WHERE video_id='" << escaped_vid << "'";

    // 按时间范围过滤：闭开区间 [from_ms, to_ms)
    // 两个条件都是可选的：<=0 当作「没传」。to_ms > from_ms 的额外判定
    // 是为了避开拼出一个永假条件（timeline_ms>=100 AND timeline_ms<50），
    // 那种查询会白白扫一遍索引再返回空集。
    if (from_ms > 0) {
        oss << " AND timeline_ms>=" << from_ms;
    }
    if (to_ms > 0 && to_ms > from_ms) {
        oss << " AND timeline_ms<" << to_ms;
    }

    // 只处理了 <=0，**没有上限钳制**：HTTP 层的 limit 直接来自客户端 JSON，
    // DanmakuService::List 也是原样透传（只对 from/to 做了 max(0,...)）。
    if (limit <= 0) {
        limit = 5000;
    }

    // timeline_ms ASC 配合上面的 (video_id, timeline_ms) 复合索引，
    // 让排序能直接走索引顺序而不需要 filesort。
    oss << " ORDER BY timeline_ms ASC LIMIT " << limit;

    std::string sql = oss.str();
    if (mysql_query(conn, sql.c_str()) != 0) {
        if (err_msg) *err_msg = mysql_error(conn);
        LOG_ERROR << "ListDanmaku query failed: " << std::string(mysql_error(conn));
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
