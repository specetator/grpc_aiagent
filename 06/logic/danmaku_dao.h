#pragma once

#include "mysql_pool.h"

#include <string>
#include <vector>

namespace sparkpush {

struct DanmakuItem {
  int64_t id;
  std::string video_id;
  int64_t timeline_ms;
  int64_t sender_id;
  std::string content_json;
  int64_t timestamp_ms;
};

// 弹幕 DAO：操作 video_danmaku 表
class DanmakuDao {
 public:
  explicit DanmakuDao(MySqlConnectionPool* pool) : pool_(pool) {}

  // 插入一条弹幕记录
  // 返回自增主键 id 到 out_id（可选）
  bool InsertDanmaku(const std::string& video_id,
                     int64_t timeline_ms,
                     int64_t sender_id,
                     const std::string& content_json,
                     int64_t* out_id,
                     std::string* err_msg);

  // 获取视频的弹幕，支持按时间范围和数量限制
  // from_ms/to_ms 为时间范围（毫秒），闭开区间 [from_ms, to_ms)，
  // 当为 0 时表示不加该方向的限制；limit<=0 时内部会使用默认上限。
  bool ListDanmaku(const std::string& video_id,
                   std::vector<DanmakuItem>* out_list,
                   std::string* err_msg,
                   int64_t from_ms = 0,
                   int64_t to_ms = 0,
                   int limit = 5000);

 private:
  MySqlConnectionPool* pool_;
};

}  // namespace sparkpush


