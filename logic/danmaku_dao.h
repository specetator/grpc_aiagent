#pragma once

#include <string>
#include <vector>

#include "mysql_pool.h"

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

    // 功能：插入一条弹幕记录
    // 参数：video_id 视频标识；timeline_ms 时间轴；sender_id 发送者；
    //       content_json JSON 内容；out_id 可选返回自增 id；err_msg 记录错误
    // 返回：成功 true，失败 false
    bool InsertDanmaku(const std::string& video_id, int64_t timeline_ms,
                       int64_t sender_id, const std::string& content_json,
                       int64_t* out_id, std::string* err_msg);

    // 功能：获取视频弹幕列表，支持时间范围与数量限制
    // 参数：video_id 视频标识；out_list 输出结果；err_msg 错误信息；
    //       from_ms 起始毫秒（含）；to_ms 结束毫秒（开）；limit 上限
    // 返回：成功 true，失败 false
    bool ListDanmaku(const std::string& video_id,
                     std::vector<DanmakuItem>* out_list, std::string* err_msg,
                     int64_t from_ms = 0, int64_t to_ms = 0, int limit = 5000);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
