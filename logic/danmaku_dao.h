#pragma once

#include <string>
#include <vector>

#include "model.h"
#include "mysql_pool.h"

namespace sparkpush {

// 弹幕 DAO：操作 video_danmaku 表。
//
// 弹幕的**持久化**不走消息 outbox：不占 msg_seq、不写 message_reservation、
// 直接 INSERT 进 video_danmaku。但**投递**仍然走 Kafka：
// DanmakuService 会把 PushToCometRequest 写进 group push topic 扇出到 Comet。
// 这个不对称是有意的：弹幕容忍丢失（回放时可重拉）但容忍不了延迟，
// 因此不值得为它付一次 outbox 事务的代价。
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
    //
    // 安全注意（阅读实现前先看这里）：本方法与 group_dao.cpp 是工程里
    // **仅有的两处用字符串拼接而不是占位符绑定**的 DAO。
    // video_id 靠 mysql_real_escape_string 转义后直接嵌进 SQL，
    // 而转义结果写在一个 256 字节的栈缓冲区里。
    // mysql_real_escape_string 最坏情况下会把每个字节变成两个，
    // 因此 video_id 长度 >= 128 时就会写穿缓冲区。
    // 而调用链（HTTP /danmaku/list -> DanmakuService::List -> 本方法）
    // 对 video_id 只做了 empty() 检查，没有长度上限。
    // 与 group_dao 不同的是：这里的转义至少让 SQL 注入不成立，
    // 风险是内存安全而不是注入。修改时请优先改成占位符绑定；
    // 若暂时保留拼接，必须先在入口加上 video_id.size() < 128 的硬校验。
    //
    // limit 同样没有上限钳制（<=0 时退化为 5000），客户端可以传任意大值。
    bool ListDanmaku(const std::string& video_id, std::vector<DanmakuItem>* out_list,
                     std::string* err_msg, int64_t from_ms = 0, int64_t to_ms = 0,
                     int limit = 5000);

   private:
    MySqlConnectionPool* pool_;
};

}  // namespace sparkpush
