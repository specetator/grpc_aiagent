#pragma once
#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
class ConversationStore;
class DanmakuDao;
class GroupMemberDao;
class KafkaProducer;
class RedisStore;
// Send 的复合返回值：状态 + 已落库的消息 + 是否已推送。
// 三个字段必须一起看：pushed=false 但 status.ok() 是合法的
//（实时扇出失败不影响弹幕已写入），调用方不能只判 status。
struct DanmakuResult {
    ServiceStatus status;
    Message message;
    bool pushed{false};
};

// 弹幕发送与查询。
//
// 弹幕走一条与普通 IM 消息**不同**的混合路径，读代码前先建立这个模型：
//   持久化   直接 INSERT 进 video_danmaku（不走消息 outbox）
//   序号     仍然走 ConversationStore::AppendMessage，占房间会话的 msg_seq
//   投递     写 group push topic，由 Comet 扇出
// 也就是说弹幕同时存在于两张表（video_danmaku 与 message），
// 前者服务回放查询，后者服务实时推送与序号轴。
//
// 鉴权方式也与其他 service 不同：它直接收 **token 字符串**而不是 actor id，
// 在 service 内部反查 user_id。这是因为弹幕接口是遗留契约：
// token 放在请求体而不是 Authorization 头（见 http_server.cpp 里的注释）。
class DanmakuService {
   public:
    DanmakuService(ConversationStore* store, DanmakuDao* dao, GroupMemberDao* members,
                   KafkaProducer* producer, RedisStore* redis)
        : store_(store),
          dao_(dao),
          members_(members),
          producer_(producer),
          redis_(redis) {}
    // 发送前的依赖检查。dao_ 与 redis_ 不在其中：
    // dao_ 为空时跳过持久化（只做实时推送），redis_ 为空时退化到成员表扇出。
    ServiceStatus SendAvailable() const;
    // 发一条弹幕。
    //
    // 已知缺陷（修改本方法时请一并处理）：content_json 是用 ostringstream
    // 把 video_id 与 text **原样拼**进去的，没有做 JSON 转义。
    // 一条包含双引号或反斜杠的弹幕会产出非法 JSON，
    // 前端 JSON.parse 直接失败。全链路（http_server -> Send）对 text
    // 只校验了 empty()，既无转义也无长度上限。
    // 正确做法是用 nlohmann::json 构造对象再 dump()，它会自动转义。
    DanmakuResult Send(const std::string& token, const std::string& video,
                       int64_t timeline_ms, const std::string& text);
    // 按视频与时间区间查弹幕（回放用）。limit 透传给 DAO，
    // 而 DAO 层也没有上限钳制（详见 danmaku_dao.h）。
    ServiceStatus List(const std::string& video, int64_t from, int64_t to, int limit,
                       std::vector<DanmakuItem>* result);

   private:
    ConversationStore* store_;
    DanmakuDao* dao_;
    GroupMemberDao* members_;
    KafkaProducer* producer_;
    RedisStore* redis_;
};
}  // namespace sparkpush
