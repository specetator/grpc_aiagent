#pragma once

#include <cstdint>
#include <string>

namespace sparkpush {

// 上行消息的解析结果（仅保留路由相关的关键信息）。
// 解析时尽量不依赖第三方 JSON，以最小依赖适配 demo。
//
// 为何只存这几个字段：Comet 的职责止于「知道该把这条消息转给谁」。
// 正文（content）不做解析，而是把整个原始 payload 直接放进
// UpstreamMessageRequest.content_json 透传给 Logic。这样 Comet 不需要
// 随业务字段演进而修改，新增消息类型时只需改 Logic。
struct UpstreamMessageMeta {
    std::string type;     // single_chat / chatroom / chatroom_join / chatroom_leave / ...
    int64_t to_user_id{0};
    int64_t group_id{0};  // 群聊 / 聊天室 room_id
    // 幂等键，由客户端生成并保证重试时不变。Comet 只透传，
    // 真正的去重在 Logic 侧的 Redis Lua 里完成。
    std::string client_msg_id;
};

// 构造极简 WebSocket 文本帧（仅用于 demo，未处理分片等复杂情况）。
std::string BuildWebSocketTextFrame(const std::string& payload);

// 从上行 JSON 中解析单聊目标 user_id：
// 示例：{"type":"single_chat","to_user_id":123,"client_msg_id":"...","content":{...}}
// 只解析数字字段，原始 JSON 透传给逻辑层。
bool ParseSingleChatJson(const std::string& json, int64_t* to_user_id);

// 从上行 JSON 中解析聊天室 room_id / group_id。
// 示例：{"type":"chatroom","group_id":456,...} 或 {"type":"chatroom_leave","room_id":456}
bool ParseChatroomJson(const std::string& json, int64_t* room_id);

// 通用解析：读取 type / to_user_id / group_id，便于 comet 根据 type 做路由/控制决策。
// 轻量级实现，不支持转义/嵌套数组等复杂 JSON 结构。
//
// 返回 false 意味着帧格式不合法，调用方应回 error 帧并丢弃该消息。
// 具体的已知局限见 .cpp 里 FindNumberField 的注释。
bool ParseUpstreamMessage(const std::string& json, UpstreamMessageMeta* meta);

}  // namespace sparkpush



