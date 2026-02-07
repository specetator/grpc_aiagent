#pragma once

#include <cstdint>
#include <string>

namespace sparkpush {

// 上行消息的解析结果（仅保留路由相关的关键信息）
struct UpstreamMessageMeta {
  std::string type;     // single_chat / chatroom / chatroom_join / chatroom_leave / danmaku / ...
  int64_t to_user_id{0};
  int64_t group_id{0};  // 群聊 / 聊天室 room_id
  std::string client_msg_id;
  std::string video_id;   // 弹幕视频ID
  int64_t timeline_ms{0}; // 弹幕时间轴（毫秒）
};

// 构造极简 WebSocket 文本帧（仅用于 demo，未处理分片等复杂情况）
std::string BuildWebSocketTextFrame(const std::string& payload);

// 构造 WebSocket Ping 帧（opcode=0x9，服务端发送，无 mask）
std::string BuildWebSocketPingFrame();

// 构造 WebSocket Pong 帧（opcode=0xA，服务端发送，无 mask）
std::string BuildWebSocketPongFrame();

// 从上行 JSON 中解析单聊目标 user_id：
// 期望格式示例：
//   {"type":"single_chat","to_user_id":123,"client_msg_id":"...","content":{...}}
// 这里只关心 to_user_id，其他字段原样透传给 logic。
bool ParseSingleChatJson(const std::string& json, int64_t* to_user_id);

// 从上行 JSON 中解析聊天室 room_id / group_id：
// 期望格式示例：
//   {"type":"chatroom","group_id":456,"client_msg_id":"...","content":{...}}
//   {"type":"chatroom_join","group_id":456}
//   {"type":"chatroom_leave","room_id":456}
// 为了兼容，也支持 "room_id" 字段。
bool ParseChatroomJson(const std::string& json, int64_t* room_id);

// 更通用的上行解析：读取 type / to_user_id / group_id，便于 comet 根据 type 做路由/控制决策
bool ParseUpstreamMessage(const std::string& json, UpstreamMessageMeta* meta);

}  // namespace sparkpush



