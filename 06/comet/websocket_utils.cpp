#include "websocket_utils.h"

#include <nlohmann/json.hpp>

namespace sparkpush {

std::string BuildWebSocketTextFrame(const std::string& payload) {
  std::string frame;
  unsigned char b1 = 0x81;  // FIN=1, text frame
  frame.push_back(static_cast<char>(b1));
  size_t len = payload.size();
  if (len < 126) {
    frame.push_back(static_cast<char>(len));
  } else if (len <= 0xFFFF) {
    frame.push_back(126);
    frame.push_back(static_cast<char>((len >> 8) & 0xFF));
    frame.push_back(static_cast<char>(len & 0xFF));
  } else {
    frame.push_back(127);
    for (int i = 7; i >= 0; --i) {
      frame.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
    }
  }
  frame.append(payload);
  return frame;
}

std::string BuildWebSocketPingFrame() {
  // FIN=1, opcode=0x9 (ping), payload length=0
  std::string frame;
  frame.push_back(static_cast<char>(0x89));
  frame.push_back(static_cast<char>(0x00));
  return frame;
}

std::string BuildWebSocketPongFrame() {
  // FIN=1, opcode=0xA (pong), payload length=0
  std::string frame;
  frame.push_back(static_cast<char>(0x8A));
  frame.push_back(static_cast<char>(0x00));
  return frame;
}

// ---- 基于 nlohmann::json 的安全解析 ----

// 安全提取整数字段，处理类型不匹配和溢出
static bool SafeGetInt64(const nlohmann::json& j, const std::string& key, int64_t* out) {
  auto it = j.find(key);
  if (it == j.end()) return false;
  if (it->is_number_integer()) {
    *out = it->get<int64_t>();
    return true;
  }
  if (it->is_number_unsigned()) {
    *out = static_cast<int64_t>(it->get<uint64_t>());
    return true;
  }
  // 兼容：客户端可能把数字传成字符串 "123"
  if (it->is_string()) {
    try {
      *out = std::stoll(it->get<std::string>());
      return true;
    } catch (...) {
      return false;
    }
  }
  return false;
}

// 安全提取字符串字段
static bool SafeGetString(const nlohmann::json& j, const std::string& key, std::string* out) {
  auto it = j.find(key);
  if (it == j.end() || !it->is_string()) return false;
  *out = it->get<std::string>();
  return true;
}

bool ParseSingleChatJson(const std::string& json_str, int64_t* to_user_id) {
  if (!to_user_id) return false;
  try {
    auto j = nlohmann::json::parse(json_str);
    return SafeGetInt64(j, "to_user_id", to_user_id);
  } catch (const nlohmann::json::parse_error&) {
    return false;
  }
}

bool ParseChatroomJson(const std::string& json_str, int64_t* room_id) {
  if (!room_id) return false;
  try {
    auto j = nlohmann::json::parse(json_str);
    if (SafeGetInt64(j, "group_id", room_id)) return true;
    if (SafeGetInt64(j, "room_id", room_id)) return true;
    return false;
  } catch (const nlohmann::json::parse_error&) {
    return false;
  }
}

bool ParseUpstreamMessage(const std::string& json_str, UpstreamMessageMeta* meta) {
  if (!meta) return false;
  try {
    auto j = nlohmann::json::parse(json_str);

    UpstreamMessageMeta m;
    SafeGetString(j, "type", &m.type);
    SafeGetString(j, "client_msg_id", &m.client_msg_id);
    SafeGetInt64(j, "to_user_id", &m.to_user_id);
    // group_id / room_id 任选其一
    if (!SafeGetInt64(j, "group_id", &m.group_id)) {
      SafeGetInt64(j, "room_id", &m.group_id);
    }
    // 弹幕字段
    SafeGetString(j, "video_id", &m.video_id);
    SafeGetInt64(j, "timeline_ms", &m.timeline_ms);

    // 至少需要有 type 或一个路由字段才认为是合法消息
    if (m.type.empty() && m.to_user_id <= 0 && m.group_id <= 0 && m.video_id.empty()) {
      return false;
    }
    *meta = m;
    return true;
  } catch (const nlohmann::json::parse_error&) {
    return false;
  }
}

}  // namespace sparkpush



