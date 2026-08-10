#include "websocket_utils.h"

#include <charconv>
#include <cctype>

namespace sparkpush {
namespace {

bool FindNumberField(const std::string& json, const std::string& key,
                     int64_t* out) {
    if (!out) return false;
    auto pos = json.find("\"" + key + "\"");
    if (pos == std::string::npos) return false;
    pos = json.find(':', pos);
    if (pos == std::string::npos) return false;
    ++pos;
    while (pos < json.size() &&
           std::isspace(static_cast<unsigned char>(json[pos]))) {
        ++pos;
    }
    size_t end = pos;
    while (end < json.size() &&
           std::isdigit(static_cast<unsigned char>(json[end]))) {
        ++end;
    }
    if (end == pos) return false;
    int64_t value = 0;
    const auto result =
        std::from_chars(json.data() + pos, json.data() + end, value);
    if (result.ec != std::errc{} || result.ptr != json.data() + end ||
        value <= 0) {
        return false;
    }
    *out = value;
    return true;
}

bool FindStringField(const std::string& json, const std::string& key,
                     std::string* out) {
    if (!out) return false;
    auto pos = json.find("\"" + key + "\"");
    if (pos == std::string::npos) return false;
    pos = json.find(':', pos);
    if (pos == std::string::npos) return false;
    ++pos;
    while (pos < json.size() &&
           std::isspace(static_cast<unsigned char>(json[pos]))) {
        ++pos;
    }
    if (pos >= json.size() || json[pos] != '"') return false;
    ++pos;
    size_t end = pos;
    while (end < json.size() && json[end] != '"') {
        if (json[end] == '\\') return false;  // 轻量解析器明确拒绝转义字段
        ++end;
    }
    if (end >= json.size()) return false;
    *out = json.substr(pos, end - pos);
    return true;
}

}  // namespace

std::string BuildWebSocketTextFrame(const std::string& payload) {
    // 构造最小化的 WebSocket 文本帧：仅支持 FIN=1 且不分片。
    // 服务端发送的帧无需掩码，直接按 RFC6455 规则拼接长度字段 + 负载。
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

// 极简 JSON 解析器：从文本中提取数字字段。
bool ParseSingleChatJson(const std::string& json, int64_t* to_user_id) {
    return FindNumberField(json, "to_user_id", to_user_id);
}

bool ParseChatroomJson(const std::string& json, int64_t* room_id) {
    // 先尝试 group_id，再尝试 room_id
    if (FindNumberField(json, "group_id", room_id)) return true;
    if (FindNumberField(json, "room_id", room_id)) return true;
    return false;
}

// 通用解析：提取 type / to_user_id / group_id。
bool ParseUpstreamMessage(const std::string& json, UpstreamMessageMeta* meta) {
    if (!meta) return false;

    UpstreamMessageMeta m;
    FindStringField(json, "type", &m.type);
    FindStringField(json, "client_msg_id", &m.client_msg_id);
    FindNumberField(json, "to_user_id", &m.to_user_id);
    // group_id / room_id 任选其一
    if (!FindNumberField(json, "group_id", &m.group_id)) {
        FindNumberField(json, "room_id", &m.group_id);
    }

    if (m.type == "single_chat" && m.to_user_id <= 0) return false;
    if ((m.type == "chatroom" || m.type == "chatroom_join" ||
         m.type == "chatroom_leave") &&
        m.group_id <= 0) {
        return false;
    }
    if (m.type != "single_chat" && m.type != "chatroom" &&
        m.type != "chatroom_join" && m.type != "chatroom_leave") {
        return false;
    }
    *meta = m;
    return true;
}

}  // namespace sparkpush

