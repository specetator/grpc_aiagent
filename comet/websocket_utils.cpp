#include "websocket_utils.h"

#include <cctype>

namespace sparkpush {

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
    if (!to_user_id) return false;
    // 通过字符串查找 + 数字提取，不依赖第三方 JSON 库，适合 demo 场景
    auto findNumberField = [&](const std::string& key, int64_t* out) -> bool {
        auto pos = json.find("\"" + key + "\"");
        if (pos == std::string::npos) return false;
        pos = json.find(":", pos);
        if (pos == std::string::npos) return false;
        ++pos;
        while (pos < json.size() &&
                      (json[pos] == ' ' || json[pos] == '\t')) {
            ++pos;
        }
        size_t end = pos;
        while (end < json.size() &&
                      std::isdigit(static_cast<unsigned char>(json[end]))) {
            ++end;
        }
        if (end == pos) return false;
        *out = std::stoll(json.substr(pos, end - pos));
        return true;
    };

    return findNumberField("to_user_id", to_user_id);
}

bool ParseChatroomJson(const std::string& json, int64_t* room_id) {
    if (!room_id) return false;
    // 兼容 group_id 与 room_id 两种字段名称。
    auto findNumberField = [&](const std::string& key, int64_t* out) -> bool {
        auto pos = json.find("\"" + key + "\"");
        if (pos == std::string::npos) return false;
        pos = json.find(":", pos);
        if (pos == std::string::npos) return false;
        ++pos;
        while (pos < json.size() &&
                      (json[pos] == ' ' || json[pos] == '\t')) {
            ++pos;
        }
        size_t end = pos;
        while (end < json.size() &&
                      std::isdigit(static_cast<unsigned char>(json[end]))) {
            ++end;
        }
        if (end == pos) return false;
        *out = std::stoll(json.substr(pos, end - pos));
        return true;
    };

    // 先尝试 group_id，再尝试 room_id
    if (findNumberField("group_id", room_id)) return true;
    if (findNumberField("room_id", room_id)) return true;
    return false;
}

// 通用解析：提取 type / to_user_id / group_id。
bool ParseUpstreamMessage(const std::string& json, UpstreamMessageMeta* meta) {
    if (!meta) return false;

    // 以极简方式提取字段，忽略转义/数组等复杂场景
    auto findNumberField = [&](const std::string& key, int64_t* out) -> bool {
        auto pos = json.find("\"" + key + "\"");
        if (pos == std::string::npos) return false;
        pos = json.find(":", pos);
        if (pos == std::string::npos) return false;
        ++pos;
        while (pos < json.size() &&
                      (json[pos] == ' ' || json[pos] == '\t')) {
            ++pos;
        }
        size_t end = pos;
        while (end < json.size() &&
                      std::isdigit(static_cast<unsigned char>(json[end]))) {
            ++end;
        }
        if (end == pos) return false;
        *out = std::stoll(json.substr(pos, end - pos));
        return true;
    };

    auto findStringField = [&](const std::string& key, std::string* out) -> bool {
        auto pos = json.find("\"" + key + "\"");
        if (pos == std::string::npos) return false;
        pos = json.find(":", pos);
        if (pos == std::string::npos) return false;
        ++pos;
        while (pos < json.size() &&
                      (json[pos] == ' ' || json[pos] == '\t')) {
            ++pos;
        }
        if (pos >= json.size() || json[pos] != '"') return false;
        ++pos;
        size_t end = pos;
        while (end < json.size() && json[end] != '"') {
            // 简化：不处理转义
            ++end;
        }
        if (end >= json.size()) return false;
        *out = json.substr(pos, end - pos);
        return true;
    };

    UpstreamMessageMeta m;
    findStringField("type", &m.type);
    findStringField("client_msg_id", &m.client_msg_id);
    findNumberField("to_user_id", &m.to_user_id);
    // group_id / room_id 任选其一
    if (!findNumberField("group_id", &m.group_id)) {
        findNumberField("room_id", &m.group_id);
    }

    // 至少需要有 type 或一个路由字段才认为是合法消息
    if (m.type.empty() && m.to_user_id <= 0 && m.group_id <= 0) {
        return false;
    }
    *meta = m;
    return true;
}

}  // namespace sparkpush



