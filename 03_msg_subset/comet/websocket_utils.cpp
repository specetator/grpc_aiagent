// ============================================================================
// WebSocket 帧构造工具实现
// ============================================================================
#include "websocket_utils.h"

#include <cctype>
#include "logging.h"

namespace sparkpush {

std::string BuildWebSocketTextFrame(const std::string& payload) {
    std::string frame;
    unsigned char b1 = 0x81;
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

bool ParseSingleChatJson(const std::string& json, int64_t* to_user_id) {
    if (!to_user_id) return false;
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

    if (findNumberField("group_id", room_id)) return true;
    if (findNumberField("room_id", room_id)) return true;
    return false;
}

bool ParseUpstreamMessage(const std::string& json, UpstreamMessageMeta* meta) {
    if (!meta) return false;

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
    if (!findNumberField("group_id", &m.group_id)) {
        findNumberField("room_id", &m.group_id);
    }

    // Debug log
    LOG_INFO << "ParseUpstreamMessage: type=" << m.type 
             << ", to_user_id=" << m.to_user_id 
             << ", group_id=" << m.group_id 
             << ", client_msg_id=" << m.client_msg_id;

    if (m.type.empty() && m.to_user_id <= 0 && m.group_id <= 0) {
        LOG_ERROR << "ParseUpstreamMessage FAILED: empty type AND no user/group";
        return false;
    }
    *meta = m;
    return true;
}

}  // namespace sparkpush

