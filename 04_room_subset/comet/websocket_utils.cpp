// ============================================================================
// WebSocket 帧构造工具实现
// ============================================================================
#include "websocket_utils.h"

#include <cctype>
#include <nlohmann/json.hpp>

#include "logging.h"

namespace sparkpush {

// 构造 WebSocket 文本帧
// 按照 RFC 6455 规范组装帧头和负载数据
std::string BuildWebSocketTextFrame(const std::string& payload) {
    std::string frame;

    // 第 1 字节：FIN + RSV + Opcode
    // FIN=1 (0x80): 完整消息，不分片
    // RSV=0: 保留位，必须为 0（除非协商了扩展）
    // Opcode=0x1: 文本帧
    unsigned char b1 = 0x81;  // 0x81 = 10000001
    frame.push_back(static_cast<char>(b1));

    // 第 2 字节及后续：MASK + Payload Length
    // MASK=0: 服务端发送不需要掩码（客户端发送必须为 1）
    // Payload Length: 根据负载大小选择不同的表示方式
    size_t len = payload.size();

    if (len < 126) {
        // 短消息（<126 字节）：长度直接用 7 位表示
        frame.push_back(static_cast<char>(len));
    } else if (len <= 0xFFFF) {
        // 中等消息（126-65535 字节）：用特殊值 126 标记，后跟 2 字节长度
        frame.push_back(126);
        frame.push_back(static_cast<char>((len >> 8) & 0xFF));  // 高字节
        frame.push_back(static_cast<char>(len & 0xFF));         // 低字节
    } else {
        // 长消息（>65535 字节）：用特殊值 127 标记，后跟 8 字节长度
        frame.push_back(127);
        for (int i = 7; i >= 0; --i) {
            frame.push_back(static_cast<char>((len >> (8 * i)) & 0xFF));
        }
    }

    // 追加负载数据（服务端无需掩码处理，直接发送原始数据）
    frame.append(payload);

    return frame;
}

bool ParseSingleChatJson(const std::string& json, int64_t* to_user_id) {
    if (!to_user_id) return false;
    // 轻量解析：避免引入完整 JSON 解析开销（但不保证对所有 JSON 形式都鲁棒）
    // 注意：该实现只处理非负整数数字字段。
    auto findNumberField = [&](const std::string& key, int64_t* out) -> bool {
        auto pos = json.find("\"" + key + "\"");
        if (pos == std::string::npos) return false;
        pos = json.find(":", pos);
        if (pos == std::string::npos) return false;
        ++pos;
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
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
    // 轻量解析 group_id / room_id，规则同 ParseSingleChatJson
    auto findNumberField = [&](const std::string& key, int64_t* out) -> bool {
        auto pos = json.find("\"" + key + "\"");
        if (pos == std::string::npos) return false;
        pos = json.find(":", pos);
        if (pos == std::string::npos) return false;
        ++pos;
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
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

bool ParseUpstreamMessage(const std::string& json_str,
                          UpstreamMessageMeta* meta) {
    if (!meta) return false;
    // 使用 nlohmann::json 做解析：第三个参数 false 表示解析失败不抛异常，
    // 而是返回 discarded，避免异常穿透影响 IO 线程。
    auto j = nlohmann::json::parse(json_str, nullptr, false);
    if (j.is_discarded() || !j.is_object()) {
        LOG_ERROR << "ParseUpstreamMessage FAILED: invalid json";
        return false;
    }

    UpstreamMessageMeta m;
    m.msg_type = j.value("msg_type", "");
    m.target_type = j.value("target_type", "");
    if (j.contains("target_id") && j["target_id"].is_number_integer()) {
        m.target_id = j["target_id"].get<int64_t>();
    }
    if (j.contains("to_user_id") && j["to_user_id"].is_number_integer()) {
        // 兼容字段：单聊常用 to_user_id，统一映射到 target_id
        m.target_id = j["to_user_id"].get<int64_t>();
        if (m.target_type.empty()) m.target_type = "single_chat";
    }
    if (j.contains("room_id") && j["room_id"].is_number_integer()) {
        // 兼容字段：房间消息常用 room_id
        m.target_id = j["room_id"].get<int64_t>();
        if (m.target_type.empty()) m.target_type = "room";
    }
    m.client_msg_id = j.value("client_msg_id", "");
    if (j.contains("timestamp") && j["timestamp"].is_number_integer()) {
        m.client_ts_ms = j["timestamp"].get<int64_t>();
    }

    if (m.msg_type.empty() || m.target_type.empty() || m.target_id <= 0) {
        // 基础必填字段校验：缺失则认为不是合法上行消息
        LOG_ERROR << "ParseUpstreamMessage FAILED: missing required fields";
        return false;
    }
    *meta = m;
    return true;
}

}  // namespace sparkpush
