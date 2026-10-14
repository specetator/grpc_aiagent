#include "websocket_utils.h"

#include <charconv>
#include <cctype>

namespace sparkpush {
namespace {

// 从 JSON 文本里直接取一个整数字段，不做完整解析。
//
// 为何不用 nlohmann::json：这在**每条上行消息**上都会跑，而完整
// 解析一个带富文本正文的 JSON 开销不小。Comet 只需要几个路由字段，
// 正文本身是原封不动透传给 Logic 的（Logic 那边才做完整解析）。
//
// 已知局限（读代码时必须知道）：json.find 搜的是整个文本，因此
// 若客户端把 "to_user_id" 写在嵌套的 content 里而顶层没有，
// 会从 content 里拿到值。这不构成安全漏洞（发送者身份由
// Logic 用 auth_token 二次校验，to_user_id 只决定收件人），
// 但意味着字段位置不规范时行为不直观。
//
// value <= 0 一律当失败：本项目的 user_id/room_id 都是正整数，
// 0 与负数没有合法含义，提前拒掉能让上层给出明确错误。
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
    // 只扫描连续数字：不接受负号、小数点、科学计数法。
    // 这使得 "123abc" 会在下面的 from_chars 校验里被拒。
    size_t end = pos;
    while (end < json.size() &&
           std::isdigit(static_cast<unsigned char>(json[end]))) {
        ++end;
    }
    if (end == pos) return false;
    int64_t value = 0;
    // 用 from_chars 而不是 stoll：后者会抛异常且会接受前导空格与 '+'，
    // 在热路径上抛异常代价很高。
    const auto result =
        std::from_chars(json.data() + pos, json.data() + end, value);
    // 三重校验：无错误码 + 消费到了预期位置 + 值为正。
    // result.ptr != end 能挡住溢出（from_chars 溢出时不会消费完全部字符）。
    if (result.ec != std::errc{} || result.ptr != json.data() + end ||
        value <= 0) {
        return false;
    }
    *out = value;
    return true;
}

// 从 JSON 文本里取一个字符串字段。
//
// 遇到反斜杠转义直接返回 false（而不是尝试解析）：这是一个有意的
// 保守选择。支持转义就要处理 \" \\ \u1234 等所有情况，复杂度大幅
// 上升；而这里要取的两个字段（type、client_msg_id）按约定都不应
// 包含需要转义的字符。宁可解析失败让客户端重发，也不要错解。
//
// 对 client_msg_id 而言这个限制是合理的：它应该是 UUID 这类纯 ASCII id。
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

// 构造下行的 WebSocket 文本帧。服务端发的帧**不加掩码**（RFC6455 规定
// 只有客户端->服务端的帧必须 mask）。
//
// 长度字段的三档编码是协议要求，容易写错：
//   len < 126        直接放在第二字节的低 7 位
//   len <= 0xFFFF    第二字节写 126，后跟 2 字节大端长度
//   更大            第二字节写 127，后跟 8 字节大端长度
// 注意 126/127 是「转义标记」而不是真实长度，后续字节一律大端序。
//
// 不做分片（总是 FIN=1）：下行的消息帧最大只有几十 KB（正文上限
// 64 KiB + 附件引用），单帧足够，分片只会让客户端去重逻辑变复杂。
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
//
// 本函数是 Comet 上行路由的唯一入口，三重校验缺一不可：
//   1) single_chat 必须有正的 to_user_id
//   2) chatroom* 必须有正的 group_id
//   3) type 必须在四个已知值里（白名单）
// 任一不满足就返回 false，OnTextMessage 会给客户端回
// "invalid message format"。
//
// 注意单个字段的 Find* 失败不会立即返回：它们只是把值留在默认的 0，
// 真正的判定集中在末尾。这样能统一给出「格式不对」而不是
// 因为字段顺序不同而报不一样的错。
//
// 这里只做形式校验，**不做权限校验**：发送者身份、会话成员资格、
// 附件所有权全部由 Logic 的 Submit 负责。Comet 不持有这些权威数据。
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

