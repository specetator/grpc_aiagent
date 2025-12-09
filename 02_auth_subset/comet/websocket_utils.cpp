#include "websocket_utils.h"

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

}  // namespace sparkpush
