// ============================================================================
// WebSocket 帧构造工具实现
// ============================================================================
#include "websocket_utils.h"

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

}  // namespace sparkpush
