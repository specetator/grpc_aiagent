#pragma once

#include <string>

namespace sparkpush {

// 构造 WebSocket 文本帧（服务端发送）
// 参数：payload 文本内容
// 返回：完整的 WebSocket 帧（FIN=1, Opcode=0x1, MASK=0）
std::string BuildWebSocketTextFrame(const std::string& payload);

}  // namespace sparkpush
