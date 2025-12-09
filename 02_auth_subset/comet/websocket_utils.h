// ============================================================================
// WebSocket 帧构造工具函数
// 
// 提供服务端发送 WebSocket 消息的辅助函数
// 
// 说明：
// - 服务端发送的帧不需要掩码（MASK=0），客户端发送的帧必须有掩码
// - 当前仅实现文本帧构造，二进制帧、ping/pong 等可类似实现
// - 简化实现，不支持分片（所有消息一次发送完）
// ============================================================================
#pragma once

#include <string>

namespace sparkpush {

// 构造 WebSocket 文本帧（服务端向客户端发送）
// 
// @param payload: 消息内容（UTF-8 文本）
// @return: 完整的 WebSocket 帧（包含帧头和负载）
// 
// 帧格式：
// - FIN=1：完整消息，不分片
// - Opcode=0x1：文本帧
// - MASK=0：服务端发送无需掩码
// - Payload Length：根据消息长度自动选择 1/3/9 字节表示
// 
// 用途：服务端主动推送消息给客户端（如聊天消息、通知等）
std::string BuildWebSocketTextFrame(const std::string& payload);

}  // namespace sparkpush
