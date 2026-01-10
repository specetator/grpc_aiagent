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

#include <cstdint>
#include <string>
namespace sparkpush {

// 上行消息（客户端 -> 服务端）的关键字段抽取结果。
// 目的：在 comet 层做轻量校验与路由信息提取，然后转交 logic 处理。
struct UpstreamMessageMeta {
    std::string msg_type;  // 消息类型（例如 text/image/...，具体由业务定义）
    std::string target_type;  // 目标类型：single_chat / room（当前 comet 主要用
                              // single_chat）
    int64_t target_id{0};  // 目标 ID：对端用户 ID 或房间 ID
    std::string client_msg_id;  // 客户端消息唯一 ID（用于幂等/重试/对账）
    int64_t client_ts_ms{0};  // 客户端时间戳（毫秒，可选）
};

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

// 解析客户端上行消息，抽取 msg_type/target_type/target_id 等元信息。
// 注意：该函数使用 nlohmann::json 做“宽松解析”，字段缺失或类型不符会返回
// false。
bool ParseUpstreamMessage(const std::string& json, UpstreamMessageMeta* meta);

}  // namespace sparkpush
