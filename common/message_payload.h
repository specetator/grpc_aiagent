#pragma once
#include <nlohmann/json.hpp>
#include "spark_push.pb.h"

namespace sparkpush {

// 把一条已落库的 ChatMessage 渲染成发给客户端的 WebSocket JSON。
//
// Metadata comes from the stored message, including legacy rows whose content lacks a WS envelope.
// 这句话是本函数最关键的设计约束：**元数据以数据库里的列为准**，
// 而不是信任 content_json 自带的字段。原因有两个：
//   1) 历史行可能是旧版写入的，正文里根本没有 msg_id/msg_seq 这些字段；
//   2) 正文是用户可控的，若信任它就能伪造 from_user_id 或 msg_seq。
// 因此下面一律用 body[...] = 赋值（覆盖）而不是 emplace（仅当不存在时写）。
//
// 为何放在 common/ ：Comet 的下推、离线补推、缺口 sync 三条路径都要
// 用同一份渲染逻辑。分头实现会让同一消息在不同路径上长出不同 JSON，
// 客户端去重就会失效。
inline std::string DownstreamMessageJson(const ChatMessage& message) {
    // 第三参 false = 不抛异常，解析失败时返回 discarded。
    auto body = nlohmann::json::parse(message.content_json(), nullptr, false);
    if (!body.is_object()) {
        // 兼容分支：正文不是 JSON object（遗留行存的是裸字符串，或已损坏）。
        // 这时不能直接丢掉消息，而是合成一个最小合法信封：
        //   type 按 session_id 前缀推导（r_ = 聊天室，否则单聊），
        //   text 尽量从可解析的字符串里取，实在不行就用原文。
        // 宁可渲染得粗糙，也不能让一条已落库的历史消息在客户端消失。
        const std::string text = body.is_string() ? body.get<std::string>() : message.content_json();
        body = {{"type", message.session_id().rfind("r_", 0) == 0 ? "chatroom" : "single_chat"},
                {"content", {{"text", text}}}};
    }
    // 权威字段覆盖。client_msg_id 必须在列：客户端靠它把「乐观气泡」
    // 与服务端正式消息对上，并做本地去重。缺了它重连补推时
    // 同一条消息会被渲染成两个气泡。
    body["msg_id"] = message.msg_id();
    body["session_id"] = message.session_id();
    body["msg_seq"] = message.msg_seq();       // 客户端连续游标的依据
    body["create_time"] = message.timestamp_ms();
    body["from_user_id"] = message.sender_id();
    body["sender_id"] = message.sender_id();
    // Historical user-controlled envelopes cannot masquerade as a live delta.
    if (message.msg_seq() > 0) {
        if (message.session_id().rfind("r_", 0) == 0) body["type"] = "chatroom";
        else if (message.session_id().rfind("s_", 0) == 0) body["type"] = "single_chat";
    }
    body["client_msg_id"] = message.client_msg_id();
    return body.dump();
}
}  // namespace sparkpush
