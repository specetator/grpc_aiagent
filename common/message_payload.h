#pragma once
#include <nlohmann/json.hpp>
#include "spark_push.pb.h"

namespace sparkpush {
// Metadata comes from the stored message, including legacy rows whose content lacks a WS envelope.
inline std::string DownstreamMessageJson(const ChatMessage& message) {
    auto body = nlohmann::json::parse(message.content_json(), nullptr, false);
    if (!body.is_object()) {
        const std::string text = body.is_string() ? body.get<std::string>() : message.content_json();
        body = {{"type", message.session_id().rfind("r_", 0) == 0 ? "chatroom" : "single_chat"},
                {"content", {{"text", text}}}};
    }
    body["msg_id"] = message.msg_id();
    body["session_id"] = message.session_id();
    body["msg_seq"] = message.msg_seq();
    body["create_time"] = message.timestamp_ms();
    body["from_user_id"] = message.sender_id();
    body["client_msg_id"] = message.client_msg_id();
    return body.dump();
}
}  // namespace sparkpush
