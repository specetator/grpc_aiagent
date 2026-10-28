#pragma once
#include <nlohmann/json.hpp>
#include <cstdint>
#include <set>
#include <string>

namespace sparkpush {
// Only Logic-generated control records for explicit Native contacts can bypass
// FIFO. Router still enforces the authenticated session/owner and approval token.
inline bool IsNativeInterrupt(const std::string& value, const std::set<int64_t>& contacts) {
    const auto request = nlohmann::json::parse(value, nullptr, false);
    if (!request.is_object() || request.value("call_model", nlohmann::json()) != false ||
        !request.contains("bot_user_id") || !request["bot_user_id"].is_number_integer() ||
        !contacts.count(request["bot_user_id"].get<int64_t>()) ||
        !request.contains("command") || !request["command"].is_object()) return false;
    const auto& command = request["command"];
    const auto name = command.value("name", nlohmann::json());
    return command.value("local_error", nlohmann::json("")) == "" &&
           (name == "stop" || name == "approve" || name == "deny" || name == "status");
}
}  // namespace sparkpush
