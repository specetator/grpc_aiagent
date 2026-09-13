#include "hermes_reply.h"

#include <algorithm>
#include <limits>
#include <nlohmann/json.hpp>
#include "citation_validation.h"
#include "agent_event.h"

namespace sparkpush {
namespace {
int64_t PositiveId(const nlohmann::json& value) {
    if (!value.is_number_integer()) return 0;
    if (value.is_number_unsigned() && value.get<uint64_t>() >
        static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) return 0;
    const auto id = value.get<int64_t>();
    return id > 0 ? id : 0;
}
}

bool PrepareHermesReply(const std::string& payload, int64_t bot_user_id,
                       const std::string& bot_name, PreparedHermesReply* out,
                       std::string* error) {
    if (!out || !error) return false;
    *out = {};
    *error = "invalid ai_reply envelope";
    try {
        const auto reply = nlohmann::json::parse(payload);
        if (!reply.is_object() || bot_user_id <= 0) return false;
        const std::string request_id = reply.at("request_id").get<std::string>();
        const int64_t user_id = PositiveId(reply.at("user_id"));
        const int64_t completed_at_ms = PositiveId(reply.at("completed_at_ms"));
        if (request_id.empty() || request_id.size() > 121 || user_id == 0 ||
            user_id == bot_user_id || completed_at_ms == 0 ||
            PositiveId(reply.at("bot_user_id")) != bot_user_id ||
            !reply.at("ok").is_boolean()) return false;
        const std::string session_id = "s_" + std::to_string(std::min(user_id, bot_user_id)) +
            "_" + std::to_string(std::max(user_id, bot_user_id));
        if (reply.at("session_id").get<std::string>() != session_id) return false;
        const bool ok = reply.value("ok", false);
        std::string text = reply.value("text", "");
        if (text.empty()) {
            const std::string bridge_error = reply.value("error", "");
            text = ok ? "Hermes 返回了空消息"
                      : "Hermes 暂时不可用" +
                            (bridge_error.empty() ? "" : "：" + bridge_error);
        }

        const bool command_reply =
            reply.value("command_handled_locally", false);
        std::string citation_warning;
        const nlohmann::json citations = ValidateHermesCitations(
            reply.value("citations", nlohmann::json::array()),
            &citation_warning);
        nlohmann::json message_content = {
            {"text", text},
            {"source", command_reply ? "hermes_command" : "hermes"},
            {"format", command_reply ? "plain_text" : "markdown"},
            {"citations", citations},
            {"hermes_model_override", reply.value("model_override", false)},
            {"hermes_model_state_confirmed", reply.value("model_state_confirmed", false)},
            {"hermes_model", reply.value("effective_model", "")},
            {"hermes_provider", reply.value("effective_provider", "")},
        };
        // Correlate a durable command receipt with the originating IM message.
        // A transport ACK alone must never be presented as model-switch success.
        if (command_reply && reply.value("command", nlohmann::json()) == "model" &&
            reply.contains("client_msg_id") && reply["client_msg_id"].is_string()) {
            const auto client_id = reply["client_msg_id"].get<std::string>();
            if (!client_id.empty() && client_id.size() <= 128) {
                nlohmann::json model = {{"provider", reply.value("effective_provider", "")},
                    {"id", reply.value("effective_model", "")}, {"name", reply.value("effective_model", "")}};
                const bool confirmed = ok && reply.value("model_state_confirmed", false) && AgentModelValid(model);
                message_content["agent_command_result"] = {{"client_msg_id", client_id},
                    {"command", "model"}, {"ok", confirmed}};
                if (confirmed) message_content["agent_command_result"]["model"] = model;
            }
        }
        if (ok && reply.contains("response_metadata") && reply["response_metadata"].is_object()) {
            const auto& metadata = reply["response_metadata"];
            if (metadata.contains("context_start_seq") && metadata["context_start_seq"].is_number_integer() &&
                metadata["context_start_seq"].get<int64_t>() >= 0)
                message_content["hermes_context_start_seq"] = metadata["context_start_seq"];
        }
        nlohmann::json event = {{"schema", "sparkpush.agent_event.v1"},
            {"type", "assistant_final"}, {"data", {{"text", text}}}};
        if (ok && command_reply &&
            reply.contains("response_metadata") && reply["response_metadata"].is_object() &&
            reply["response_metadata"].contains("agent_event")) {
            if (!NormalizeAgentEvent(reply["response_metadata"]["agent_event"], &event) ||
                event["type"] != "assistant_final" || event["data"]["text"] != text) return false;
        }
        if (command_reply && !event["data"].contains("presentation")) {
            event["data"]["presentation"] = {{"kind", "command_card"}, {"title", "命令帮助"},
                {"actions", nlohmann::json::array({{{"id", "help"}, {"label", "全部命令"}},
                                                 {{"id", "status"}, {"label", "会话状态"}}})}};
        }
        if (event["data"].contains("presentation"))
            message_content["agent_event"] = event;
        if (reply.contains("command") && reply.at("command").is_string()) {
            message_content["hermes_command"] = reply.at("command");
        }

        nlohmann::json content = {
            {"type", "single_chat"},
            {"session_id", session_id},
            {"from_user_id", bot_user_id},
            {"to_user_id", user_id},
            {"client_msg_id", "hermes:" + request_id},
            {"sender", {{"uid", bot_user_id},
                         {"name", bot_name}}},
            {"content", std::move(message_content)},
        };

        out->request_id = request_id;
        out->user_id = user_id;
        out->ok = ok;
        out->citation_warning = citation_warning;
        out->message.session_id = session_id;
        out->message.sender_id = bot_user_id;
        out->message.msg_type = "text";
        out->message.timestamp_ms = completed_at_ms;
        out->message.client_msg_id = "hermes:" + request_id;
        out->message.content_json = content.dump();
        error->clear();
        return true;
    } catch (const nlohmann::json::exception&) {
        // Never log parser diagnostics: they can contain user message text.
        return false;
    }
}

bool PersistHermesReply(
    Message* message, bool* is_new,
    const std::function<bool(Message*, bool*)>& allocate,
    const std::function<bool(const Message&)>& persist) {
    if (!message || !is_new || !allocate || !persist || !allocate(message, is_new))
        return false;
    auto content = nlohmann::json::parse(message->content_json);
    content["msg_id"] = message->msg_id;
    content["msg_seq"] = message->msg_seq;
    content["create_time"] = message->timestamp_ms;
    message->content_json = content.dump();
    // A Redis dedup hit only proves sequence allocation; always retry publish.
    return persist(*message);
}

}  // namespace sparkpush
