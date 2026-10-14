#include "hermes_reply.h"

#include <algorithm>
#include <limits>
#include <nlohmann/json.hpp>
#include "citation_validation.h"
#include "agent_event.h"

namespace sparkpush {
namespace {
// 只接受正的、能放进 int64 的整数。unsigned 且超过 int64 上限时直接当 0，
// 避开 get<int64_t>() 在超大无符号值上的实现定义行为。
// 返回 0 表示「这个字段不能用」，调用方一律按校验失败处理。
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
                       std::string* error,
                       const nlohmann::json* authorized_room_request) {
    if (!out || !error) return false;
    *out = {};
    // 默认错误先写上。任何提前 return 都带着这句，成功路径末尾才 clear。
    // 这样漏写错误文本时不会返回空 error，让上游以为「失败了但没原因」。
    *error = "invalid ai_reply envelope";
    try {
        const auto reply = nlohmann::json::parse(payload);
        if (!reply.is_object() || bot_user_id <= 0) return false;
        const std::string request_id = reply.at("request_id").get<std::string>();
        const int64_t user_id = PositiveId(reply.at("user_id"));
        // completed_at_ms 来自载荷，不读墙上时钟。见头文件：重放必须得到相同字节。
        const int64_t completed_at_ms = PositiveId(reply.at("completed_at_ms"));
        // request_id 上限 121：client_msg_id 列是 VARCHAR(128)，
        // 前面还要拼 "hermes:"（7 字节），121+7=128。再长就会插失败或被截断，
        // 幂等比对失真。这也对齐 room_agent_request.request_id VARCHAR(121)。
        // user_id == bot_user_id 直接拒：机器人不能回复自己。
        if (request_id.empty() || request_id.size() > 121 || user_id == 0 ||
            user_id == bot_user_id || completed_at_ms == 0 ||
            PositiveId(reply.at("bot_user_id")) != bot_user_id ||
            !reply.at("ok").is_boolean()) return false;
        const bool room = authorized_room_request != nullptr;
        // 房间回复必须和授权请求的五个字段逐一相等。
        // 少一个或改一个都拒：共享 Agent 的回复不能被改挂到别的房间或用户。
        if (room) {
            for (const auto* key : {"request_id", "session_id", "user_id", "bot_user_id", "collaboration"})
                if (!reply.contains(key) || reply[key] != authorized_room_request->at(key)) return false;
        }
        // 单聊 session_id 由双方 id 按升序重算，不相信载荷里自己报的值。
        // 对不上就拒，避免一条回复被写进另一个会话。
        const std::string session_id = room ? authorized_room_request->at("session_id").get<std::string>() :
            "s_" + std::to_string(std::min(user_id, bot_user_id)) +
            "_" + std::to_string(std::max(user_id, bot_user_id));
        if (reply.at("session_id").get<std::string>() != session_id) return false;
        const bool ok = reply.value("ok", false);
        std::string text = reply.value("text", "");
        // 空文本也要落一条消息。失败时用户需要看到「出错了」，
        // 而不是永远等一个不会来的回复（头文件里 ok=false 仍发消息就是这个原因）。
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
        // 译：用持久的命令回执对上那条发起它的 IM 消息。
        // 仅仅一个传输层 ACK 绝不能被展示成「模型切换成功」。
        // confirmed 要同时满足 ok、model_state_confirmed 和模型字段合法，
        // 三者缺一就把 agent_command_result.ok 写成 false。
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
            // Persist only the allowlisted runtime counters, not arbitrary provider metadata.
            // 译：只持久化白名单里的运行时计数，不收任意的 provider 元数据。
            // 载荷来自 Bridge，里面可能带着上游供应商塞进来的字段；
            // 原样写入消息体会把不可控内容存进历史，也会让重放字节不稳定。
            if (metadata.contains("usage") && metadata["usage"].is_object()) {
                nlohmann::json usage = nlohmann::json::object();
                for (const auto* key : {"prompt_tokens", "completion_tokens", "cached_prompt_tokens"}) {
                    const auto& source = metadata["usage"];
                    if (source.contains(key) && source[key].is_number_integer() &&
                        (!source[key].is_number_unsigned() || source[key].get<uint64_t>() <=
                         static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) &&
                        source[key].get<int64_t>() >= 0)
                        usage[key] = source[key];
                }
                if (!usage.empty()) message_content["inference_usage"] = std::move(usage);
            }
            if (metadata.contains("worker_id") && metadata["worker_id"].is_string() &&
                metadata["worker_id"].get_ref<const std::string&>().size() <= 128)
                message_content["inference_worker_id"] = metadata["worker_id"];
        }
        nlohmann::json event = {{"schema", "sparkpush.agent_event.v1"},
            {"type", "assistant_final"}, {"data", {{"text", text}}}};
        bool has_agent_event = false;
        if (ok &&
            reply.contains("response_metadata") && reply["response_metadata"].is_object() &&
            reply["response_metadata"].contains("agent_event")) {
            // 终态事件的类型和正文必须与上面已经确定的 text 一致。
            // 对不上就整条拒：不能让事件里的文本和消息正文各说各的。
            if (!NormalizeAgentEvent(reply["response_metadata"]["agent_event"], &event) ||
                event["type"] != "assistant_final" || event["data"]["text"] != text) return false;
            if (event.contains("envelope")) {
                const auto& envelope = event["envelope"];
                // 终态序号必须等于本轮 delta_count，且 envelope 标了 terminal。
                // 这样一条被拼接进来的旧事件没法声称自己是这次流的结尾。
                if (envelope.value("request_id", "") != request_id ||
                    envelope.value("conversation_id", "") != session_id ||
                    !envelope.value("terminal", false) ||
                    !reply.contains("delta_count") ||
                    !reply["delta_count"].is_number_integer() ||
                    reply["delta_count"].get<int64_t>() < 0 ||
                    envelope.value("sequence", -1LL) !=
                        reply["delta_count"].get<int64_t>()) return false;
            }
            has_agent_event = true;
        }
        if (command_reply && !event["data"].contains("presentation")) {
            event["data"]["presentation"] = {{"kind", "command_card"}, {"title", "命令帮助"},
                {"actions", nlohmann::json::array({{{"id", "help"}, {"label", "全部命令"}},
                                                 {{"id", "status"}, {"label", "会话状态"}}})}};
        }
        if (has_agent_event || event["data"].contains("presentation"))
            message_content["agent_event"] = event;
        if (reply.contains("command") && reply.at("command").is_string()) {
            message_content["hermes_command"] = reply.at("command");
        }

        nlohmann::json content = {
            {"type", room ? "chatroom" : "single_chat"},
            {"session_id", session_id},
            {"from_user_id", bot_user_id},
            {"to_user_id", user_id},
            // client_msg_id 由 request_id 确定性拼出，不随机生成。
            // 重放同一条 Kafka 记录会得到同一个 id，InsertMessage 才能幂等。
            {"client_msg_id", "hermes:" + request_id},
            {"sender", {{"uid", bot_user_id},
                         {"name", bot_name}}},
            {"content", std::move(message_content)},
        };

        if (room) {
            content["group_id"] = reply.at("collaboration").at("room_id");
            content["actor_user_id"] = user_id;
            content["task_id"] = reply.at("collaboration").at("task_id");
            content["request_id"] = request_id;
            content.erase("to_user_id");
        }

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
        // 译：不要记录解析器的诊断信息，里面可能带着用户消息正文。
        // 缺字段与类型不对都走这里，对外只留上面那句固定的 envelope 错误。
        return false;
    }
}

bool PersistHermesReply(
    Message* message, bool* is_new,
    const std::function<bool(Message*, bool*)>& allocate,
    const std::function<bool(const Message&)>& persist) {
    // 必须先 allocate。persist 发出去的字节要带上已分配的 msg_id/msg_seq，
    // 顺序倒了的话 Job 落库对不上预约。
    if (!message || !is_new || !allocate || !persist || !allocate(message, is_new))
        return false;
    auto content = nlohmann::json::parse(message->content_json);
    content["msg_id"] = message->msg_id;
    content["msg_seq"] = message->msg_seq;
    content["create_time"] = message->timestamp_ms;
    message->content_json = content.dump();
    // A Redis dedup hit only proves sequence allocation; always retry publish.
    // 译：Redis 去重命中只证明序号分配过，发布必须照样重试。
    // is_new==false 也要 persist：上次可能在 Kafka 确认前就失败了，
    // 不重发这条回复就永远到不了用户。幂等由 client_msg_id 保证，不会插出第二条。
    return persist(*message);
}

}  // namespace sparkpush
