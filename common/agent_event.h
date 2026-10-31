#pragma once
#include <nlohmann/json.hpp>
#include <algorithm>
#include <string>

namespace sparkpush {

// =====================================================================
// Agent 事件的**校验 + 投影**层。
//
// 本文件是 Agent 输出进入 IM 消息流的唯一安全闸门。设计原则写在
// NormalizeAgentEvent 上方的英文注释里：只输出「渠道无关的公开契约」，
// 绝不转发任意运行时对象、凭证、可执行动作或原始 HTML。
//
// 两个关键手段：
//   1) 白名单：type、presentation.kind、action.id 全部枚举匹配，
//      未知值直接拒。不是「拦已知危险项」而是「只放已知安全项」。
//   2) 重建而不是过滤：命中白名单后并不把原对象透传，而是用
//      挑出来的字段**重新构造**一个新 JSON（见 public_data / safe / projected）。
//      这比「删掉危险字段」可靠得多：新增的嵌套字段不会意外泄出。
//
// 所有校验函数都返回 bool 且无副作用，失败时调用方丢弃整个事件。

// 通用「可见字符 token」校验：非空、不超长、不含控制字符。
//
// c <= 32 覆盖空格与所有 C0 控制符，c == 127 是 DEL。
// 为何连空格也禁：这些值会进入 route_key/session_key 等标识符，
// 带空格会在日志、URL、shell 命令里造成分词歧义与注入面。
inline bool AgentSafeToken(const std::string& value, size_t max_bytes) {
    return !value.empty() && value.size() <= max_bytes &&
        std::none_of(value.begin(), value.end(), [](unsigned char c) {
            return c <= 32 || c == 127;
        });
}

// slug 校验：小写字母开头，只允许 [a-z0-9_-]，最长 48。
//
// 用于 tenant_id / channel_id / agent_id 这些会拼进标识符的字段。
// 限定字符集比事后转义更可靠：这些值可能出现在文件名、URL 路径、
// 日志检索表达式里，任何一处的转义遗漏都是漏洞。
inline bool AgentSlugValid(const std::string& value) {
    return !value.empty() && value.size() <= 48 && value[0] >= 'a' && value[0] <= 'z' &&
        std::all_of(value.begin(), value.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
        });
}

// 校验 Agent 事件信封（路由/会话/事件账本的身份信息）。
//
// 为何要卡**精确长度**而不只是上限（event_id 恰好 36、route_key 恰好 35）：
// 这些 id 由 Gateway 生成，格式固定（evt_ + 32 hex = 36，rt_ + 32 hex = 35）。
// 卡死长度能挡住「前缀对但后面塞了额外结构」的伪造值，
// 也保证它们能安全地当数据库主键与日志检索键使用。
//
// sequence 上限 2^31：避开下游用 int32 存时溢出。
// created_at_ms 要求 > 0：0 意味着未设置，不能当成合法时间戳。
inline bool AgentEnvelopeValid(const nlohmann::json& envelope) {
    if (!envelope.is_object() ||
        envelope.value("schema", nlohmann::json()) != "sparkpush.agent_envelope.v1") return false;
    if (envelope.contains("run_id")) {
        if (!envelope["run_id"].is_string()) return false;
        const auto id = envelope["run_id"].get<std::string>();
        if (id.empty() || id.size() > 80 ||
            !std::all_of(id.begin(), id.end(), [](unsigned char c) {
                return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                       (c >= '0' && c <= '9') || c == '_' || c == '-';
            })) return false;
    }
    for (const auto* key : {"event_id", "request_id", "route_key", "session_key",
                            "tenant_id", "channel_id", "conversation_id", "thread_id", "agent_id"})
        if (!envelope.contains(key) || !envelope[key].is_string()) return false;
    const auto event_id = envelope["event_id"].get<std::string>();
    const auto route_key = envelope["route_key"].get<std::string>();
    if (event_id.size() != 36 || event_id.rfind("evt_", 0) != 0 ||
        route_key.size() != 35 || route_key.rfind("rt_", 0) != 0) return false;
    const auto hex_tail = [](const std::string& value, size_t offset) {
        return std::all_of(value.begin() + static_cast<std::ptrdiff_t>(offset), value.end(),
            [](unsigned char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
    };
    if (!hex_tail(event_id, 4) || !hex_tail(route_key, 3) ||
        !AgentSafeToken(envelope["request_id"].get<std::string>(), 121) ||
        !AgentSafeToken(envelope["session_key"].get<std::string>(), 256) ||
        !AgentSafeToken(envelope["conversation_id"].get<std::string>(), 160) ||
        !AgentSafeToken(envelope["thread_id"].get<std::string>(), 96) ||
        !AgentSlugValid(envelope["tenant_id"].get<std::string>()) ||
        !AgentSlugValid(envelope["channel_id"].get<std::string>()) ||
        !AgentSlugValid(envelope["agent_id"].get<std::string>())) return false;
    if (!envelope.contains("sequence") || !envelope["sequence"].is_number_integer() ||
        envelope["sequence"].get<int64_t>() < 0 || envelope["sequence"].get<int64_t>() >= (1LL << 31) ||
        !envelope.contains("created_at_ms") || !envelope["created_at_ms"].is_number_integer() ||
        envelope["created_at_ms"].get<int64_t>() <= 0 ||
        !envelope.contains("replayable") || !envelope["replayable"].is_boolean() ||
        !envelope.contains("terminal") || !envelope["terminal"].is_boolean()) return false;
    return !envelope.contains("replayed") || envelope["replayed"].is_boolean();
}

// 校验模型标识。关键约束是 provider 里不能包含 ':'：
// 因为下面会拼成 "provider:id" 作为唯一标识，provider 自带冒号
// 会造成歧义（"a:b" + ":c" 与 "a" + ":b:c" 无法区分）。
inline bool AgentModelValid(const nlohmann::json& model) {
    if (!model.is_object()) return false;
    for (const auto* key : {"provider", "id", "name"})
        if (!model.contains(key) || !model[key].is_string()) return false;
    const auto provider = model["provider"].get<std::string>();
    const auto id = model["id"].get<std::string>();
    const auto target = provider + ":" + id;
    return !provider.empty() && !id.empty() && provider.find(':') == std::string::npos &&
        target.size() <= 128 && model["name"].get<std::string>().size() <= 640 &&
        std::none_of(target.begin(), target.end(), [](unsigned char c) { return c <= 32 || c == 127; });
}

inline bool AgentProviderValid(const nlohmann::json& provider) {
    if (!provider.is_object() || !provider.contains("id") ||
        !provider["id"].is_string() || !provider.contains("name") ||
        !provider["name"].is_string() || !provider.contains("model_count") ||
        !provider["model_count"].is_number_integer()) return false;
    const auto id = provider["id"].get<std::string>();
    const auto name = provider["name"].get<std::string>();
    const auto count = provider["model_count"].get<int64_t>();
    if (id.empty() || id.find(':') != std::string::npos || id.size() > 128 ||
        name.empty() || name.size() > 160 || count < 0 || count > 100000) return false;
    if (provider.contains("current") && !provider["current"].is_boolean()) return false;
    return std::none_of(id.begin(), id.end(), [](unsigned char c) {
               return c <= 32 || c == 127;
           }) &&
           std::none_of(name.begin(), name.end(), [](unsigned char c) {
               return c == 0 || c < 32 || c == 127;
           });
}

// 校验指令卡片里的一个动作按钮。
//
// 这是防注入的核心白名单：动作 id 必须在末尾的枚举列表里，
// 未知 id 直接拒。因此 Agent 不能自己发明一个动作让前端去执行。
//
// 三个带参数的动作有额外的严格校验：
//   agent_open     value 必须是纯数字、不以 0 开头、<= 2^53-1。
//                  上限是 JavaScript 的 Number.MAX_SAFE_INTEGER：
//                  超过它前端解析会丢精度，开错会话。
//   agent_set      value 必须是 slug
//   reasoning_set  value 必须是七个枚举之一
inline bool AgentCommandActionValid(const nlohmann::json& action) {
    if (!action.is_object() || !action.contains("id") || !action["id"].is_string() ||
        !action.contains("label") || !action["label"].is_string() ||
        action["label"].get<std::string>().empty() || action["label"].get<std::string>().size() > 160) return false;
    const auto id = action["id"].get<std::string>();
    if (id == "agent_open") {
        if (!action.contains("value") || !action["value"].is_string()) return false;
        const auto value = action["value"].get<std::string>();
        return !value.empty() && value.size() <= 16 && value[0] != '0' &&
            std::all_of(value.begin(), value.end(), [](unsigned char c) { return c >= '0' && c <= '9'; }) &&
            std::stoull(value) <= 9007199254740991ULL;
    }
    if (id == "agent_set") {
        if (!action.contains("value") || !action["value"].is_string()) return false;
        const auto value = action["value"].get<std::string>();
        return !value.empty() && value.size() <= 48 && value[0] >= 'a' && value[0] <= 'z' &&
            std::all_of(value.begin(), value.end(), [](unsigned char c) {
                return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
            });
    }
    if (id == "reasoning_set") {
        const auto level = action.value("value", nlohmann::json());
        return level == "off" || level == "minimal" || level == "low" || level == "medium" ||
               level == "high" || level == "xhigh" || level == "max";
    }
    return id == "agent" || id == "model" || id == "reasoning" || id == "new" || id == "retry" ||
        id == "restart" || id == "restart_confirm" || id == "status" || id == "help" || id == "new_confirm" || id == "retry_confirm" || id == "cancel" ||
        id == "roleplay" || id == "write" || id == "scene" || id == "character" || id == "persona" ||
        id == "world" || id == "memory" || id == "branch" || id == "branches" || id == "export";
}

// Validate and project onto a channel-neutral, public contract. Never forward
// arbitrary runtime objects, credentials, executable actions or raw HTML.
//
// 本文件的主函数。把 Agent 运行时事件收敛成可以安全渲染到
// Web/Android 客户端的公开结构。
//
// 流程：
//   1) 卡 schema 与 type 白名单：assistant_delta / assistant_final /
//      assistant_progress / error，以及只含短文本的 tool_start /
//      tool_update / tool_end。工具事件不是回答，不能带 presentation。
//   2) 正文长度分级限制：final/delta 可到 16 MiB，但 progress、error
//      和工具事件只允许 1 KiB——它们是状态提示，不应承载正文，限制能挡住
//      用「进度事件」大量泵数据的滥用
//   3) 若有 presentation，按 kind 分支逐个校验并**重建**
//   4) 最后只把 schema/type/data(/envelope) 四个字段写进 output
//
// 第 4 步是安全关键：无论 input 里还有什么额外字段（凭证、内部
// 路由、工具调用详情），都不会出现在 output 里。
//
// presentation 只允许出现在 assistant_final：流式中间帧带 UI 卡片会造成
// 前端反复重建 DOM，也给了中途注入 UI 的机会。
// ui.dump().size() > 96000 的限制是对整个卡片序列化后的总大小卡上限，
// 避开「每个字段都合规但加起来巨大」的情况。
inline bool NormalizeAgentEvent(const nlohmann::json& input, nlohmann::json* output) {
    if (!output || !input.is_object() || input.value("schema", nlohmann::json()) != "sparkpush.agent_event.v1" ||
        !input.contains("type") || !input["type"].is_string() ||
        !input.contains("data") || !input["data"].is_object()) return false;
    const auto kind = input["type"].get<std::string>();
    const auto& data = input["data"];
    const bool tool_event = kind == "tool_start" || kind == "tool_update" || kind == "tool_end";
    if ((kind != "assistant_delta" && kind != "assistant_final" &&
         kind != "assistant_progress" && kind != "error" && !tool_event) ||
        !data.contains("text") || !data["text"].is_string() ||
        data["text"].get<std::string>().size() > 16 * 1024 * 1024) return false;
    if ((kind == "assistant_progress" || kind == "error" || tool_event) &&
        data["text"].get<std::string>().size() > 1024) return false;
    if (tool_event && data.contains("presentation")) return false;
    nlohmann::json public_data = {{"text", data["text"]}};
    if (data.contains("presentation")) {
        const auto& ui = data["presentation"];
        if (kind != "assistant_final" || !ui.is_object() || ui.dump().size() > 96000) return false;
        if (ui.value("kind", nlohmann::json()) == "command_card") {
            if (!ui.contains("title") || !ui["title"].is_string() || ui["title"].get<std::string>().size() > 320 ||
                !ui.contains("actions") || !ui["actions"].is_array() || ui["actions"].empty() ||
                ui["actions"].size() > 16) return false;
            auto actions = nlohmann::json::array();
            for (const auto& action : ui["actions"]) {
                if (!AgentCommandActionValid(action)) return false;
                // 重建而不是透传：只拷 id/label/selected 三个字段。
                // selected 用 == true 强转成真正的 bool，避开传入
                // 字符串 "true" 或其他 truthy 值时语义不一致。
                nlohmann::json safe = {{"id", action["id"]}, {"label", action["label"]},
                    {"selected", action.value("selected", nlohmann::json()) == true}};
                // value 只对这三个需要参数的动作保留；其他动作即使
                // 带了 value 也会被丢掉，避开多余数据流入前端。
                if (action["id"] == "reasoning_set" || action["id"] == "agent_set" || action["id"] == "agent_open") safe["value"] = action["value"];
                actions.push_back(safe);
            }
            public_data["presentation"] = {{"kind", "command_card"}, {"title", ui["title"]}, {"actions", actions}};
        } else if (ui.value("kind", nlohmann::json()) == "reasoning_picker") {
            if (!ui.contains("levels") || !ui["levels"].is_array() || ui["levels"].size() > 8) return false;
            public_data["presentation"] = { {"kind", "reasoning_picker"}, {"levels", ui["levels"]},
                {"current", ui.value("current", nlohmann::json())} };
        } else if (ui.value("kind", nlohmann::json()) == "creative_workspace") {
            if (!ui.contains("title") || !ui["title"].is_string() || ui["title"].get<std::string>().size() > 320 ||
                !ui.contains("state") || !ui["state"].is_object() || !ui.contains("actions") ||
                !ui["actions"].is_array() || ui["actions"].size() > 16) return false;
            auto actions = nlohmann::json::array();
            for (const auto& action : ui["actions"]) {
                if (!AgentCommandActionValid(action)) return false;
                actions.push_back({{"id", action["id"]}, {"label", action["label"]},
                    {"selected", action.value("selected", false) == true}});
            }
            auto projected = nlohmann::json{{"kind", "creative_workspace"}, {"title", ui["title"]},
                {"state", ui["state"]}, {"actions", actions}};
            if (ui.contains("artifact")) {
                const auto& artifact = ui["artifact"];
                // artifact 是 Agent 生成的文本产物（比如一段代码/文章）。
                // name 里禁 '/' 与 '\\' 以及控制字符：前端可能拿它做下载
                // 文件名，不拦就会造成路径穿越或生成非法文件名。
                if (!artifact.is_object() || !artifact.contains("name") || !artifact["name"].is_string() ||
                    !artifact.contains("mime") || !artifact["mime"].is_string() ||
                    !artifact.contains("text") || !artifact["text"].is_string() ||
                    artifact["name"].get<std::string>().size() > 120 || artifact["mime"].get<std::string>().size() > 120 ||
                    artifact["text"].get<std::string>().size() > 240000 ||
                    artifact["name"].get<std::string>().find_first_of("/\\") != std::string::npos ||
                    std::any_of(artifact["name"].get<std::string>().begin(), artifact["name"].get<std::string>().end(),
                        [](unsigned char c) { return c == 0 || c < 32; })) return false;
                projected["artifact"] = {{"name", artifact["name"]}, {"mime", artifact["mime"]}, {"text", artifact["text"]}};
            }
            public_data["presentation"] = projected;
        } else {
            if (ui.value("kind", nlohmann::json()) != "model_picker" ||
                !ui.contains("models") || !ui["models"].is_array() || ui["models"].size() > 512 ||
                !ui.contains("current") || !AgentModelValid(ui["current"]) ||
                ui.dump().size() > 96000) return false;
            auto project = [](const nlohmann::json& model) {
                return nlohmann::json{{"provider", model["provider"]}, {"id", model["id"]},
                    {"name", model["name"]}, {"reasoning", model.value("reasoning", nlohmann::json()) == true}};
            };
            auto models = nlohmann::json::array();
            for (const auto& model : ui["models"]) {
                if (!AgentModelValid(model)) return false;
                models.push_back(project(model));
            }
            nlohmann::json providers = nlohmann::json::array();
            if (ui.contains("providers")) {
                if (!ui["providers"].is_array() || ui["providers"].size() > 128) return false;
                for (const auto& provider : ui["providers"]) {
                    if (!AgentProviderValid(provider)) return false;
                    nlohmann::json safe = {
                        {"id", provider["id"]},
                        {"name", provider["name"]},
                        {"model_count", provider["model_count"]},
                        {"current", provider.value("current", false)}};
                    providers.push_back(std::move(safe));
                }
            }
            public_data["presentation"] = {{"kind", "model_picker"}, {"models", models},
                {"current", project(ui["current"])}, {"truncated", ui.value("truncated", nlohmann::json()) == true}};
            if (ui.contains("providers")) public_data["presentation"]["providers"] = providers;
            if (ui.contains("selected_provider")) {
                if (!ui["selected_provider"].is_string()) return false;
                const auto selected = ui["selected_provider"].get<std::string>();
                if (selected.empty() || selected.find(':') != std::string::npos || selected.size() > 128 ||
                    std::any_of(selected.begin(), selected.end(), [](unsigned char c) {
                        return c <= 32 || c == 127;
                    })) return false;
                public_data["presentation"]["selected_provider"] = selected;
            }
        }
    }
    *output = {{"schema", "sparkpush.agent_event.v1"}, {"type", kind}, {"data", public_data}};
    if (input.contains("envelope")) {
        if (!AgentEnvelopeValid(input["envelope"])) return false;
        (*output)["envelope"] = input["envelope"];
    }
    return true;
}
// Public source provenance contains no host paths or arbitrary tool metadata.
inline bool NormalizeProjectSnapshot(const nlohmann::json& input,nlohmann::json* output) {
    if(!input.is_object() || !input.contains("project_id") || !input["project_id"].is_string() ||
       !AgentSlugValid(input["project_id"].get<std::string>()) || !input.contains("revision") ||
       !input["revision"].is_string() || !input.contains("file_count") || !input["file_count"].is_number_integer())return false;
    const auto revision=input["revision"].get<std::string>();
    if(revision.size()!=64 || revision.find_first_not_of("0123456789abcdef")!=std::string::npos ||
       input["file_count"]<0 || input["file_count"]>4096)return false;
    *output={{"project_id",input["project_id"]},{"revision",revision},{"file_count",input["file_count"]}};
    return true;
}
}  // namespace sparkpush
