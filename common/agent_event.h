#pragma once
#include <nlohmann/json.hpp>
#include <algorithm>
#include <string>

namespace sparkpush {
inline bool AgentSafeToken(const std::string& value, size_t max_bytes) {
    return !value.empty() && value.size() <= max_bytes &&
        std::none_of(value.begin(), value.end(), [](unsigned char c) {
            return c <= 32 || c == 127;
        });
}

inline bool AgentSlugValid(const std::string& value) {
    return !value.empty() && value.size() <= 48 && value[0] >= 'a' && value[0] <= 'z' &&
        std::all_of(value.begin(), value.end(), [](unsigned char c) {
            return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-';
        });
}

inline bool AgentEnvelopeValid(const nlohmann::json& envelope) {
    if (!envelope.is_object() ||
        envelope.value("schema", nlohmann::json()) != "sparkpush.agent_envelope.v1") return false;
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
inline bool NormalizeAgentEvent(const nlohmann::json& input, nlohmann::json* output) {
    if (!output || !input.is_object() || input.value("schema", nlohmann::json()) != "sparkpush.agent_event.v1" ||
        !input.contains("type") || !input["type"].is_string() ||
        !input.contains("data") || !input["data"].is_object()) return false;
    const auto kind = input["type"].get<std::string>();
    const auto& data = input["data"];
    if ((kind != "assistant_delta" && kind != "assistant_final" &&
         kind != "assistant_progress" && kind != "error") ||
        !data.contains("text") || !data["text"].is_string() ||
        data["text"].get<std::string>().size() > 16 * 1024 * 1024) return false;
    if ((kind == "assistant_progress" || kind == "error") &&
        data["text"].get<std::string>().size() > 1024) return false;
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
                nlohmann::json safe = {{"id", action["id"]}, {"label", action["label"]},
                    {"selected", action.value("selected", nlohmann::json()) == true}};
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
}  // namespace sparkpush
