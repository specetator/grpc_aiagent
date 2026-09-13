#pragma once
#include <nlohmann/json.hpp>
#include <algorithm>
#include <string>

namespace sparkpush {
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
        id == "restart" || id == "restart_confirm" || id == "status" || id == "help" || id == "new_confirm" || id == "retry_confirm" || id == "cancel";
}

// Validate and project onto a channel-neutral, public contract. Never forward
// arbitrary runtime objects, credentials, executable actions or raw HTML.
inline bool NormalizeAgentEvent(const nlohmann::json& input, nlohmann::json* output) {
    if (!output || !input.is_object() || input.value("schema", nlohmann::json()) != "sparkpush.agent_event.v1" ||
        !input.contains("type") || !input["type"].is_string() ||
        !input.contains("data") || !input["data"].is_object()) return false;
    const auto kind = input["type"].get<std::string>();
    const auto& data = input["data"];
    if ((kind != "assistant_delta" && kind != "assistant_final" && kind != "assistant_progress") ||
        !data.contains("text") || !data["text"].is_string() ||
        data["text"].get<std::string>().size() > 16 * 1024 * 1024) return false;
    if (kind == "assistant_progress" && data["text"].get<std::string>().size() > 1024) return false;
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
            public_data["presentation"] = {{"kind", "model_picker"}, {"models", models},
                {"current", project(ui["current"])}, {"truncated", ui.value("truncated", nlohmann::json()) == true}};
        }
    }
    *output = {{"schema", "sparkpush.agent_event.v1"}, {"type", kind}, {"data", public_data}};
    return true;
}
}  // namespace sparkpush
