#include "hermes_command.h"

#include <algorithm>
#include <cctype>

namespace sparkpush {
namespace {

std::string TrimCopy(const std::string& value) {
    const size_t begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) return {};
    const size_t end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

std::string LowerCopy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char c) {
                       return static_cast<char>(std::tolower(c));
                   });
    return value;
}

bool ContainsSpaceOrControl(const std::string& value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char c) {
        return std::isspace(c) || std::iscntrl(c);
    });
}

bool IsKnowledgeManagementCommand(const std::string& arguments) {
    const std::string lowered = LowerCopy(TrimCopy(arguments));
    const size_t separator = lowered.find_first_of(" \t\r\n");
    const std::string first = lowered.substr(0, separator);
    return first == "build" || first == "rebuild" || first == "ingest" ||
           first == "sync" || first == "delete" || first == "remove" ||
           first == "source" || first == "add" || first == "enable" ||
           first == "disable";
}

bool ApplyModelArgument(const std::string& arguments, bool* model_override,
                        std::string* model, std::string* provider,
                        std::string* error) {
    const std::string target = TrimCopy(arguments);
    if (target.empty() || target == "refresh") return true;  // /model 仅查询，不改变当前值。
    if (target == "default" || target == "reset") {
        *model_override = false;
        model->clear();
        provider->clear();
        return true;
    }
    if (target.size() > 128 || ContainsSpaceOrControl(target)) {
        if (error) {
            *error = "模型格式无效；请使用 /model provider:model，或 "
                     "/model default 恢复默认模型";
        }
        return false;
    }
    if (target == "--global" || target.rfind("--global", 0) == 0) {
        if (error) {
            *error = "Spark Push 只支持会话级模型切换，不允许 /model --global";
        }
        return false;
    }

    const size_t separator = target.find(':');
    if (separator == std::string::npos) {
        if (error) *error = "请发送 /model 点击选择，或使用 /model provider:model";
        return false;
    } else {
        *provider = target.substr(0, separator);
        *model = target.substr(separator + 1);
        if (provider->empty() || model->empty()) {
            if (error) *error = "模型格式无效；provider 和 model 不能为空";
            return false;
        }
    }
    *model_override = true;
    return true;
}

void ApplyPersistedEntry(const HermesConversationEntry& entry,
                         HermesPromptPlan* plan, int64_t* pending_new_seq) {
    if (entry.assistant && entry.legacy_new_confirmation && *pending_new_seq > plan->context_start_seq) {
        plan->context_start_seq = *pending_new_seq;
        plan->messages.clear();
        plan->model_override = false;
        plan->model.clear();
        plan->provider.clear();
    }
    if (entry.assistant && entry.has_context_state && entry.context_start_seq > plan->context_start_seq) {
        plan->messages.clear();
        plan->model_override = false;
        plan->model.clear();
        plan->provider.clear();
        plan->context_start_seq = entry.context_start_seq;
    }
    // 模型状态先于 command_reply 过滤处理，因为 /model 的确认回复本身就是
    // 最新、可持久恢复的状态快照。
    if (entry.has_model_state) {
        plan->model_override = entry.model_override;
        plan->model = entry.model_override ? entry.model : std::string{};
        plan->provider = entry.model_override ? entry.provider : std::string{};
    }
    if (entry.command_reply || entry.text.empty()) return;

    if (entry.assistant) {
        plan->messages.emplace_back("assistant", entry.text);
        return;
    }

    const HermesCommand command = ParseHermesCommand(entry.text);
    switch (command.kind) {
        case HermesCommandKind::kNone:
            plan->messages.emplace_back("user", entry.text);
            break;
        case HermesCommandKind::kNew:
            *pending_new_seq = entry.msg_seq;
            break;
        case HermesCommandKind::kRestart:
        case HermesCommandKind::kRetry:
            // User intent and preview cards cannot mutate confirmed context.
            break;
        case HermesCommandKind::kCann:
        case HermesCommandKind::kKnowledge:
            plan->messages.emplace_back("user", entry.text);
            break;
        case HermesCommandKind::kModel: {
            // A user command is intent, not confirmation. Only the Agent's
            // verified model snapshot may change subsequent request state.
            break;
        }
        case HermesCommandKind::kAgent:
        case HermesCommandKind::kReasoning:
        case HermesCommandKind::kStatus:
        case HermesCommandKind::kHelp:
        case HermesCommandKind::kUnknown:
            // 本地命令及其确认回复不进入模型上下文。
            break;
    }
}

void LimitPrompt(size_t max_prompt_chars, HermesPromptPlan* plan) {
    if (plan->messages.empty()) {
        plan->prompt_chars = 0;
        return;
    }
    std::vector<std::pair<std::string, std::string>> selected;
    size_t chars = 0;
    for (auto it = plan->messages.rbegin(); it != plan->messages.rend(); ++it) {
        const size_t item_chars = it->second.size();
        // 最新消息即使超预算也保留，避免用户输入被静默截断。
        if (!selected.empty() && chars + item_chars > max_prompt_chars) break;
        selected.push_back(*it);
        chars += item_chars;
    }
    std::reverse(selected.begin(), selected.end());
    plan->messages = std::move(selected);
    plan->prompt_chars = chars;
}

}  // namespace

HermesCommand ParseHermesCommand(const std::string& text) {
    HermesCommand result;
    const std::string input = TrimCopy(text);
    if (input.empty() || input[0] != '/') return result;

    const size_t separator = input.find_first_of(" \t\r\n");
    const std::string token =
        LowerCopy(input.substr(1, separator == std::string::npos
                                     ? std::string::npos
                                     : separator - 1));
    result.name = token;
    if (separator != std::string::npos) {
        result.arguments = TrimCopy(input.substr(separator + 1));
    }

    if (token == "new" || token == "reset" || token == "clear") {
        result.kind = HermesCommandKind::kNew;
        result.name = "new";
    } else if (token == "status") {
        result.kind = HermesCommandKind::kStatus;
    } else if (token == "restart") {
        result.kind = HermesCommandKind::kRestart;
    } else if (token == "retry") {
        result.kind = HermesCommandKind::kRetry;
    } else if (token == "reasoning" || token == "thinking") {
        result.kind = HermesCommandKind::kReasoning;
        result.name = "reasoning";
    } else if (token == "agent" || token == "agents") {
        result.kind = HermesCommandKind::kAgent;
        result.name = "agent";
    } else if (token == "model") {
        result.kind = HermesCommandKind::kModel;
    } else if (token == "help" || token == "commands") {
        result.kind = HermesCommandKind::kHelp;
        result.name = "help";
    } else if (token == "cann") {
        result.kind = HermesCommandKind::kCann;
    } else if (token == "kb") {
        result.kind = HermesCommandKind::kKnowledge;
    } else if (token == "roleplay" || token == "rp" || token == "write" ||
               token == "scene" || token == "character" || token == "world" ||
               token == "memory" || token == "remember" || token == "forget") {
        result.kind = HermesCommandKind::kCreative;
        result.name = token;
    } else {
        result.kind = HermesCommandKind::kUnknown;
    }
    return result;
}

std::string HermesCommandName(HermesCommandKind kind) {
    switch (kind) {
        case HermesCommandKind::kAgent:
            return "agent";
        case HermesCommandKind::kNew:
            return "new";
        case HermesCommandKind::kStatus:
            return "status";
        case HermesCommandKind::kRestart:
            return "restart";
        case HermesCommandKind::kRetry:
            return "retry";
        case HermesCommandKind::kReasoning:
            return "reasoning";
        case HermesCommandKind::kModel:
            return "model";
        case HermesCommandKind::kHelp:
            return "help";
        case HermesCommandKind::kCann:
            return "cann";
        case HermesCommandKind::kKnowledge:
            return "kb";
        case HermesCommandKind::kCreative:
            return "creative";
        case HermesCommandKind::kUnknown:
            return "unknown";
        case HermesCommandKind::kNone:
            return {};
    }
    return {};
}

HermesPromptPlan BuildHermesPromptPlan(
    const std::vector<HermesConversationEntry>& history,
    const std::string& current_text, int64_t current_seq,
    size_t max_prompt_chars) {
    HermesPromptPlan plan;
    int64_t pending_new_seq = 0;
    for (const auto& entry : history) ApplyPersistedEntry(entry, &plan, &pending_new_seq);

    plan.command = ParseHermesCommand(current_text);
    switch (plan.command.kind) {
        case HermesCommandKind::kNone:
            if (!current_text.empty()) {
                plan.messages.emplace_back("user", current_text);
            }
            break;
        case HermesCommandKind::kRestart:
            plan.call_model = false;
            if (!plan.command.arguments.empty() && plan.command.arguments != "now" &&
                plan.command.arguments != "--yes" && plan.command.arguments != "-y")
                plan.local_error = "用法：/restart 打开确认卡片，或 /restart now 确认重启";
            break;
        case HermesCommandKind::kNew:
            plan.call_model = false;
            if (!plan.command.arguments.empty() && plan.command.arguments != "now" &&
                plan.command.arguments != "--yes" && plan.command.arguments != "-y")
                plan.local_error = "用法：/new 打开确认卡片，或 /new now 确认新建";
            break;
        case HermesCommandKind::kAgent:
        case HermesCommandKind::kReasoning:
        case HermesCommandKind::kStatus:
        case HermesCommandKind::kHelp:
        case HermesCommandKind::kUnknown:
            plan.call_model = false;
            break;
        case HermesCommandKind::kModel:
            plan.call_model = false;
            ApplyModelArgument(plan.command.arguments, &plan.model_override,
                               &plan.model, &plan.provider, &plan.local_error);
            break;
        case HermesCommandKind::kRetry:
            // The runtime selects the last user message under its session lock.
            plan.call_model = plan.command.arguments == "now" || plan.command.arguments == "--yes" ||
                              plan.command.arguments == "-y";
            if (!plan.command.arguments.empty() && !plan.call_model)
                plan.local_error = "用法：/retry 打开确认卡片，或 /retry now 确认重试";
            break;
        case HermesCommandKind::kCann:
        case HermesCommandKind::kKnowledge:
            plan.force_knowledge_retrieval = true;
            if (plan.command.arguments.empty()) {
                plan.call_model = false;
                plan.local_error = plan.command.kind == HermesCommandKind::kCann
                                       ? "用法：/cann <CANN 开发问题>"
                                       : "用法：/kb <问题>；/kb status 查看知识库状态";
            } else if (plan.command.kind == HermesCommandKind::kKnowledge &&
                       IsKnowledgeManagementCommand(plan.command.arguments)) {
                plan.call_model = false;
                plan.local_error =
                    "聊天命令仅允许只读检索和 /kb status；建库、同步、删除请在 "
                    "cann-rag CLI 中显式执行";
            } else {
                plan.messages.emplace_back("user", current_text);
            }
            break;
        case HermesCommandKind::kCreative:
            plan.call_model = false;
            break;
    }

    plan.context_message_count = plan.messages.size();
    if (plan.call_model) LimitPrompt(max_prompt_chars, &plan);
    return plan;
}

}  // namespace sparkpush
