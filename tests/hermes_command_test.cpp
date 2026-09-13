#include "hermes_command.h"

#include <iostream>
#include <string>
#include <vector>

namespace {

bool Check(bool condition, const std::string& message) {
    if (condition) return true;
    std::cerr << "FAIL: " << message << '\n';
    return false;
}

sparkpush::HermesConversationEntry User(int64_t seq,
                                        const std::string& text) {
    sparkpush::HermesConversationEntry entry;
    entry.msg_seq = seq;
    entry.text = text;
    return entry;
}

sparkpush::HermesConversationEntry Assistant(int64_t seq,
                                             const std::string& text) {
    auto entry = User(seq, text);
    entry.assistant = true;
    return entry;
}

sparkpush::HermesConversationEntry CommandReply(int64_t seq,
                                                const std::string& text) {
    auto entry = Assistant(seq, text);
    entry.command_reply = true;
    return entry;
}

}  // namespace

int main() {
    using sparkpush::BuildHermesPromptPlan;
    using sparkpush::HermesCommandKind;
    using sparkpush::ParseHermesCommand;

    if (!Check(ParseHermesCommand(" /NEW --yes demo ").kind ==
                   HermesCommandKind::kNew,
               "/new parsing failed") ||
        !Check(ParseHermesCommand("/reset").kind ==
                   HermesCommandKind::kNew,
               "/reset alias failed") ||
        !Check(ParseHermesCommand("/commands").kind ==
                   HermesCommandKind::kHelp,
               "/commands alias failed") ||
        !Check(ParseHermesCommand("/cann DataCopyPad").kind ==
                   HermesCommandKind::kCann,
               "/cann parsing failed") ||
        !Check(ParseHermesCommand("/kb status").kind ==
                   HermesCommandKind::kKnowledge,
               "/kb parsing failed") ||
        !Check(ParseHermesCommand("普通消息").kind ==
                   HermesCommandKind::kNone,
               "normal message parsed as command")) {
        return 1;
    }

    auto refresh = BuildHermesPromptPlan({}, "/model refresh", 2);
    if (!Check(!refresh.call_model && refresh.local_error.empty() && !refresh.model_override,
               "catalog refresh treated as a model selection")) return 1;
    for (const auto& command : {"/restart", "/restart now", "/restart --yes"}) {
        auto restart = BuildHermesPromptPlan({User(1, "/restart now")}, command, 2);
        if (!Check(!restart.call_model && restart.messages.empty() && restart.local_error.empty() &&
                   restart.command.kind == HermesCommandKind::kRestart, "restart escaped control path")) return 1;
    }
    if (!Check(!BuildHermesPromptPlan({}, "/restart everything", 2).local_error.empty(),
               "restart accepted arbitrary arguments")) return 1;
    std::vector<sparkpush::HermesConversationEntry> reset_history = {
        User(1, "旧问题"), Assistant(2, "旧回答"), User(3, "/new"),
        CommandReply(4, "已开启新上下文"), User(5, "新问题"),
        Assistant(6, "新回答")};
    reset_history[3].has_context_state = true;
    reset_history[3].context_start_seq = 3;
    auto plan = BuildHermesPromptPlan(reset_history, "继续", 7);
    const auto agent_plan = BuildHermesPromptPlan({User(1, "/agent pi")}, "/agent hermes-technical", 2);
    if (!Check(!agent_plan.call_model && agent_plan.command.kind == HermesCommandKind::kAgent &&
               agent_plan.command.arguments == "hermes-technical" && agent_plan.messages.empty(),
               "Agent switch entered model context")) return 1;
    if (!Check(plan.call_model, "normal message should call model") ||
        !Check(plan.context_start_seq == 3, "reset boundary was not retained") ||
        !Check(plan.messages.size() == 3, "old context was not removed") ||
        !Check(plan.messages[0].second == "新问题", "wrong context after /new") ||
        !Check(plan.messages[2].second == "继续", "current prompt missing")) {
        return 1;
    }

    reset_history[3].has_context_state = false;
    reset_history[3].legacy_new_confirmation = true;
    plan = BuildHermesPromptPlan(reset_history, "继续", 7);
    if (!Check(plan.context_start_seq == 3, "legacy successful new boundary lost on upgrade")) return 1;
    std::vector<sparkpush::HermesConversationEntry> retry_history = {
        User(1, "问题 A"), Assistant(2, "回答 A")};
    plan = BuildHermesPromptPlan(retry_history, "/retry", 3);
    if (!Check(!plan.call_model && plan.messages.size() == 2, "retry preview changed context")) return 1;
    plan = BuildHermesPromptPlan({}, "/retry now", 4);
    if (!Check(plan.call_model, "confirmed retry must use runtime history even when IM window is empty")) return 1;
    plan = BuildHermesPromptPlan(retry_history, "/new", 4);
    if (!Check(!plan.call_model && plan.context_start_seq == 0 && plan.messages.size() == 2,
               "new preview reset context")) return 1;
    plan = BuildHermesPromptPlan({User(1, "old"), User(2, "/new now")}, "next", 3);
    if (!Check(plan.context_start_seq == 0 && plan.messages.size() == 2, "unconfirmed new reset context")) return 1;
    plan = BuildHermesPromptPlan({}, "/thinking high", 4);
    if (!Check(!plan.call_model && plan.command.name == "reasoning", "thinking alias called LLM")) return 1;
    plan = BuildHermesPromptPlan({}, "/new unsupported-name", 4);
    if (!Check(!plan.local_error.empty(), "unsupported new argument was silently accepted")) return 1;

    plan = BuildHermesPromptPlan({}, "/model zai:glm-5", 1);
    if (!Check(!plan.call_model && plan.model_override,
               "/model should be handled locally") ||
        !Check(plan.provider == "zai" && plan.model == "glm-5",
               "provider:model parsing failed")) {
        return 1;
    }

    auto model_snapshot = Assistant(2, "模型回答");
    model_snapshot.command_reply = true;
    model_snapshot.has_model_state = true;
    model_snapshot.model_override = true;
    model_snapshot.provider = "zai";
    model_snapshot.model = "glm-5";
    plan = BuildHermesPromptPlan({model_snapshot}, "下一问", 3);
    if (!Check(plan.model_override && plan.provider == "zai" &&
                   plan.model == "glm-5",
               "model state snapshot was not restored")) {
        return 1;
    }

    plan = BuildHermesPromptPlan({model_snapshot, User(3, "/model broken:missing")}, "下一问", 4);
    if (!Check(plan.model == "glm-5" && plan.provider == "zai",
               "unconfirmed user command replaced confirmed model")) return 1;
    plan = BuildHermesPromptPlan({}, "/model bare-alias", 1);
    if (!Check(!plan.local_error.empty(), "ambiguous bare model accepted")) return 1;

    plan = BuildHermesPromptPlan({model_snapshot}, "/new", 3);
    if (!Check(plan.model_override && plan.model == "glm-5",
               "/new preview must preserve model override") ||
        !Check(!plan.call_model && plan.messages.empty(),
               "/new should not enter the model prompt")) {
        return 1;
    }

    plan = BuildHermesPromptPlan({}, "/model --global", 1);
    if (!Check(!plan.local_error.empty(),
               "unsupported global model change was accepted")) {
        return 1;
    }

    plan = BuildHermesPromptPlan({}, "/does-not-exist", 1);
    if (!Check(!plan.call_model &&
                   plan.command.kind == HermesCommandKind::kUnknown,
               "unknown slash command reached the model")) {
        return 1;
    }

    plan = BuildHermesPromptPlan({}, "/cann DataCopyPad 非对齐搬运", 1);
    if (!Check(plan.call_model && plan.force_knowledge_retrieval,
               "/cann should force a model knowledge lookup") ||
        !Check(plan.messages.size() == 1 &&
                   plan.messages[0].second.find("DataCopyPad") !=
                       std::string::npos,
               "/cann question missing from prompt")) {
        return 1;
    }

    plan = BuildHermesPromptPlan({}, "/kb status", 1);
    if (!Check(plan.call_model && plan.force_knowledge_retrieval,
               "/kb status should reach the read-only agent tool")) {
        return 1;
    }

    plan = BuildHermesPromptPlan({}, "/kb rebuild", 1);
    if (!Check(!plan.call_model && !plan.local_error.empty(),
               "/kb rebuild must be rejected in chat")) {
        return 1;
    }

    std::cout << "hermes command tests passed\n";
    return 0;
}
