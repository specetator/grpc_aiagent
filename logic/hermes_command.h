#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace sparkpush {

enum class HermesCommandKind {
    kNone,
    kNew,
    kStatus,
    kRetry,
    kRestart,
    kModel,
    kAgent,
    kReasoning,
    kHelp,
    kCann,
    kKnowledge,
    kUnknown,
};

struct HermesCommand {
    HermesCommandKind kind{HermesCommandKind::kNone};
    std::string name;
    std::string arguments;
};

// BuildHermesPromptPlan 使用的轻量历史结构，避免命令解析依赖 DAO/JSON。
struct HermesConversationEntry {
    int64_t msg_seq{0};
    bool assistant{false};
    bool command_reply{false};
    std::string text;

    // Bot 回复会携带当轮实际采用的模型状态。每轮都重复记录，使 /model
    // 即使滚出最近 50 条历史，重启后的 Logic 仍能恢复会话级覆盖值。
    bool legacy_new_confirmation{false};
    bool has_context_state{false};
    int64_t context_start_seq{0};
    bool has_model_state{false};
    bool model_override{false};
    std::string model;
    std::string provider;
};

struct HermesPromptPlan {
    HermesCommand command;
    std::vector<std::pair<std::string, std::string>> messages;
    bool call_model{true};
    bool force_knowledge_retrieval{false};
    bool model_override{false};
    std::string model;
    std::string provider;
    std::string local_error;
    int64_t context_start_seq{0};
    size_t context_message_count{0};
    size_t prompt_chars{0};
};

HermesCommand ParseHermesCommand(const std::string& text);
std::string HermesCommandName(HermesCommandKind kind);

HermesPromptPlan BuildHermesPromptPlan(
    const std::vector<HermesConversationEntry>& history,
    const std::string& current_text, int64_t current_seq,
    size_t max_prompt_chars = 12 * 1024);

}  // namespace sparkpush
