#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace sparkpush {

// Agent 的斜杠命令类型。用户在单聊里发 "/xxx" 就会被解成其中一种。
//
// kNone 与 kUnknown 的区别很关键，不要混：
//   kNone    不以 '/' 开头，是**普通提问**，应该交给模型
//   kUnknown 以 '/' 开头但不是已知命令，应该回一句「未知命令」，
//            **不能当成提问发给模型**
// 若不区分，用户打错的 "/modle gpt-4" 会被当成普通文本送给模型，
// 而模型很可能会一本正经地把它当提示词处理，用户得到的是一堆无关回答
// 而不是「你命令打错了」。
enum class HermesCommandKind {
    kNone,
    kNew,         // /new  /reset  /clear     开新会话上下文
    kStatus,      // /status                  查当前会话状态
    kRetry,       // /retry                   重试上一次
    kRestart,     // /restart                 重启 Agent 会话
    kModel,       // /model                   查看/切换模型
    kAgent,       // /agent  /agents          查看/切换 agent
    kReasoning,   // /reasoning  /thinking    推理过程开关
    kHelp,        // /help  /commands         命令列表
    kCann,        // /cann                    CANN 相关
    kKnowledge,   // /kb                      知识库检索
    kCreative,    // 创作类（roleplay/rp/write/scene/... 共十多个别名）
    kStop,        // /stop                    停止当前 Hermes run
    kApprove,     // /approve                 一次性允许当前审批
    kDeny,        // /deny                    拒绝当前审批
    kCompact,     // /compact                 仅在原生运行时支持时转发
    kUnknown,
};

// 解析结果。name 会被**归一化**：多个别名映射到同一个规范名
//（/reset 与 /clear 都变成 "new"），因此回复里展示的命令名是一致的。
// 例外是 kCreative：它保留用户原本输入的那个 token，
// 因为十多个创作子命令的语义并不等价，归一化会丢信息。
struct HermesCommand {
    HermesCommandKind kind{HermesCommandKind::kNone};
    std::string name;
    std::string arguments;
};

// BuildHermesPromptPlan 使用的轻量历史结构，避免命令解析依赖 DAO/JSON。
//
// 这个结构存在的理由：命令解析与 prompt 拼接应该是**纯函数**，
// 不碰数据库也不碰 nlohmann::json。因此调用方先把历史从 Message
// 投影成这个最小结构。好处是 tests/hermes_command_test.cpp 能直接
// 构造几十条历史验证上下文裁剪逻辑，不需要任何存储。
struct HermesConversationEntry {
    int64_t msg_seq{0};
    bool assistant{false};
    bool command_reply{false};
    std::string text;

    // Bot 回复会携带当轮实际采用的模型状态。每轮都重复记录，使 /model
    // 即使滚出最近 50 条历史，重启后的 Logic 仍能恢复会话级覆盖值。
    //
    // 这组字段的设计意图值得单独说：它们把「会话级模型覆盖」这个状态
    // **存在消息历史里**而不是单独一张表。好处是无需额外存储且天然随
    // 会话生命周期；代价是必须每轮重复写，否则一旦那条消息滚出
    // 历史窗口，覆盖值就丢了。has_* 标志用来区分「没设过」与「设为空」。
    bool legacy_new_confirmation{false};
    bool has_context_state{false};
    int64_t context_start_seq{0};
    bool has_model_state{false};
    bool model_override{false};
    std::string model;
    std::string provider;
};

// BuildHermesPromptPlan 的输出：一份可以直接发给 Agent 的计划。
struct HermesPromptPlan {
    HermesCommand command;
    // (role, content) 对，role 只有 "user" 与 "assistant"。
    std::vector<std::pair<std::string, std::string>> messages;
    // 为假时本地直接回答，不调模型。/status、/help、/unknown 这类命令
    // 的答案完全在服务端，调模型既浪费又会得到不确定的回答。
    bool call_model{true};
    // /kb 等命令强制走知识检索，不依赖模型自己决定要不要查。
    bool force_knowledge_retrieval{false};
    bool model_override{false};
    std::string model;
    std::string provider;
    // 本地命令的回复文本。非空时直接把它当 Agent 回复发出，不进模型。
    std::string local_error;
    // 本轮上下文从哪个 msg_seq 开始（/new 之后的第一条）。
    int64_t context_start_seq{0};
    // 下面三个是诊断用统计，会进指标/日志，不参与业务判定。
    size_t context_message_count{0};
    size_t prompt_chars{0};
};

// 解析一条用户文本。不以 '/' 开头时返回 kind=kNone（而不是报错）。
// 命令名大小写不敏感（内部会 LowerCopy），但 arguments 保留原样。
HermesCommand ParseHermesCommand(const std::string& text);
std::string HermesCommandName(HermesCommandKind kind);

// 把历史 + 当前输入组装成一份 prompt 计划。
//
// 最关键的不变量（详见 .cpp 里的英文注释）：
//   **用户命令只是意图，不是确认。**
// 也就是说，历史里一条 "/model gpt-4" 的用户消息**不会**改变后续请求的
// 模型状态；只有 Agent 已验证的回复快照才能改。同理，/retry、/restart
// 这类意图也不能变更已确认的上下文。
// 若不守这条，在多人共享的场景下任何人都能靠发一条聊天文本改变
// 会话状态——那本质上就是一种提示注入。
//
// max_prompt_chars 默认 12 KiB。裁剪方式是**从最新往旧反向累加**，
// 装不下就停，因此被丢掉的是最旧的消息，最近的上下文一定保留。
// 最新一条（即用户本次输入）**即使超预算也保留**：
// 静默截断用户刚打的字比 prompt 偏长难查得多。
HermesPromptPlan BuildHermesPromptPlan(
    const std::vector<HermesConversationEntry>& history,
    const std::string& current_text, int64_t current_seq,
    size_t max_prompt_chars = 12 * 1024);

}  // namespace sparkpush
