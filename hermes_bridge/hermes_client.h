#pragma once

#include <cstdint>
#include <functional>
#include <string>

#include <nlohmann/json.hpp>

#include "config.h"

namespace sparkpush {

struct HermesChatOptions {
    // 为空时使用 Bridge 的默认模型/provider。provider 非空时按 Hermes
    // API 的 provider:model 直连语义发送。
    std::string model;
    std::string provider;
    // Spark 会话标识，供 Pi gateway 复用同一 Agent session 和工具历史。
    std::string session_id;
    // 端到端 turn 幂等键。Gateway 使用它恢复已完成结果，绝不因 Bridge
    // 重启而用同一请求重复调用 Agent 工具。
    std::string request_id;
    // Trusted Logic scope. Not accepted from a public chat message body.
    // 译：这是 Logic 已经信任过的作用域，不从公开聊天消息体里取。
    // 房间、任务这些字段若改成客户端直接传，就等于让用户自己指定协作范围。
    nlohmann::json collaboration;
    int64_t context_start_seq{0};
    bool retry{false};
    std::function<void(const std::string&)> on_progress;
    std::function<void(const nlohmann::json&)> on_agent_event;
    // Agent control uses /agent/control; it never sends a prompt to the model.
    // 译：控制命令走 /agent/control，绝不把提示词送给模型。
    // Chat() 在 control 非空时会整份替换请求体，不走 /chat/completions。
    nlohmann::json control;
    // Pi RPC images for the current turn only; base64 payloads, never Kafka IDs.
    // 译：只带本轮的 Pi RPC 图片。载荷是 base64，不是 Kafka 里的附件 id。
    // 附件 id 留在 Kafka 上，Bridge 读盘之后才填这个字段。
    nlohmann::json images = nlohmann::json::array();
};

struct HermesChatResult {
    // text 是 Hermes transform_llm_output 之后的权威最终文本。流式 delta
    // 仅用于预览，收到 hermes.final 后必须以此处文本覆盖。
    std::string text;
    // Provider/gateway terminal diagnostics. These are observational fields;
    // they do not change the existing request or message protocol.
    // 译：供应商/网关的终态诊断。这些字段只用于观察，
    // 不改变现有的请求或消息协议。finish_reason 不是 "stop" 时调用失败。
    std::string finish_reason;
    std::size_t stream_chunk_count{0};
    nlohmann::json agent_event{nlohmann::json::object()};
    nlohmann::json citations{nlohmann::json::array()};
    nlohmann::json metadata = nlohmann::json::object();
};

class ModelClient {
   public:
    virtual ~ModelClient() = default;
    virtual bool Chat(const nlohmann::json& messages,
                      const HermesChatOptions& options, HermesChatResult* result,
                      std::string* err_msg) const = 0;
    virtual bool ChatStream(const nlohmann::json& messages,
                    const HermesChatOptions& options,
                    const std::function<void(const std::string&)>& on_delta,
                    HermesChatResult* result, std::string* err_msg) const = 0;
};

class HermesClient : public ModelClient {
   public:
    explicit HermesClient(const HermesBridgeConfig& config) : config_(config) {}

    // 非流式兼容路径：完整回答返回后才结束调用。
    bool Chat(const nlohmann::json& messages,
              const HermesChatOptions& options, HermesChatResult* result,
              std::string* err_msg) const override;

    // 流式 Chat Completions：每收到一段 assistant 文本就回调一次。调用方
    // 负责把增量投递给客户端，answer 仍会累积为最终完整回答。
    bool ChatStream(const nlohmann::json& messages,
                    const HermesChatOptions& options,
                    const std::function<void(const std::string&)>& on_delta,
                    HermesChatResult* result, std::string* err_msg) const override;

   private:
    HermesBridgeConfig config_;
};

}  // namespace sparkpush
