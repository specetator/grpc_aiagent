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
    int64_t context_start_seq{0};
    bool retry{false};
    std::function<void(const std::string&)> on_progress;
    // Agent control uses /agent/control; it never sends a prompt to the model.
    nlohmann::json control;
};

struct HermesChatResult {
    // text 是 Hermes transform_llm_output 之后的权威最终文本。流式 delta
    // 仅用于预览，收到 hermes.final 后必须以此处文本覆盖。
    std::string text;
    nlohmann::json citations{nlohmann::json::array()};
    nlohmann::json metadata{nlohmann::json::object()};
};

class HermesClient {
   public:
    explicit HermesClient(const HermesBridgeConfig& config) : config_(config) {}

    // 非流式兼容路径：完整回答返回后才结束调用。
    bool Chat(const nlohmann::json& messages,
              const HermesChatOptions& options, HermesChatResult* result,
              std::string* err_msg) const;

    // 流式 Chat Completions：每收到一段 assistant 文本就回调一次。调用方
    // 负责把增量投递给客户端，answer 仍会累积为最终完整回答。
    bool ChatStream(const nlohmann::json& messages,
                    const HermesChatOptions& options,
                    const std::function<void(const std::string&)>& on_delta,
                    HermesChatResult* result, std::string* err_msg) const;

   private:
    HermesBridgeConfig config_;
};

}  // namespace sparkpush
