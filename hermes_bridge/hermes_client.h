#pragma once

#include <functional>
#include <string>

#include <nlohmann/json.hpp>

#include "config.h"

namespace sparkpush {

class HermesClient {
   public:
    explicit HermesClient(const HermesBridgeConfig& config) : config_(config) {}

    // 非流式兼容路径：完整回答返回后才结束调用。
    bool Chat(const nlohmann::json& messages, std::string* answer,
              std::string* err_msg) const;

    // 流式 Chat Completions：每收到一段 assistant 文本就回调一次。调用方
    // 负责把增量投递给客户端，answer 仍会累积为最终完整回答。
    bool ChatStream(const nlohmann::json& messages,
                   const std::function<void(const std::string&)>& on_delta,
                   std::string* answer, std::string* err_msg) const;

   private:
    HermesBridgeConfig config_;
};

}  // namespace sparkpush
