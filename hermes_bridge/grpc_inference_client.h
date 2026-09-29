#pragma once

#include <atomic>

#include "hermes_client.h"

namespace sparkpush {

class GrpcInferenceClient final : public ModelClient {
 public:
    GrpcInferenceClient(const HermesBridgeConfig& config,
                        const std::atomic<bool>* running)
        : config_(config), running_(running) {}
    bool Chat(const nlohmann::json& messages, const HermesChatOptions& options,
              HermesChatResult* result, std::string* err_msg) const override;
    bool ChatStream(const nlohmann::json& messages,
                    const HermesChatOptions& options,
                    const std::function<void(const std::string&)>& on_delta,
                    HermesChatResult* result, std::string* err_msg) const override;

 private:
    HermesBridgeConfig config_;
    const std::atomic<bool>* running_;
};

}  // namespace sparkpush
