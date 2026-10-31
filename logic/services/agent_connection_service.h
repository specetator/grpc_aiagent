#pragma once
#include <atomic>
#include <functional>
#include <string>
#include <nlohmann/json.hpp>
#include "services/service_status.h"

namespace sparkpush {
// The only network destination is an operator-owned loopback Gateway.
// Customer URL validation/probing and private credential storage live there.
class AgentConnectionService {
 public:
    using Transport = std::function<nlohmann::json(const nlohmann::json&)>;
    explicit AgentConnectionService(Transport transport) : transport_(std::move(transport)) {}
    static Transport LocalGateway(int port, const std::string& key,
                                  const std::string& path = "/v1/agent/connections");
    ServiceStatus Execute(int64_t actor, const nlohmann::json& input, nlohmann::json* output);
 private:
    Transport transport_;
    std::atomic<int> active_{0};
};
}
