#pragma once
#include <nlohmann/json.hpp>

#include "services/service_status.h"

namespace sparkpush {
class KnowledgeService {
   public:
    explicit KnowledgeService(std::string root) : root_(std::move(root)) {}
    ServiceStatus Document(const std::string& id, const std::string& chunk,
                           nlohmann::json* result);

   private:
    std::string root_;
};
}  // namespace sparkpush
