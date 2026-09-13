#pragma once

#include <string>

#include <nlohmann/json.hpp>

namespace sparkpush {

// 只保留满足公共 schema、长度上限和 cannkb URI 绑定关系的引用。
nlohmann::json ValidateHermesCitations(const nlohmann::json& input,
                                       std::string* warning = nullptr);

}  // namespace sparkpush
