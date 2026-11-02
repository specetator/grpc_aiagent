#pragma once

#include <nlohmann/json.hpp>
#include <cstdint>
#include <set>
#include <stdexcept>
#include <string>

namespace sparkpush::media_json {

inline uint64_t Number(const nlohmann::json& input, const char* field,
                       uint64_t maximum, uint64_t fallback = 0) {
    if (!input.contains(field)) return fallback;
    const auto& value = input.at(field);
    if (!value.is_number_integer() ||
        (!value.is_number_unsigned() && value.get<int64_t>() < 0))
        throw std::invalid_argument("integer required");
    const auto number = value.get<uint64_t>();
    if (number > maximum) throw std::invalid_argument("integer outside range");
    return number;
}

// Optional fields may be absent; required fields are read by each operation.
inline bool Fields(const nlohmann::json& input, const std::set<std::string>& allowed) {
    if (!input.is_object()) return false;
    for (const auto& field : input.items())
        if (!allowed.count(field.key())) return false;
    return true;
}

inline bool Hex(const std::string& value) {
    return value.size() == 64 &&
           value.find_first_not_of("0123456789abcdef") == std::string::npos;
}

}  // namespace sparkpush::media_json
