#pragma once

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <string_view>

namespace sparkpush {

// UTF-8 byte count and Unicode code-point count are intentionally reported
// separately. A code point is counted at each non-continuation byte; malformed
// UTF-8 therefore still produces a stable diagnostic count without changing
// the stored payload.
struct TextMetrics {
    std::size_t utf8_bytes{0};
    std::size_t unicode_chars{0};
};

inline TextMetrics MeasureText(std::string_view value) noexcept {
    TextMetrics result;
    result.utf8_bytes = value.size();
    for (unsigned char byte : value) {
        if ((byte & 0xC0u) != 0x80u) ++result.unicode_chars;
    }
    return result;
}

inline std::size_t UnicodeCharCount(std::string_view value) noexcept {
    return MeasureText(value).unicode_chars;
}

inline bool LengthAuditEnabled() noexcept {
    static const bool enabled = [] {
        const char* value = std::getenv("SPARK_PUSH_LENGTH_AUDIT");
        return value && (std::strcmp(value, "1") == 0 ||
                         std::strcmp(value, "true") == 0 ||
                         std::strcmp(value, "TRUE") == 0);
    }();
    return enabled;
}

}  // namespace sparkpush
