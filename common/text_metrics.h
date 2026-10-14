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
//
// 为何两个数字都要：模型输出的「长度被截断」排障时，必须区分
// 「字节超限」还是「字符超限」。中文一个字符占 3 字节，只看字节数
// 会把 500 字的中文回答误判成 1500 字，从而错估 max_tokens 配置。
// length_audit 日志（见 message_service.cpp）同时打这两个值就是为了这个。
struct TextMetrics {
    std::size_t utf8_bytes{0};
    std::size_t unicode_chars{0};
};

// 统计 UTF-8 字节数与码点数。
//
// 判定逻辑：(byte & 0xC0) != 0x80 意为「不是续字节」。
// UTF-8 里续字节的形如 10xxxxxx，而首字节是 0xxxxxxx / 110xxxxx /
// 1110xxxx / 11110xxx。因此「非续字节」的个数恰好等于码点数。
//
// 这个实现故意**不做完整 UTF-8 校验**：非法序列也会给出一个稳定
// 的诊断值而不报错。原因是本函数只用于日志/审计，绝不能因为
// 模型返回了脏字节就抛异常中断消息落库。
//
// noexcept + O(n) 单遍扫描，无内存分配，可安全用在热路径。
inline TextMetrics MeasureText(std::string_view value) noexcept {
    TextMetrics result;
    result.utf8_bytes = value.size();
    for (unsigned char byte : value) {
        if ((byte & 0xC0u) != 0x80u) ++result.unicode_chars;
    }
    return result;
}

// 只要码点数的便捷包装。
inline std::size_t UnicodeCharCount(std::string_view value) noexcept {
    return MeasureText(value).unicode_chars;
}

// 长度审计开关，由环境变量 SPARK_PUSH_LENGTH_AUDIT 控制。
//
// 用局部 static + lambda 做一次性初始化：C++11 保证线程安全，
// 且 getenv 只在首次调用时执行一次，避开热路径上的重复系统调用。
// 代价是运行中改环境变量不会生效，必须重启进程——对审计开关而言
// 这是可接受的（也避免了中途开关导致日志不完整）。
//
// 默认关闭：length_audit 会为每条消息多打一行包含完整字节/字符统计
// 的日志，长期开启会显著放大日志量。
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
