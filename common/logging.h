#pragma once

#include <string>
#include <cstddef>

#include <muduo/base/Logging.h>

namespace sparkpush {
struct LoggingOptions {
    size_t max_file_bytes{50 * 1024 * 1024};
    int keep_files{5};
    bool console{false};
};

// 初始化日志，将 muduo 的日志输出重定向到 logs/<program>.log
// program 建议传入 "logic" / "job" / "comet" 等组件名。
// 返回 true 表示成功打开日志文件，false 表示失败（此时继续输出到 stderr）。
bool InitLogging(const std::string& program, const LoggingOptions& options = {});

// 可选：关闭日志，释放文件句柄（一般在进程退出前调用一次即可）。
void ShutdownLogging();

}  // namespace sparkpush



