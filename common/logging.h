#pragma once

#include <string>

#include <muduo/base/Logging.h>

namespace sparkpush {

// 初始化日志，将 muduo 的日志输出重定向到 logs/<program>.log
// program 建议传入 "logic" / "job" / "comet" 等组件名。
// 返回 true 表示成功打开日志文件，false 表示失败（此时继续输出到 stderr）。
bool InitLogging(const std::string& program);

// 可选：关闭日志，释放文件句柄（一般在进程退出前调用一次即可）。
void ShutdownLogging();

// 轻量封装，统一日志入口，便于后续替换日志实现
inline void LogInfo(const std::string& msg) {
    LOG_INFO << msg;
}

// 记录错误级别日志，统一入口便于替换实现。
inline void LogError(const std::string& msg) {
    LOG_ERROR << msg;
}

// 记录警告级别日志，提示潜在风险。
inline void LogWarn(const std::string& msg) {
    LOG_WARN << msg;
}

}  // namespace sparkpush



