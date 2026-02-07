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

inline void LogInfo(const std::string& msg) {
  LOG_INFO << msg;
}

inline void LogError(const std::string& msg) {
  LOG_ERROR << msg;
}

inline void LogWarn(const std::string& msg) {
  LOG_WARN << msg;
}

}  // namespace sparkpush



