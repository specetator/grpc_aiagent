#pragma once

#include <string>

#include <muduo/base/Logging.h>

namespace sparkpush {

struct Config;

// 初始化日志，将 muduo 的日志输出重定向到 logs/<program>.log
// program 建议传入 "logic" / "job" / "comet" 等组件名。
// 返回 true 表示成功打开日志文件，false 表示失败（此时继续输出到 stderr）。
bool InitLogging(const std::string& program);

// 读取配置中的日志项并应用：
// - 设置日志级别（cfg.log_level）
// - 若 cfg.log_file 非空，则切换日志文件（支持占位符）
//
// 支持的占位符（形如 ${name}）：
// - ${program} 组件名（InitLogging 传入的 program）
// - ${pid}     进程 pid
// - ${comet_id}
// - ${listen_addr}
// - ${listen_port}
// - ${http_port}
// - ${comet_grpc_port}
// - ${kafka_consumer_group}
void ApplyLoggingConfig(const std::string& program, const Config& cfg);

// 可选：关闭日志，释放文件句柄（一般在进程退出前调用一次即可）。
void ShutdownLogging();

}  // namespace sparkpush



