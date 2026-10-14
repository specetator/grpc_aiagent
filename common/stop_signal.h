#pragma once
#include <csignal>
namespace sparkpush {

// 统一的优雅停机开关。
//
// 用 volatile sig_atomic_t 而不是 atomic<bool>：这是 C++ 标准中唯一
// 保证在信号处理函数里读写安全的类型（信号可能在任意指令间插入，
// 普通 atomic 的 lock 前缀在部分平台上仍可能死锁）。
//
// inline 变量（C++17）让多个翻译单元包含本头时仍共享同一实例。
inline volatile std::sig_atomic_t stop_requested = 0;

// 信号处理函数。只置位，不做任何实际清理：
// 日志刷盘、Kafka 提交、gRPC Shutdown 都可能阻塞或分配内存，
// 在信号上下文里调这些是未定义行为。真正的收尾在主循环里做。
inline void StopSignalHandler(int) { stop_requested = 1; }

// 注册 SIGINT（Ctrl-C）与 SIGTERM（docker stop / sparkctl down）。
// 先把标志清零，保证同一进程内重复安装不会带着上次的停机状态。
//
// 调用方需在主循环里周期检查 stop_requested（参见 logic/main.cpp 的
// loop.runEvery 与 comet/app.cpp），因此停机延迟最多一个轮询周期。
inline void InstallStopSignals() {
    stop_requested = 0;
    std::signal(SIGINT, StopSignalHandler);
    std::signal(SIGTERM, StopSignalHandler);
}
}  // namespace sparkpush
