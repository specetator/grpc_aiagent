#pragma once

#include "config.h"

namespace sparkpush {

// RunComet 是 comet 进程的入口封装：
// 1. 负责初始化事件循环、TCP WebSocket 服务与 gRPC 服务。
// 2. 传入的 Config 已经过配置文件解析，包含监听端口、线程数、Logic 服务地址等。
// 3. 该接口会阻塞直到事件循环退出，通常只在 main 中调用一次。
// 4. 不在内部持有全局单例，方便未来做热重启或多实例部署。
// 运行 comet 进程：启动 WebSocket 服务器和 gRPC CometService。
void RunComet(const Config& cfg);

}  // namespace sparkpush



