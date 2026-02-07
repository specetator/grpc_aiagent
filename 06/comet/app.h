#pragma once

#include "config.h"

namespace sparkpush {

// 运行 comet 进程：启动 WebSocket 服务器和 gRPC CometService
void RunComet(const Config& cfg);

}  // namespace sparkpush



