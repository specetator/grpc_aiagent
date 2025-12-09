#include "app.h"

#include "comet_server.h"
#include "logging.h"

namespace sparkpush {

// comet 进程主入口：启动 muduo WebSocket 服务
void RunComet(const Config& cfg) {
    EventLoop loop;
    
    // 创建 comet 服务器：负责 WebSocket 连接管理 + 握手鉴权
    CometServer server(&loop, cfg);
    server.SetThreadNum(cfg.comet_io_threads);
    server.Start();
    
    LOG_INFO << "Comet server listening on port " << std::to_string(cfg.listen_port);

    // 在当前线程运行事件循环
    loop.loop();
}

}  // namespace sparkpush
