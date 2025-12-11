// ============================================================================
// comet 服务入口实现
// ============================================================================
#include "app.h"

#include "comet_server.h"
#include "comet_grpc_service.h"
#include "logging.h"

#include <grpcpp/grpcpp.h>
#include <thread>

namespace sparkpush {

// comet 进程主入口函数
// 创建并启动 WebSocket 服务器和 gRPC 服务器，进入事件循环
void RunComet(const Config& cfg) {
    // 1. 创建 muduo 事件循环（单线程主循环）
    EventLoop loop;
    
    // 2. 创建 CometServer 实例
    //    - 绑定到配置的监听端口
    //    - 初始化 gRPC 客户端，连接到 logic 服务
    CometServer server(&loop, cfg);
    
    // 3. 设置 IO 线程数（用于处理连接的读写）
    //    - 主线程负责 accept 新连接
    //    - IO 线程池负责处理已建立连接的数据读写
    server.SetThreadNum(cfg.comet_io_threads);
    
    // 4. 启动 WebSocket 服务器（开始监听端口）
    server.Start();
    
    LOG_INFO << "Comet WebSocket server listening on port " << std::to_string(cfg.listen_port);

    // 5. 启动 gRPC 服务器（在独立线程中运行，接收 job 的推送请求）
    std::string grpc_addr = "0.0.0.0:" + std::to_string(cfg.comet_grpc_port);
    CometServiceImpl grpc_service(&server);
    
    grpc::ServerBuilder builder;
    builder.AddListeningPort(grpc_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(&grpc_service);
    
    std::unique_ptr<grpc::Server> grpc_server(builder.BuildAndStart());
    LOG_INFO << "Comet gRPC server listening on " << grpc_addr;
    
    // 在独立线程中运行 gRPC 服务器
    std::thread grpc_thread([&grpc_server]() {
        grpc_server->Wait();
    });

    // 6. 进入事件循环（阻塞，直到 loop.quit() 被调用）
    //    - 处理新连接、数据读写、定时器等事件
    //    - 所有业务逻辑都在事件回调中执行
    loop.loop();
    
    // 7. 清理：关闭 gRPC 服务器
    grpc_server->Shutdown();
    if (grpc_thread.joinable()) {
        grpc_thread.join();
    }
}

}  // namespace sparkpush
