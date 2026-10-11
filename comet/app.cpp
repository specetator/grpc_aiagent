#include "app.h"

#include "comet_grpc_service.h"
#include "comet_server.h"
#include "grpc_keepalive.h"
#include "logging.h"
#include "stop_signal.h"

#include <grpcpp/grpcpp.h>

#include <thread>

namespace sparkpush {

// comet 进程主入口：启动 muduo WebSocket 服务与 gRPC CometService。
void RunComet(const Config& cfg) {
    RequireRpcKey("job");
    auto credentials = InternalServerCredentials(cfg.internal_grpc_listen_addr);
    InstallStopSignals();
    EventLoop loop;
    // 创建 comet 服务器：负责 WebSocket 连接管理 + 上行转发 + 下行推送。
    // 这里不做全局单例，保持生命周期由 main 控制，便于未来做热重启。
    CometServer server(&loop, cfg);
    server.SetThreadNum(cfg.comet_io_threads);
    server.Start();
    LOG_INFO << "Comet server listening on port " << std::to_string(cfg.listen_port);

    // gRPC 服务器（用于 job 推送）。
    // 端口从配置中的 comet_grpc_port 读取，而不是隐式使用 listen_port+100。
    // 这样便于运维侧在同机多实例场景下手动分配端口，避免冲突。
    std::string addr =
        cfg.internal_grpc_listen_addr + ":" + std::to_string(cfg.comet_grpc_port);
    grpc::ServerBuilder builder;
    CometServiceImpl grpcService(&server);
    builder.AddListeningPort(addr, credentials);
    ApplyGrpcKeepaliveServerArgs(&builder);
    builder.RegisterService(&grpcService);
    std::unique_ptr<grpc::Server> grpcServer = builder.BuildAndStart();
    if (!grpcServer) throw std::runtime_error("Comet gRPC bind failed");
    LOG_INFO << "Comet gRPC server listening on " << addr;

    // gRPC 自己管理请求线程，主线程运行 muduo 并响应退出信号。
    loop.runEvery(0.1, [&] { if (stop_requested) loop.quit(); });

    // 必须在创建 EventLoop 的线程中调用 loop()。
    loop.loop();

    grpcServer->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
    grpcServer->Wait();
}

}  // namespace sparkpush



