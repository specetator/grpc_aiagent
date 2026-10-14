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
//
// 两个监听器共用一个进程但相互独立：
//   WebSocket（listen_port）        面向客户端，跑在 muduo EventLoop 上
//   gRPC（comet_grpc_port）         面向 Job 下推，由 gRPC 自己的线程池处理
// gRPC handler 里会调 CometServer 的方法，后者用 conns_mu_ 保护共享表，
// 并通过 runInLoop 把实际的 socket 写回 muduo 线程，避开跳线程操作连接。
void RunComet(const Config& cfg) {
    // 校验的是 "job" 角色的密钥：Comet 的 gRPC 服务端只接受 Job 调用，
    // 因此需要的是 job 密钥（用来验证入站请求），而不是 comet 密钥。
    // 放在最前面：密钥没配就直接抛，不要先把端口开起来再失败。
    RequireRpcKey("job");
    auto credentials = InternalServerCredentials(cfg.internal_grpc_listen_addr);
    InstallStopSignals();
    EventLoop loop;
    // 创建 comet 服务器：负责 WebSocket 连接管理 + 上行转发 + 下行推送。
    // 这里不做全局单例，保持生命周期由 main 控制，便于未来做热重启。
    //
    // 注意 server 必须在 loop 之后声明：销毁时逆序，server 先于 loop
    // 销毁，避开 server 的回调在 loop 已亡后还被触发。
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
    // grpcService 只是 server 的薄适配层（鉴权 + 转发），不持有状态，
    // 因此可以安全地放在栈上。但它必须比 grpcServer 活得久：
    // 下面的 Shutdown/Wait 会在 grpcServer 销毁前跑完，顺序是安全的。
    CometServiceImpl grpcService(&server);
    builder.AddListeningPort(addr, credentials);
    // keepalive 参数必须与 Job 客户端配套，否则 Job 发的频繁 ping
    // 会被计入 strike 并触发 GOAWAY（详见 grpc_keepalive.h）。
    ApplyGrpcKeepaliveServerArgs(&builder);
    builder.RegisterService(&grpcService);
    std::unique_ptr<grpc::Server> grpcServer = builder.BuildAndStart();
    if (!grpcServer) throw std::runtime_error("Comet gRPC bind failed");
    LOG_INFO << "Comet gRPC server listening on " << addr;

    // gRPC 自己管理请求线程，主线程运行 muduo 并响应退出信号。
    //
    // 用 runEvery(0.1) 轮询而不是在信号处理函数里直接退：
    // 信号上下文里不能做复杂操作（见 stop_signal.h）。100ms 的轮询
    // 周期就是停机响应延迟的上限。
    loop.runEvery(0.1, [&] { if (stop_requested) loop.quit(); });

    // 必须在创建 EventLoop 的线程中调用 loop()。
    loop.loop();

    // loop 退出后才关 gRPC：给在飞的下推请求 3 秒收尾时间。
    // 若先关 gRPC，正在处理的 PushStream 会被强制中断，
    // Job 侧会把那批消息全部标为失败并重放。
    grpcServer->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
    grpcServer->Wait();
}

}  // namespace sparkpush



