#include "app.h"

#include "comet_grpc_service.h"
#include "comet_server.h"
#include "logging.h"
#include "signal_handler.h"

#include <grpcpp/grpcpp.h>

#include <thread>

namespace sparkpush {

void RunComet(const Config& cfg) {
  EventLoop loop;
  CometServer server(&loop, cfg);
  server.SetThreadNum(cfg.comet_io_threads);
  server.Start();
  LogInfo("Comet server listening on port " +
          std::to_string(cfg.listen_port));

  // gRPC 服务器（用于 job 推送）
  std::string addr =
      cfg.listen_addr + ":" + std::to_string(cfg.comet_grpc_port);
  grpc::ServerBuilder builder;
  CometServiceImpl grpcService(&server);
  builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
  builder.RegisterService(&grpcService);
  std::unique_ptr<grpc::Server> grpcServer = builder.BuildAndStart();
  LogInfo("Comet gRPC server listening on " + addr);

  // 优雅关闭：收到 SIGTERM/SIGINT 后退出 EventLoop 并关闭 gRPC
  InstallSignalHandler([&loop, &grpcServer]() {
    // quit() 是线程安全的，会唤醒 EventLoop
    loop.quit();
    if (grpcServer) {
      grpcServer->Shutdown();
    }
  });

  // gRPC 使用单独线程阻塞 Wait，muduo EventLoop 在当前线程运行
  std::thread grpc_thread([&grpcServer]() {
    grpcServer->Wait();
  });

  // 必须在创建 EventLoop 的线程中调用 loop()
  loop.loop();

  // 优雅关闭序列
  LogInfo("Comet shutting down gracefully...");
  if (grpcServer) {
    grpcServer->Shutdown();
  }
  if (grpc_thread.joinable()) {
    grpc_thread.join();
  }
  LogInfo("Comet shutdown complete");
}

}  // namespace sparkpush



