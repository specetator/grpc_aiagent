#include "config.h"
#include "logging.h"

#include "grpc_service.h"
#include "http_server.h"
#include "conversation_store.h"
#include "session_dao.h"
#include "message_dao.h"
#include "user_session_state_dao.h"
#include "kafka_producer.h"
#include "mysql_pool.h"
#include "redis_pool.h"
#include "user_dao.h"
#include "redis_store.h"
#include "group_dao.h"
#include "danmaku_dao.h"

#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>

#include "signal_handler.h"

#include <algorithm>
#include <thread>
#include <string>

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
  std::fprintf(stderr,
               "用法: %s [--config <配置文件>] [config_path]\n"
               "默认配置文件为 conf/logic.conf。\n",
               prog);
}

ArgParseResult ParseConfigPath(int argc,
                               char** argv,
                               std::string* config_path) {
  const std::string default_path = "conf/logic.conf";
  *config_path = default_path;
  bool positional_used = false;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-c" || arg == "--config") {
      if (i + 1 >= argc) {
        std::fprintf(stderr, "%s 需要一个配置文件路径\n", arg.c_str());
        return ArgParseResult::kError;
      }
      *config_path = argv[++i];
    } else if (arg == "-h" || arg == "--help") {
      return ArgParseResult::kHelp;
    } else if (!arg.empty() && arg[0] == '-') {
      std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
      return ArgParseResult::kError;
    } else {
      if (positional_used) {
        std::fprintf(stderr, "重复的配置文件参数: %s\n", arg.c_str());
        return ArgParseResult::kError;
      }
      *config_path = arg;
      positional_used = true;
    }
  }
  return ArgParseResult::kOk;
}

}  // namespace

namespace sparkpush {

int RunLogic(const Config& cfg) {
  KafkaProducer producer;
  if (!producer.Init(cfg.kafka_brokers, cfg.kafka_push_topic)) {
    LogError("Failed to initialize Kafka push producer");
    return 1;
  }

  // 广播任务 Kafka producer（写入 broadcast_task topic）
  KafkaProducer broadcast_producer;
  if (!broadcast_producer.Init(cfg.kafka_brokers, cfg.kafka_broadcast_topic)) {
    LogError("Failed to initialize Kafka broadcast producer");
    return 1;
  }

   // 初始化 MySQL 连接池
  MySqlConnectionPool mysql_pool;
  MySqlConfig mc;
  mc.host = cfg.mysql_host;
  mc.port = cfg.mysql_port;
  mc.user = cfg.mysql_user;
  mc.password = cfg.mysql_password;
  mc.db = cfg.mysql_db;
  mc.pool_size = cfg.mysql_pool_size;
  mc.min_pool_size =
      (cfg.mysql_pool_min_size > 0) ? cfg.mysql_pool_min_size : mc.pool_size;
  mc.max_pool_size =
      (cfg.mysql_pool_max_size > 0)
          ? cfg.mysql_pool_max_size
          : std::max(mc.min_pool_size, mc.pool_size);
  mc.idle_timeout_ms = cfg.mysql_idle_timeout_ms;
  if (!mysql_pool.Init(mc)) {
    LogError("Failed to init MySQL pool");
    return 1;
  }
  UserDao user_dao(&mysql_pool);
  DanmakuDao danmaku_dao(&mysql_pool);
  GroupDao group_dao(&mysql_pool);
  GroupMemberDao group_member_dao(&mysql_pool);
  SessionDao session_dao(&mysql_pool);
  MessageDao message_dao(&mysql_pool);
  UserSessionStateDao state_dao(&mysql_pool);

  // 初始化 Redis 连接池
  RedisConnectionPool redis_pool;
  RedisConfig rc;
  rc.host = cfg.redis_host;
  rc.port = cfg.redis_port;
  rc.password = cfg.redis_password;
  rc.db = cfg.redis_db;
  rc.pool_size = cfg.redis_pool_size;
  rc.min_pool_size =
      (cfg.redis_pool_size > 0) ? cfg.redis_pool_size : 1;
  rc.max_pool_size =
      (cfg.redis_max_pool_size > 0)
          ? cfg.redis_max_pool_size
          : std::max(rc.min_pool_size, cfg.redis_pool_size);
  rc.connect_timeout_ms = cfg.redis_connect_timeout_ms;
  rc.rw_timeout_ms = cfg.redis_rw_timeout_ms;
  rc.idle_timeout_ms = cfg.redis_idle_timeout_ms;
  if (!redis_pool.Init(rc)) {
    LogError("Failed to init Redis pool");
    return 1;
  }
  RedisStore redis_store(&redis_pool);

  ConversationStore conversation_store(&session_dao,
                                       &message_dao,
                                       &state_dao,
                                       &redis_store);

  // 启动 gRPC 服务器（LogicService）
  std::string grpc_addr = cfg.listen_addr + ":" + std::to_string(cfg.listen_port);
  grpc::ServerBuilder builder;
  auto service = std::make_unique<LogicServiceImpl>(
      &conversation_store,
      &group_member_dao,
      &user_dao,
      &producer,
      &broadcast_producer,
      &redis_store);
  builder.AddListeningPort(grpc_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(service.get());
  std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
  LogInfo("Logic gRPC server listening on " + grpc_addr);

  // 启动 HTTP 服务器（登录 / 发送消息）
  muduo::net::EventLoop loop;
  // HTTP 监听端口从配置中读取（http_port），避免隐式使用 listen_port+1
  muduo::net::InetAddress httpAddr(cfg.http_port);
  HttpApiServer httpServer(&loop,
                           httpAddr,
                           &conversation_store,
                           &user_dao,
                           &group_dao,
                           &group_member_dao,
                           &redis_store,
                           &producer,
                           &broadcast_producer,
                           &danmaku_dao);
  httpServer.setThreadNum(cfg.http_threads);
  httpServer.start();
  LogInfo("Logic HTTP server listening on port " +
          std::to_string(cfg.http_port) + " with " +
          std::to_string(cfg.http_threads) + " IO threads");

  // 优雅关闭：收到 SIGTERM/SIGINT 后退出 EventLoop 并关闭 gRPC
  InstallSignalHandler([&loop, &server]() {
    loop.quit();
    if (server) {
      server->Shutdown();
    }
  });

  // gRPC 使用单独线程阻塞 Wait，muduo EventLoop 在当前线程运行
  std::thread grpc_thread([&server]() {
    server->Wait();
  });

  // 必须在创建 EventLoop 的线程中调用 loop()
  loop.loop();

  // 优雅关闭序列
  LogInfo("Logic shutting down gracefully...");
  if (server) {
    server->Shutdown();
  }
  if (grpc_thread.joinable()) {
    grpc_thread.join();
  }
  mysql_pool.Stop();
  LogInfo("Logic shutdown complete");
  return 0;
}

}  // namespace sparkpush

int main(int argc, char** argv) {
  std::string config_path;
  ArgParseResult result = ParseConfigPath(argc, argv, &config_path);
  if (result == ArgParseResult::kHelp) {
    PrintUsage(argv[0]);
    return 0;
  }
  if (result == ArgParseResult::kError) {
    PrintUsage(argv[0]);
    return 1;
  }
  // 初始化日志到独立文件 logs/logic.log
  sparkpush::InitLogging("logic");

  sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
  int ret = sparkpush::RunLogic(cfg);

  sparkpush::ShutdownLogging();
  return ret;
}





