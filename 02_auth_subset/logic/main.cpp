#include <grpcpp/grpcpp.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>

#include <algorithm>
#include <cstdio>
#include <string>
#include <thread>

#include "config.h"
#include "grpc_service.h"
#include "http_server.h"
#include "logging.h"
#include "mysql_pool.h"
#include "redis_pool.h"
#include "redis_store.h"
#include "user_dao.h"

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/logic.conf。\n",
                 prog);
}

ArgParseResult ParseConfigPath(int argc, char** argv,
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
    // MySQL
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
    mc.max_pool_size = (cfg.mysql_pool_max_size > 0)
                           ? cfg.mysql_pool_max_size
                           : std::max(mc.min_pool_size, mc.pool_size);
    mc.idle_timeout_ms = cfg.mysql_idle_timeout_ms;
    if (!mysql_pool.Init(mc)) {
        LOG_ERROR << "Failed to init MySQL pool";
        return 1;
    }
    UserDao user_dao(&mysql_pool);

    // Redis
    RedisConnectionPool redis_pool;
    RedisConfig rc;
    rc.host = cfg.redis_host;
    rc.port = cfg.redis_port;
    rc.password = cfg.redis_password;
    rc.db = cfg.redis_db;
    rc.pool_size = cfg.redis_pool_size;
    rc.min_pool_size = (cfg.redis_pool_size > 0) ? cfg.redis_pool_size : 1;
    rc.max_pool_size = (cfg.redis_max_pool_size > 0)
                           ? cfg.redis_max_pool_size
                           : std::max(rc.min_pool_size, cfg.redis_pool_size);
    rc.connect_timeout_ms = cfg.redis_connect_timeout_ms;
    rc.rw_timeout_ms = cfg.redis_rw_timeout_ms;
    rc.idle_timeout_ms = cfg.redis_idle_timeout_ms;
    if (!redis_pool.Init(rc)) {
        LOG_ERROR << "Failed to init Redis pool";
        return 1;
    }
    RedisStore redis_store(&redis_pool);

    // gRPC 服务：仅 VerifyToken
    std::string grpc_addr =
        cfg.listen_addr + ":" + std::to_string(cfg.listen_port);
    grpc::ServerBuilder builder;
    auto service = std::make_unique<LogicServiceImpl>(&redis_store);
    builder.AddListeningPort(grpc_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    LOG_INFO << "Logic gRPC server listening on " << grpc_addr;

    // HTTP 服务：注册/登录
    muduo::net::EventLoop loop;
    muduo::net::InetAddress httpAddr(cfg.http_port);
    HttpApiServer httpServer(&loop, httpAddr, &user_dao, &redis_store);
    httpServer.start();
    LOG_INFO << "Logic HTTP server listening on port "
             << std::to_string(cfg.http_port);

    std::thread grpc_thread([&server]() { server->Wait(); });
    loop.loop();
    if (grpc_thread.joinable()) {
        grpc_thread.join();
    }
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

    sparkpush::InitLogging("logic");
    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    int ret = sparkpush::RunLogic(cfg);
    sparkpush::ShutdownLogging();
    return ret;
}
