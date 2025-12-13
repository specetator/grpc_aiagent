// ============================================================================
// logic 服务主程序
//
// 功能：
// 1. HTTP 服务：提供用户注册、登录接口（端口独立）
// 2. gRPC 服务：提供 token 验证接口，供 comet 调用（端口独立）
// 3. 依赖：MySQL（用户数据）、Redis（token 和路由）
//
// 架构：
// - HTTP 服务使用 muduo EventLoop，处理客户端注册/登录请求
// - gRPC 服务独立线程运行，处理 comet 的 token 验证请求
// - 两个服务共享 MySQL 和 Redis 连接池
// ============================================================================
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
#include "kafka_producer.h"
#include "logging.h"
#include "mysql_pool.h"
#include "redis_pool.h"
#include "redis_store.h"
#include "room_dao.h"
#include "room_store.h"
#include "user_dao.h"

namespace {

// 命令行参数解析结果
enum class ArgParseResult { kOk, kHelp, kError };

// 打印命令行用法说明
void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/logic.conf。\n",
                 prog);
}

// 解析命令行参数，提取配置文件路径
// 支持两种方式：
// 1. 选项方式：--config path 或 -c path
// 2. 位置参数：直接跟路径
// @return: 解析结果（成功/帮助/错误）
ArgParseResult ParseConfigPath(int argc, char** argv,
                               std::string* config_path) {
    const std::string default_path = "conf/logic.conf";
    *config_path = default_path;
    bool positional_used = false;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-c" || arg == "--config") {
            // 选项方式：后面必须跟配置文件路径
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s 需要一个配置文件路径\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            return ArgParseResult::kHelp;
        } else if (!arg.empty() && arg[0] == '-') {
            // 未知选项
            std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
            return ArgParseResult::kError;
        } else {
            // 位置参数方式（不能重复）
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

// logic 服务主函数：初始化所有依赖，启动 HTTP 和 gRPC 服务
int RunLogic(const Config& cfg) {
    // ========== 1. 初始化 MySQL 连接池 ==========
    MySqlConnectionPool mysql_pool;
    // 配置 MySQL 连接参数
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

    // 初始化连接池并测试连接
    if (!mysql_pool.Init(mc)) {
        LOG_ERROR << "Failed to init MySQL pool";
        return 1;
    }

    // 创建用户 DAO，操作 user 表
    // 初始化 DAO
    UserDao user_dao(&mysql_pool);  // 内存版
    RoomDao room_dao(&mysql_pool);

    KafkaProducer push_producer;
    if (!push_producer.Init(cfg.kafka_brokers, cfg.kafka_push_topic)) {
        LOG_ERROR << "Failed to initialize Kafka push producer";
        return 1;
    }
    // ========== 2. 初始化 Redis 连接池 ==========
    RedisConnectionPool redis_pool;
    // 配置 Redis 连接参数
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

    // 初始化 Redis 连接池
    if (!redis_pool.Init(rc)) {
        LOG_ERROR << "Failed to init Redis pool";
        return 1;
    }

    // 创建 Redis 存储封装，管理 token 和路由
    RedisStore redis_store(&redis_pool);
    RoomStore room_store(&room_dao, &redis_store, 300, 600,
                         (cfg.room_list_prefer_redis != 0));

    // ========== 3. 启动 gRPC 服务（token 验证接口）==========
    std::string grpc_addr =
        cfg.listen_addr + ":" + std::to_string(cfg.listen_port);
    grpc::ServerBuilder builder;
    // 创建 gRPC 服务实现
    auto service = std::make_unique<LogicServiceImpl>(&user_dao, &push_producer,
                                                      &redis_store, &room_store,
                                                      cfg.connection_ttl_ms);
    // 配置监听地址和端口（不使用 TLS，内网通信）
    builder.AddListeningPort(grpc_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());

    // 构建并启动 gRPC 服务器
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    LOG_INFO << "Logic gRPC server listening on " << grpc_addr;

    // ========== 4. 启动 HTTP 服务（注册/登录接口）==========
    muduo::net::EventLoop loop;
    muduo::net::InetAddress httpAddr(cfg.http_port);
    HttpApiServer httpServer(&loop, httpAddr, &user_dao, &redis_store,
                             &room_store, &push_producer);
    httpServer.start();
    LOG_INFO << "Logic HTTP server listening on port "
             << std::to_string(cfg.http_port);

    // ========== 5. 运行服务（双线程模式）==========
    // gRPC 服务在独立线程运行
    std::thread grpc_thread([&server]() { server->Wait(); });

    // HTTP 服务在主线程的事件循环中运行（阻塞）
    loop.loop();

    // 清理：等待 gRPC 线程结束
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