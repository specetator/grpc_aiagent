#include <grpcpp/grpcpp.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>

#include <algorithm>
#include <cstdio>
#include <string>
#include <thread>

#include "config.h"
#include "conversation_store.h"
#include "audit_log_dao.h"
#include "danmaku_dao.h"
#include "group_dao.h"
#include "grpc_service.h"
#include "http_server.h"
#include "kafka_consumer.h"
#include "kafka_producer.h"
#include "logging.h"
#include "message_dao.h"
#include "mysql_pool.h"
#include "redis_pool.h"
#include "redis_store.h"
#include "session_dao.h"
#include "thread_pool.h"
#include "user_dao.h"
#include "user_session_state_dao.h"

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/logic.conf。\n",
                 prog);
}

// 解析命令行配置路径，支持 -c/--config/位置参数
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

// 启动逻辑服务：初始化 Kafka/MySQL/Redis、构造服务并启动 gRPC/HTTP
int RunLogic(const Config& cfg) {
    KafkaProducer single_producer;
    if (!single_producer.Init(cfg.kafka_brokers, cfg.kafka_single_topic)) {
        LOG_ERROR << "Failed to initialize Kafka single producer";
        return 1;
    }

    KafkaProducer group_producer;
    if (!group_producer.Init(cfg.kafka_brokers, cfg.kafka_group_topic)) {
        LOG_ERROR << "Failed to initialize Kafka group producer";
        return 1;
    }

    // 广播任务 Kafka producer（写入 broadcast_task topic）
    KafkaProducer broadcast_producer;
    if (!broadcast_producer.Init(cfg.kafka_brokers,
                                 cfg.kafka_broadcast_topic)) {
        LOG_ERROR << "Failed to initialize Kafka broadcast producer";
        return 1;
    }

    // 持久化 outbox topic：Logic 只负责等待 Kafka delivery report，MySQL
    // 写入转移到 Job，避免 Logic 崩溃时本地异步任务丢失。
    KafkaProducer persist_producer;
    if (!persist_producer.Init(cfg.kafka_brokers, cfg.kafka_persist_topic)) {
        LOG_ERROR << "Failed to initialize Kafka persist producer";
        return 1;
    }

    // Hermes 请求 producer 只在启用集成时创建；回复 consumer 在 Logic
    // 内部启动，使 Hermes 回复可以复用普通单聊的落盘与 Comet 路由。
    KafkaProducer hermes_request_producer;
    if (cfg.hermes_enabled &&
        !hermes_request_producer.Init(cfg.kafka_brokers,
                                      cfg.kafka_ai_request_topic)) {
        LOG_ERROR << "Failed to initialize Kafka Hermes request producer";
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
    mc.max_pool_size = (cfg.mysql_pool_max_size > 0)
                           ? cfg.mysql_pool_max_size
                           : std::max(mc.min_pool_size, mc.pool_size);
    mc.idle_timeout_ms = cfg.mysql_idle_timeout_ms;
    if (!mysql_pool.Init(mc)) {
        LOG_ERROR << "Failed to init MySQL pool";
        return 1;
    }
    UserDao user_dao(&mysql_pool);
    std::string user_schema_err;
    if (!user_dao.EnsureUserCenterSchema(&user_schema_err)) {
        LOG_ERROR << "Failed to ensure user center schema: "
                  << user_schema_err;
        return 1;
    }
    std::string hermes_user_err;
    if (!user_dao.EnsureHermesBotUser(cfg.hermes_bot_user_id,
                                      kHermesBotAccount, kHermesBotName,
                                      &hermes_user_err)) {
        LOG_ERROR << "Failed to ensure Hermes Bot user: " << hermes_user_err;
        return 1;
    }
    AuditLogDao audit_log_dao(&mysql_pool);
    for (const auto& bot : cfg.agent_bot_users) {
        if (bot.first == cfg.hermes_bot_user_id) continue;
        if (!user_dao.EnsureHermesBotUser(bot.first, "agent_bot_" + std::to_string(bot.first),
                                        bot.second, &hermes_user_err)) {
            LOG_ERROR << "Failed to ensure Agent contact: " << hermes_user_err;
            return 1;
        }
    }
    std::string audit_schema_err;
    if (!audit_log_dao.EnsureSchema(&audit_schema_err)) {
        LOG_ERROR << "Failed to ensure audit log schema: " << audit_schema_err;
        return 1;
    }
    DanmakuDao danmaku_dao(&mysql_pool);
    GroupDao group_dao(&mysql_pool);
    GroupMemberDao group_member_dao(&mysql_pool);
    SessionDao session_dao(&mysql_pool);
    MessageDao message_dao(&mysql_pool);
    UserSessionStateDao state_dao(&mysql_pool);
    std::string state_schema_err;
    if (!state_dao.EnsureDeliveredSeqColumn(&state_schema_err)) {
        LOG_ERROR << "Failed to ensure delivered cursor schema: "
                  << state_schema_err;
        return 1;
    }

    // 初始化 Redis 连接池
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
    rc.health_check_interval_ms = cfg.redis_health_check_interval_ms;
    if (!redis_pool.Init(rc)) {
        LOG_ERROR << "Failed to init Redis pool";
        return 1;
    }
    RedisStore redis_store(&redis_pool);

    ConversationStore conversation_store(&session_dao, &message_dao, &state_dao,
                                         &redis_store);

    // 启动 gRPC 服务器（LogicService）
    std::string grpc_addr =
        cfg.listen_addr + ":" + std::to_string(cfg.listen_port);
    grpc::ServerBuilder builder;
    auto service = std::make_unique<LogicServiceImpl>(
        &conversation_store, &group_member_dao, &user_dao, &single_producer,
        &group_producer, &broadcast_producer, &persist_producer, &redis_store,
        cfg.rate_limit, cfg.persist_kafka_timeout_ms,
        cfg.hermes_enabled ? &hermes_request_producer : nullptr,
        cfg.hermes_enabled, cfg.hermes_bot_user_id, cfg.agent_bot_users);

    KafkaConsumer hermes_reply_consumer;
    KafkaConsumer hermes_delta_consumer;
    if (cfg.hermes_enabled) {
        KafkaConsumer::Options hermes_consumer_options;
        hermes_consumer_options.enable_auto_commit = false;
        hermes_consumer_options.auto_offset_reset = "earliest";
        hermes_consumer_options.max_processing_attempts = 3;
        hermes_consumer_options.dead_letter_topic = cfg.kafka_ai_reply_topic + ".dlq";
        if (!hermes_reply_consumer.Init(
                cfg.kafka_brokers,
                cfg.kafka_consumer_group + "_hermes_reply",
                cfg.kafka_ai_reply_topic,
                [&service](const std::string& key, const std::string& payload) {
                    return service->HandleHermesReply(payload, key);
                },
                hermes_consumer_options)) {
            LOG_ERROR << "Failed to initialize Kafka Hermes reply consumer";
            return 1;
        }
        // ai_delta is ephemeral; it must not use the durable reply DLQ.
        hermes_consumer_options.dead_letter_topic.clear();
        if (!hermes_delta_consumer.Init(
                cfg.kafka_brokers,
                cfg.kafka_consumer_group + "_hermes_delta",
                cfg.kafka_ai_delta_topic,
                [&service](const std::string& key, const std::string& payload) {
                    return service->HandleHermesDelta(payload, key);
                },
                hermes_consumer_options)) {
            LOG_ERROR << "Failed to initialize Kafka Hermes delta consumer";
            return 1;
        }
    }
    builder.AddListeningPort(grpc_addr, grpc::InsecureServerCredentials());
    builder.RegisterService(service.get());
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    LOG_INFO << "Logic gRPC server listening on " << grpc_addr;

    // 启动 HTTP 服务器（登录 / 发送消息）
    muduo::net::EventLoop loop;
    // HTTP 监听端口从配置中读取（http_port），避免隐式使用 listen_port+1
    muduo::net::InetAddress httpAddr(cfg.http_port);
    HttpApiServer httpServer(&loop, httpAddr, &conversation_store, &user_dao,
                             &group_dao, &group_member_dao, &redis_store,
                             &group_producer, &broadcast_producer, &danmaku_dao,
                             &audit_log_dao, cfg.cann_knowledge_root,
                             cfg.admin_account,
                             cfg.admin_password);
    httpServer.start();
    LOG_INFO << "Logic HTTP server listening on port "
             << std::to_string(cfg.http_port);

    // gRPC 使用单独线程阻塞 Wait，muduo EventLoop 在当前线程运行
    std::thread grpc_thread([&server]() { server->Wait(); });

    if (cfg.hermes_enabled) {
        hermes_reply_consumer.Start();
        hermes_delta_consumer.Start();
        LOG_INFO << "Hermes integration enabled, bot_user_id="
                 << std::to_string(cfg.hermes_bot_user_id)
                 << ", request_topic=" << cfg.kafka_ai_request_topic
                 << ", delta_topic=" << cfg.kafka_ai_delta_topic
                 << ", reply_topic=" << cfg.kafka_ai_reply_topic;
    }

    // 必须在创建 EventLoop 的线程中调用 loop()
    loop.loop();

    // 如果有优雅退出逻辑，可在此处 server->Shutdown() 后再 join
    if (grpc_thread.joinable()) {
        grpc_thread.join();
    }
    hermes_reply_consumer.Stop();
    hermes_delta_consumer.Stop();
    return 0;
}

}  // namespace sparkpush

// 程序入口：解析配置路径，加载配置并启动逻辑服务
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
