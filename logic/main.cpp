#include <grpcpp/grpcpp.h>
#include <muduo/net/EventLoop.h>
#include <muduo/net/InetAddress.h>

#include <algorithm>
#include <cstdio>
#include <string>
#include <thread>

#include "attachment_dao.h"
#include "audit_log_dao.h"
#include "config.h"
#include "conversation_store.h"
#include "danmaku_dao.h"
#include "group_dao.h"
#include "grpc_keepalive.h"
#include "grpc_service.h"
#include "http_server.h"
#include "repositories/room_agent_repository.h"
#include "services/room_agent_service.h"
#include "kafka_consumer.h"
#include "kafka_producer.h"
#include "logging.h"
#include "message_dao.h"
#include "periodic_worker.h"
#include "stop_signal.h"
#include "metrics.h"
#include "mysql_pool.h"
#include "redis_pool.h"
#include "redis_store.h"
#include "repositories/dao_repositories.h"
#include "services/admin_service.h"
#include "services/attachment_service.h"
#include "services/conversation_service.h"
#include "services/danmaku_service.h"
#include "services/knowledge_service.h"
#include "services/message_service.h"
#include "services/room_service.h"
#include "services/user_service.h"
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
ArgParseResult ParseConfigPath(int argc, char** argv, std::string* config_path) {
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
//
// 【启动顺序的约束】本函数里各步的先后位置不是随意的：
//   1. RequireRpcKey / InternalServerCredentials 先跑：两者都会抛异常，
//      目的是“凭证没配好就不要启动”，而不是起来后所有 RPC 静默 401。
//   2. Kafka producer -> MySQL/Redis 池 -> DAO -> service：依赖方向自下而上。
//   3. schema 校验/建表（EnsureSchema / CheckSchema）在构造 service 之前，
//      任何一项失败都直接 return 1，不开端口。
//   4. Hermes consumer 的 **Init** 在 gRPC 绑定之前（连不上 Kafka 就不开服务），
//      但 **Start** 在服务器启动之后（避开还没准备好就开始消费）。
//   5. spark_push_service_ready 置 1 在最后，之前任何一步失败都不会让
//      /ready 报健康。
//
// 【返回值的含义】return 1 表示“启动失败或运行期故障”，return 0 表示
// “收到停止信号后干净退出”。两者对 systemd/docker 的重启策略意义不同。
int RunLogic(const Config& cfg) {
    // 下面两行是启动门禁，失败会抛 std::runtime_error，由 main() 的
    // try/catch 接住并记日志。这里只验 "comet" 角色密钥：Logic 作为
    // 服务端只需要知道“谁能调我”，不需要自己的客户端密钥。
    // InternalServerCredentials 还会强制一条部署约束：非回环地址必须用 TLS。
    RequireRpcKey("comet");
    auto credentials = InternalServerCredentials(cfg.internal_grpc_listen_addr);
    // 四个业务 producer，每个在 Init 时绑定一个固定 topic。
    // 分四个实例是被接口形状逼出来的：KafkaProducer::Init(brokers, topic)
    // 把 topic 存在成员 topic_name_ 里，发送接口上并没有 topic 参数，
    // 因此一个实例只能对一个 topic。
    // 注意四个实例的 librdkafka 配置是**完全相同**的（acks=all +
    // enable.idempotence，见 common/kafka_producer.cpp），不要误以为
    // 它们有不同的可靠性等级。分实例的代价是四份缓冲区与各自的
    // librdkafka 后台线程。
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
    if (!broadcast_producer.Init(cfg.kafka_brokers, cfg.kafka_broadcast_topic)) {
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
        !hermes_request_producer.Init(cfg.kafka_brokers, cfg.kafka_ai_request_topic)) {
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
        LOG_ERROR << "Failed to ensure user center schema: " << user_schema_err;
        return 1;
    }
    std::string hermes_user_err;
    if (!user_dao.EnsureHermesBotUser(cfg.hermes_bot_user_id, kHermesBotAccount,
                                      kHermesBotName, &hermes_user_err)) {
        LOG_ERROR << "Failed to ensure Hermes Bot user: " << hermes_user_err;
        return 1;
    }
    AuditLogDao audit_log_dao(&mysql_pool);
    for (const auto& bot : cfg.agent_bot_users) {
        if (bot.first == cfg.hermes_bot_user_id) continue;
        if (!user_dao.EnsureHermesBotUser(bot.first,
                                          "agent_bot_" + std::to_string(bot.first),
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
    MySqlRoomAgentRepository room_agent_repository(&mysql_pool, &group_dao, &group_member_dao);
    std::string room_schema_error;
    if (!room_agent_repository.EnsureSchema(&room_schema_error)) {
        LOG_ERROR << "Failed to ensure room Agent schema: " << room_schema_error;
        return 1;
    }
    RoomAgentService room_agent_service(&room_agent_repository, cfg.hermes_bot_user_id,
                                       cfg.hermes_enabled && cfg.room_agent_enabled);
    OperatorTaskService operator_task_service(&room_agent_repository);
    SessionDao session_dao(&mysql_pool);
    MessageDao message_dao(&mysql_pool);
    std::string reservation_schema_error;
    if (!message_dao.reservations().CheckSchema(&reservation_schema_error)) {
        LOG_ERROR << "Message reservation schema missing; apply sql/migrations/20261011_message_reservations.sql after draining old Kafka events";
        return 1;
    }
    UserSessionStateDao state_dao(&mysql_pool);
    AttachmentDao attachment_dao(&mysql_pool, cfg.attachment_dir);
    std::string attachment_schema_err;
    if (!attachment_dao.EnsureSchema(&attachment_schema_err)) {
        LOG_ERROR << "Failed to ensure attachment schema: " << attachment_schema_err;
        return 1;
    }
    std::string state_schema_err;
    if (!state_dao.EnsureDeliveredSeqColumn(&state_schema_err)) {
        LOG_ERROR << "Failed to ensure delivered cursor schema: " << state_schema_err;
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

    // Composition root: storage -> business services -> HTTP/gRPC adapters.
    // Both transports share these exact instances and business-state owners.
    DaoUserRepository users(&user_dao, &redis_store, &audit_log_dao);
    DaoConversationRepository conversations(&conversation_store, &group_member_dao);
    DaoAttachmentRepository attachments(&attachment_dao);
    UserService::Options user_options;
    user_options.admin_account = cfg.admin_account;
    user_options.admin_password = cfg.admin_password;
    user_options.bot_users.insert(cfg.hermes_bot_user_id);
    for (const auto& bot : cfg.agent_bot_users)
        user_options.bot_users.insert(bot.first);
    UserService user_service(&users, std::move(user_options));
    ConversationService conversation_service(&conversations);
    AttachmentService attachment_service(&attachments, &conversation_service);
    MessageService message_service(
        &conversation_store, &group_member_dao, &user_service, &single_producer,
        &group_producer, &broadcast_producer, &persist_producer, &redis_store,
        cfg.rate_limit, cfg.persist_kafka_timeout_ms,
        cfg.hermes_enabled ? &hermes_request_producer : nullptr, cfg.hermes_enabled,
        cfg.hermes_bot_user_id, cfg.agent_bot_users, &attachment_service, &room_agent_service);
    RoomService room_service(&conversation_store, &group_dao, &group_member_dao,
                             &redis_store);
    AdminService admin_service(&user_dao, &redis_store, &audit_log_dao);
    DanmakuService danmaku_service(&conversation_store, &danmaku_dao, &group_member_dao,
                                   &group_producer, &redis_store);
    KnowledgeService knowledge_service(cfg.cann_knowledge_root);

    // 启动 gRPC 服务器（LogicService）
    std::string grpc_addr = cfg.internal_grpc_listen_addr + ":" + std::to_string(cfg.listen_port);
    grpc::ServerBuilder builder;
    auto service = std::make_unique<LogicServiceImpl>(
        &user_service, &conversation_service, &message_service, &room_service);

    KafkaConsumer hermes_reply_consumer;
    KafkaConsumer hermes_delta_consumer;
    if (cfg.hermes_enabled) {
        KafkaConsumer::Options hermes_consumer_options;
        // 三个选项各自针对一个具体风险：
        //   enable_auto_commit=false  手动提交。自动提交会在“已提交但还没
        //                             处理完”的窗口里崩溃时丢消息，而 AI 回复
        //                             丢了用户就会永久等一个不会来的回复。
        //   auto_offset_reset=earliest 新消费组从最早开始。Logic 重启期间
        //                             累积的 AI 回复必须被补上，用 latest
        //                             就会静默丢掉重启窗口里的全部回复。
        //   max_processing_attempts=3 三次失败后进 DLQ，避开一条毒消息
        //                             把消费卡死（无限重试会堵住整个分区）。
        hermes_consumer_options.enable_auto_commit = false;
        hermes_consumer_options.auto_offset_reset = "earliest";
        hermes_consumer_options.max_processing_attempts = 3;
        hermes_consumer_options.dead_letter_topic = cfg.kafka_ai_reply_topic + ".dlq";
        // 两个 consumer 用**不同的消费组**（_hermes_reply / _hermes_delta）：
        // 它们读不同 topic，若共用一个组名，偏移量提交会互相干扰。
        if (!hermes_reply_consumer.Init(
                cfg.kafka_brokers, cfg.kafka_consumer_group + "_hermes_reply",
                cfg.kafka_ai_reply_topic,
                [&message_service](const std::string& key, const std::string& payload) {
                    return message_service.HandleHermesReply(payload, key);
                },
                hermes_consumer_options)) {
            LOG_ERROR << "Failed to initialize Kafka Hermes reply consumer";
            return 1;
        }
        // ai_delta is ephemeral; it must not use the durable reply DLQ.
        //
        // 译：ai_delta 是临时数据，它绝不能复用上面那个持久回复的 DLQ。
        //
        // 为何要 clear 而不是给 delta 也配一个 DLQ：流式增量（打字机效果）
        // 丢了只是少看到一段中间过程，最终的 ai_reply 仍然会完整到达；
        // 而把它们写进 DLQ 会把一个高频、可丢的 topic 的失败记录
        // 永久堆积在磁盘上，而且与真正需要人工处理的 ai_reply 混在一起，
        // 让 DLQ 失去“每一条都值得看”这个性质。
        // 注意这里改的是**同一个 Options 对象**，因此必须在 reply consumer
        // Init 之后才能 clear，顺序不能颠倒。
        hermes_consumer_options.dead_letter_topic.clear();
        if (!hermes_delta_consumer.Init(
                cfg.kafka_brokers, cfg.kafka_consumer_group + "_hermes_delta",
                cfg.kafka_ai_delta_topic,
                [&message_service](const std::string& key, const std::string& payload) {
                    return message_service.HandleHermesDelta(payload, key);
                },
                hermes_consumer_options)) {
            LOG_ERROR << "Failed to initialize Kafka Hermes delta consumer";
            return 1;
        }
    }
    builder.AddListeningPort(grpc_addr, credentials);
    ApplyGrpcKeepaliveServerArgs(&builder);
    builder.RegisterService(service.get());
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) { LOG_ERROR << "Logic gRPC bind failed"; return 1; }
    LOG_INFO << "Logic gRPC server listening on " << grpc_addr;

    // 启动 HTTP 服务器（登录 / 发送消息）
    muduo::net::EventLoop loop;
    // HTTP 监听端口从配置中读取（http_port），避免隐式使用 listen_port+1
    muduo::net::InetAddress httpAddr(cfg.http_port);
    HttpApiServer httpServer(
        &loop, httpAddr,
        {user_service, conversation_service, attachment_service, message_service,
         room_service, admin_service, danmaku_service, knowledge_service,
         &room_agent_service, &operator_task_service}, cfg.logic_http_threads);
    httpServer.start();
    // outbox 崩溃恢复定时器（每 1s）。这是事务性 outbox 模式的另一半：
    // 正常路径上 Submit 会同步走完 PrepareOutbox -> SendAndWait -> Published，
    // 但如果在三步中间崩溃，就会留下 stage=1（payload 已存、Kafka 未确认）
    // 的记录。本定时器把它们扫出来重发，重发用同一个 msg_id，
    // Job 侧幂等落库，因此最终语义是“至少一次事件 + 幂等消费”。
    PeriodicWorker outbox_recovery([&] {
        std::vector<std::pair<Message, std::string>> pending;
        std::string error;
        // 每轮最多 10 条：把单次定时器的耗时封顶。Pending 内部还会先做
        // 两件事：封存超时的 stage=0 预约（10 分钟）与清理已终结的行，
        // 封存必须在扫描之前，否则 SafePrefix 会被这些空洞一直压低。
        if (!message_dao.reservations().Pending(&pending, 10, &error)) {
            LOG_ERROR << "Message outbox scan failed";
            return;
        }
        for (const auto& record : pending) {
            // 两处都是 **break** 而不是 continue，这是本函数最关键的设计：
            //   1. Pending 返回的顺序是 ORDER BY created_at 升序，同一会话内
            //      的重发顺序必须与首次提交顺序一致。跳过失败的那条去发
            //      后面的，就会打乱会话内的消息顺序。
            //   2. SendAndWait 失败通常意味着 Kafka 整体不可用，而它会
            //      阻塞直到 persist_kafka_timeout_ms。不 break 就会在一个
            //      1s 周期的定时器里连续烧十次超时，把定时器线程占死。
            // break 之后本轮放弃，下一秒从队头重试，顺序与耗时都可控。
            if (!persist_producer.SendAndWait(record.first.session_id, record.second,
                                             cfg.persist_kafka_timeout_ms)) break;
            if (!message_dao.reservations().Published(record.first, &error)) break;
        }
    }, std::chrono::milliseconds(1000));
    LOG_INFO << "Logic HTTP server listening on port " << std::to_string(cfg.http_port);

    // gRPC 使用单独线程阻塞 Wait，muduo EventLoop 在当前线程运行
    //
    // InstallStopSignals 放在两个服务器都启动之后：早装的话一个在
    // 启动过程中到达的 SIGTERM 会被记录下来但没人看，晚装则可能错过信号。
    InstallStopSignals();
    bool consumer_failed = false;
    // 就绪信号置 1。/ready 读的就是这个 gauge（见 http_server.cpp），
    // 启动脚本与健康检查都要求它为 1，避开“进程在、但发不出去”。
    MetricsRegistry::Instance().Set("spark_push_service_ready", 1);
    // 0.1s 周期的自监定时器，同时承担两个职责：
    //   1. 监控 Kafka consumer。consumer 死了是**不可降级**的故障：
    //      AI 回复永远不会到达，而进程看起来还活着。因此主动退出，
    //      让 systemd/docker 重启比带着故障跑更好。
    //   2. 监控 stop_requested（由 InstallStopSignals 置位）。
    //
    // 两个分支都先把 ready 置 0 再 loop.quit()，这个顺序很重要：
    // 它让 /ready 在进程真正退出**之前**先变不健康，给上游负载均衡
    // 一个摘掉本节点的窗口。若直接退出，负载均衡会在接下来几秒里
    // 持续把请求发给一个已死的节点。
    // consumer_failed 写成成员外的局部变量而不是当场 return：它要在
    // loop() 返回后被用作进程退出码。
    loop.runEvery(0.1, [&] {
        consumer_failed = cfg.hermes_enabled && (hermes_reply_consumer.failed() || hermes_delta_consumer.failed());
        if (consumer_failed || stop_requested) {
            MetricsRegistry::Instance().Set("spark_push_service_ready", 0);
            loop.quit();
        }
    });

    // consumer 的 Start 放在最后（而不是 Init 之后立即启）：
    // 避开 gRPC/HTTP 还没开始监听就已经在消费 AI 回复并尝试下推。
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
    //
    // 本行阻塞直到上面的定时器调了 loop.quit()。因此它下面的四行
    // 就是整个进程的**关闭序列**，顺序不能随便改：
    //   1. server->Shutdown(now + 3s)  停止接受新 RPC，给在飞的 3 秒完成。
    //      用带 deadline 的重载而不是无参版本：无参版会立即断开所有
    //      在飞调用，而一个正在等 Kafka delivery report 的 Submit 被断开
    //      就会让客户端收到错误、但消息其实已经进了 outbox。
    //   2. server->Wait()  阻塞到 gRPC 真的停了。
    //   3. outbox_recovery.Stop()  **必须在 gRPC 之后**：反过来就会在
    //      还有请求在 Reserve/PrepareOutbox 的时候关掉重发兜底，
    //      那些预约只能等下一次进程启动才被扫到。
    //   4. consumer.Stop()  最后停消费：它会在处理完当前消息后才退。
    // 注意四个 producer 没有显式 Stop：它们是栈上对象，靠析构函数
    // 停 poll 线程并 flush(3000)（见 common/kafka_producer.cpp）。
    // 析构发生在 return 之后，顺序在以上四步之后，因此不会丢消息。
    // 但 flush 只等 3 秒：Kafka 完全不可用时仍可能丢下未发完的事件，
    // 而这些事件已由 outbox 兜住（stage=1 的记录下次启动会被重发）。
    loop.loop();
    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
    server->Wait();
    outbox_recovery.Stop();
    hermes_reply_consumer.Stop();
    hermes_delta_consumer.Stop();
    // 退出码区分“干净关闭”与“consumer 故障”：前者应该被 systemd 当成
    // 正常停止（不触发告警），后者需要进告警。两者都会重启，
    // 但运维需要能从退出码上看出发生了什么。
    return consumer_failed ? 1 : 0;
}

}  // namespace sparkpush

// 程序入口：解析配置路径，加载配置并启动逻辑服务
//
// ret 初值是 1：只有 RunLogic 正常返回才会改写它。因此上面 try 块里
// 任何一步抛异常（RequireRpcKey 密钥不足、InternalServerCredentials 要求
// TLS 而没配、LoadConfig 文件读不了）都会让进程以 1 退出。
//
// InitLogging 在 LoadConfig **之前**：否则配置错误本身就没地方记。
// ShutdownLogging 无论成败与否都要调：异步日志需要 flush，
// 漏了就会丢掉最后几条（往往正是最关键的那几条错误）。
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

    int ret = 1;
    try {
        sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
        ret = sparkpush::RunLogic(cfg);
    } catch (const std::exception& error) {
        LOG_ERROR << "Logic startup/runtime failure: " << error.what();
    }

    sparkpush::ShutdownLogging();
    return ret;
}
