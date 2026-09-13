#pragma once

#include <cstdint>
#include <string>
#include <map>

#include "rate_limiter.h"

namespace sparkpush {

// 配置项集中描述所有组件启动所需的参数，便于统一加载和透传。
struct Config {
    // 监听地址与端口
    std::string listen_addr;
    int listen_port{0};

    // logic 对外的 HTTP 端口（未配置时回退为 listen_port + 1）
    int http_port{0};
    // Job / Comet 的本机 metrics 端口；Logic 复用 http_port /metrics。
    int metrics_port{0};
    std::string logic_grpc_target;

    // Kafka 相关主题与消费组
    std::string kafka_brokers;
    // 单聊和群聊使用独立 topic / consumer group，避免大群或广播挤占单聊。
    std::string kafka_single_topic;
    std::string kafka_group_topic;
    // 兼容旧配置的统一推送 topic；新配置应优先填写上面两个 topic。
    std::string kafka_push_topic;
    std::string kafka_broadcast_topic;
    std::string kafka_persist_topic;
    // Hermes Bridge 使用的请求/回复 topic；只有 hermes_enabled 时启用。
    std::string kafka_ai_request_topic;
    // Hermes 流式增量 topic；仅用于实时展示，不进入历史持久化。
    std::string kafka_ai_delta_topic;
    std::string kafka_ai_reply_topic;
    std::string kafka_consumer_group;

    // Redis 连接池参数
    std::string redis_host;
    int redis_port{6379};
    std::string redis_password;
    int redis_db{0};
    int redis_pool_size{4};
    int redis_max_pool_size{0};
    int redis_connect_timeout_ms{2000};
    int redis_rw_timeout_ms{2000};
    int redis_idle_timeout_ms{60000};
    // 热连接无需每次借出都 PING；仅空闲超过该时间后做健康检查。
    int redis_health_check_interval_ms{30000};

    // MySQL 连接池参数
    std::string mysql_host;
    int mysql_port{3306};
    std::string mysql_user;
    std::string mysql_password;
    std::string mysql_db;
    int mysql_pool_size{4};
    int mysql_pool_min_size{0};
    int mysql_pool_max_size{0};
    int mysql_idle_timeout_ms{60000};

    // comet 服务配置
    std::string comet_id;
    // 仅 job 进程使用，格式：id=addr,id2=addr2
    std::string comet_targets;
    // muduo worker EventLoop 数量
    int comet_io_threads{4};
    // comet gRPC 端口（未配置时回退为 listen_port + 100）
    int comet_grpc_port{0};
    // Comet 异步 Unary gRPC 线程池
    int comet_grpc_pool_size{4};
    // Comet↔Logic 双向流（对齐 06）
    bool use_grpc_stream{false};
    int grpc_stream_count{4};

    // Job -> Comet 长连接推送。
    bool use_push_stream{true};
    int push_stream_queue_max{10000};
    int push_stream_reconnect_base_ms{200};
    int push_stream_reconnect_max_ms{5000};
    int push_rpc_deadline_ms{2000};

    // Logic 写入持久化 topic 时等待 broker delivery report 的超时。
    int persist_kafka_timeout_ms{5000};

    // 单聊/群聊/广播分别使用令牌桶，避免广播流量挤占单聊资源。
    RateLimitConfig rate_limit;

    // 可选管理账号；口令仅从环境变量注入，未配置时管理登录禁用。
    std::string admin_account;
    std::string admin_password;

    // Hermes Bot 集成。API Key 和 Hermes 地址由 Bridge 进程单独读取，
    // Logic 只负责把 AI 请求写入 Kafka 并消费 AI 回复。
    bool hermes_enabled{false};
    int64_t hermes_bot_user_id{900000000001LL};
    // Additional independent Agent contacts; the primary Pi contact keeps its ID.
    std::map<int64_t, std::string> agent_bot_users{{900000000101LL, "Hermes · technical"}};
    // 只读原文接口读取 cann-rag 当前 generation；客户端不能提交文件路径。
    std::string cann_knowledge_root;

};

// 简单 key=value 文本配置加载，读取失败则返回内置默认值。
Config LoadConfig(const std::string& path);

}  // namespace sparkpush
