#pragma once

#include <string>

namespace sparkpush {

// 配置项集中描述所有组件启动所需的参数，便于统一加载和透传。
struct Config {
    // 监听地址与端口
    std::string listen_addr;
    int listen_port{0};

    // logic 对外的 HTTP 端口（未配置时回退为 listen_port + 1）
    int http_port{0};
    std::string logic_grpc_target;

    // Kafka 相关主题与消费组
    std::string kafka_brokers;
    std::string kafka_push_topic;
    std::string kafka_broadcast_topic;
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

    // comet 保活/路由相关参数（毫秒）
    // - comet_idle_timeout_ms：WebSocket 连接空闲超时（用于剔除长时间无心跳/无业务的连接）
    // - comet_redis_ttl_ms：Comet 写入 Redis 的 user_connections:{uid} 键 TTL
    int comet_idle_timeout_ms{60000};
    int comet_redis_ttl_ms{60000};

    // job RPC worker 线程数
    int job_rpc_worker_threads{8};

    // 连接相关 TTL（毫秒）
    int connection_ttl_ms{60000};
};

// 简单 key=value 文本配置加载，读取失败则返回内置默认值。
Config LoadConfig(const std::string& path);

}  // namespace sparkpush


