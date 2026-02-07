#pragma once

#include <string>

namespace sparkpush {

struct Config {
  std::string listen_addr;
  int listen_port{0};
  // logic HTTP 端口（如未配置，可回退为 listen_port+1）
  int http_port{0};
  std::string logic_grpc_target;
  std::string kafka_brokers;
  std::string kafka_push_topic;
  std::string kafka_broadcast_topic;
  std::string kafka_consumer_group;

  // Redis/MySQL 配置（连接池参数）
  std::string redis_host;
  int redis_port{6379};
  std::string redis_password;
  int redis_db{0};
  int redis_pool_size{4};

  std::string mysql_host;
  int mysql_port{3306};
  std::string mysql_user;
  std::string mysql_password;
  std::string mysql_db;
  int mysql_pool_size{4};
  int mysql_pool_min_size{0};
  int mysql_pool_max_size{0};
  int mysql_idle_timeout_ms{60000};

  std::string comet_id;
  std::string comet_targets;  // 仅 job 使用，格式：id=addr,id2=addr2

  // comet 进程的 IO 线程数（即 muduo 的 worker EventLoop 个数，>=1）
  int comet_io_threads{4};

  // comet 异步 gRPC 调用线程池大小（用于 VerifyToken、SendUpstreamMessage 等）
  int comet_grpc_pool_size{4};

  // comet 自身提供的 gRPC 服务端口（给 job 调用），如未配置可回退为 listen_port+100
  int comet_grpc_port{0};

  // 是否使用 gRPC 双向流模式（性能更高）- Comet -> Logic
  bool use_grpc_stream{false};
  // 双向流连接数量（多流可提升吞吐）
  int grpc_stream_count{1};
  
  // 是否使用 gRPC 客户端流模式 - Job -> Comet
  bool use_push_stream{false};

  int redis_max_pool_size{0};
  int redis_connect_timeout_ms{2000};
  int redis_rw_timeout_ms{2000};
  int redis_idle_timeout_ms{60000};

  int job_rpc_worker_threads{8};

  // Logic HTTP 服务器 IO 线程数（0=单线程，N=N个工作线程）
  int http_threads{4};
};

// 简单 key=value 文本配置加载
Config LoadConfig(const std::string& path);

}  // namespace sparkpush


