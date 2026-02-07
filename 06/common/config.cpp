#include "config.h"

#include "logging.h"

#include <fstream>
#include <iostream>
#include <sstream>

namespace sparkpush {

static void Trim(std::string& s) {
  size_t start = s.find_first_not_of(" \t\r\n");
  size_t end = s.find_last_not_of(" \t\r\n");
  if (start == std::string::npos || end == std::string::npos) {
    s.clear();
  } else {
    s = s.substr(start, end - start + 1);
  }
}

Config LoadConfig(const std::string& path) {
  Config cfg;
  // 默认值
  cfg.listen_addr = "0.0.0.0";
  cfg.listen_port = 9000;
  // logic: 默认 HTTP 端口为 gRPC 端口 + 1
  cfg.http_port = cfg.listen_port + 1;
  cfg.logic_grpc_target = "127.0.0.1:9100";
  cfg.kafka_brokers = "127.0.0.1:9092";
  cfg.kafka_push_topic = "push_to_comet";
  cfg.kafka_broadcast_topic = "broadcast_task";
  cfg.kafka_consumer_group = "spark_push_group";
  cfg.redis_host = "127.0.0.1";
  cfg.redis_port = 6379;
  cfg.redis_password.clear();
  cfg.redis_db = 0;
  cfg.redis_pool_size = 4;
  cfg.redis_max_pool_size = 0;
  cfg.redis_connect_timeout_ms = 2000;
  cfg.redis_rw_timeout_ms = 2000;
  cfg.redis_idle_timeout_ms = 60000;
  cfg.mysql_host = "127.0.0.1";
  cfg.mysql_port = 3306;
  cfg.mysql_user = "root";
  cfg.mysql_password.clear();
  cfg.mysql_db = "spark_push";
  cfg.mysql_pool_size = 4;
  cfg.mysql_pool_min_size = 0;
  cfg.mysql_pool_max_size = 0;
  cfg.mysql_idle_timeout_ms = 60000;
  cfg.comet_id = "comet-1";
  cfg.comet_targets.clear();
  cfg.comet_io_threads = 4;
  cfg.comet_grpc_pool_size = 4;
  // comet: 默认 gRPC 端口为 WebSocket 端口 + 100
  cfg.comet_grpc_port = cfg.listen_port + 100;
  cfg.job_rpc_worker_threads = 8;

  std::ifstream fin(path);
  if (!fin) {
    LogError("LoadConfig open file failed: " + path + ", use defaults");
    return cfg;
  }

  std::string line;
  while (std::getline(fin, line)) {
    Trim(line);
    if (line.empty() || line[0] == '#') continue;
    auto pos = line.find('=');
    if (pos == std::string::npos) continue;
    std::string key = line.substr(0, pos);
    std::string value = line.substr(pos + 1);
    Trim(key);
    Trim(value);

    if (key == "listen_addr") cfg.listen_addr = value;
    else if (key == "listen_port") {
      cfg.listen_port = std::stoi(value);
      // 若尚未显式配置 http_port / comet_grpc_port，则根据新的 listen_port 重新推导默认值
      if (cfg.http_port == 0) {
        cfg.http_port = cfg.listen_port + 1;
      }
      if (cfg.comet_grpc_port == 0) {
        cfg.comet_grpc_port = cfg.listen_port + 100;
      }
    } else if (key == "http_port") {
      cfg.http_port = std::stoi(value);
    }
    else if (key == "logic_grpc_target") cfg.logic_grpc_target = value;
    else if (key == "kafka_brokers") cfg.kafka_brokers = value;
    else if (key == "kafka_push_topic") cfg.kafka_push_topic = value;
    else if (key == "kafka_broadcast_topic") cfg.kafka_broadcast_topic = value;
    else if (key == "kafka_consumer_group") cfg.kafka_consumer_group = value;
    else if (key == "redis_host") cfg.redis_host = value;
    else if (key == "redis_port") cfg.redis_port = std::stoi(value);
    else if (key == "redis_password") cfg.redis_password = value;
    else if (key == "redis_db") cfg.redis_db = std::stoi(value);
    else if (key == "redis_pool_size") cfg.redis_pool_size = std::stoi(value);
    else if (key == "redis_max_pool_size") cfg.redis_max_pool_size = std::stoi(value);
    else if (key == "redis_connect_timeout_ms") cfg.redis_connect_timeout_ms = std::stoi(value);
    else if (key == "redis_rw_timeout_ms") cfg.redis_rw_timeout_ms = std::stoi(value);
    else if (key == "redis_idle_timeout_ms") cfg.redis_idle_timeout_ms = std::stoi(value);
    else if (key == "mysql_host") cfg.mysql_host = value;
    else if (key == "mysql_port") cfg.mysql_port = std::stoi(value);
    else if (key == "mysql_user") cfg.mysql_user = value;
    else if (key == "mysql_password") cfg.mysql_password = value;
    else if (key == "mysql_db") cfg.mysql_db = value;
    else if (key == "mysql_pool_size") cfg.mysql_pool_size = std::stoi(value);
    else if (key == "mysql_pool_min_size") cfg.mysql_pool_min_size = std::stoi(value);
    else if (key == "mysql_pool_max_size") cfg.mysql_pool_max_size = std::stoi(value);
    else if (key == "mysql_idle_timeout_ms") cfg.mysql_idle_timeout_ms = std::stoi(value);
    else if (key == "comet_id") cfg.comet_id = value;
    else if (key == "comet_targets") cfg.comet_targets = value;
    else if (key == "comet_io_threads") cfg.comet_io_threads = std::stoi(value);
    else if (key == "comet_grpc_pool_size") cfg.comet_grpc_pool_size = std::stoi(value);
    else if (key == "comet_grpc_port") cfg.comet_grpc_port = std::stoi(value);
    else if (key == "use_grpc_stream") cfg.use_grpc_stream = (value == "true" || value == "1");
    else if (key == "grpc_stream_count") cfg.grpc_stream_count = std::stoi(value);
    else if (key == "use_push_stream") cfg.use_push_stream = (value == "true" || value == "1");
    else if (key == "job_rpc_worker_threads") cfg.job_rpc_worker_threads = std::stoi(value);
    else if (key == "http_threads") cfg.http_threads = std::stoi(value);
  }

  return cfg;
}

}  // namespace sparkpush


