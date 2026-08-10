#include "config.h"

#include "logging.h"

#include <fstream>
#include <cstdlib>
#include <iostream>
#include <sstream>

namespace sparkpush {

// 删除字符串首尾空白字符，避免解析配置时产生噪声。
// 内部使用引用修改原串，便于后续直接使用。
static void Trim(std::string& s) {
    size_t start = s.find_first_not_of(" \t\r\n");
    size_t end = s.find_last_not_of(" \t\r\n");
    if (start == std::string::npos || end == std::string::npos) {
        s.clear();
    } else {
        s = s.substr(start, end - start + 1);
    }
}

// 展开 ${ENV_NAME}。变量未设置时展开为空串，避免把占位符当成口令使用。
static std::string ExpandEnvironment(const std::string& input) {
    std::string output;
    size_t cursor = 0;
    while (cursor < input.size()) {
        size_t begin = input.find("${", cursor);
        if (begin == std::string::npos) {
            output.append(input, cursor, std::string::npos);
            break;
        }
        output.append(input, cursor, begin - cursor);
        size_t end = input.find('}', begin + 2);
        if (end == std::string::npos) {
            output.append(input, begin, std::string::npos);
            break;
        }
        const std::string name = input.substr(begin + 2, end - begin - 2);
        const char* value = name.empty() ? nullptr : std::getenv(name.c_str());
        if (value) output += value;
        cursor = end + 1;
    }
    return output;
}

static void OverrideFromEnv(const char* name, std::string* value) {
    if (!name || !value) return;
    const char* env_value = std::getenv(name);
    if (env_value) *value = env_value;
}

static void ApplySensitiveEnvOverrides(Config* cfg) {
    if (!cfg) return;
    OverrideFromEnv("SPARK_PUSH_MYSQL_PASSWORD", &cfg->mysql_password);
    OverrideFromEnv("SPARK_PUSH_REDIS_PASSWORD", &cfg->redis_password);
    OverrideFromEnv("SPARK_PUSH_ADMIN_ACCOUNT", &cfg->admin_account);
    OverrideFromEnv("SPARK_PUSH_ADMIN_PASSWORD", &cfg->admin_password);
    const char* hermes_enabled = std::getenv("SPARK_PUSH_HERMES_ENABLED");
    if (hermes_enabled) {
        cfg->hermes_enabled = std::string(hermes_enabled) == "true" ||
                              std::string(hermes_enabled) == "1";
    }
}

// 从给定路径加载 key=value 配置文件，并回填到 Config。
// 若文件不存在或解析失败，保留内置默认值以便组件仍可启动。
Config LoadConfig(const std::string& path) {
    Config cfg;

    // 默认值，确保即便配置文件不存在也能跑起来。
    cfg.listen_addr = "0.0.0.0";
    cfg.listen_port = 9000;
    // logic: 默认 HTTP 端口为 gRPC 端口 + 1
    cfg.http_port = cfg.listen_port + 1;
    cfg.metrics_port = 0;
    cfg.logic_grpc_target = "127.0.0.1:9100";
    cfg.kafka_brokers = "127.0.0.1:9092";
    cfg.kafka_single_topic = "push_single";
    cfg.kafka_group_topic = "push_group";
    cfg.kafka_push_topic = "push_to_comet";
    cfg.kafka_broadcast_topic = "broadcast_task";
    cfg.kafka_persist_topic = "persist_message";
    cfg.kafka_ai_request_topic = "ai_request";
    cfg.kafka_ai_delta_topic = "ai_delta";
    cfg.kafka_ai_reply_topic = "ai_reply";
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
    // comet: 默认 gRPC 端口为 WebSocket 端口 + 100
    cfg.comet_grpc_port = cfg.listen_port + 100;
    cfg.comet_grpc_pool_size = 4;
    cfg.use_grpc_stream = false;
    cfg.grpc_stream_count = 4;
    cfg.use_push_stream = true;
    cfg.push_stream_queue_max = 10000;
    cfg.push_stream_reconnect_base_ms = 200;
    cfg.push_stream_reconnect_max_ms = 5000;
    cfg.push_rpc_deadline_ms = 2000;
    cfg.persist_kafka_timeout_ms = 5000;
    cfg.rate_limit = RateLimitConfig{};
    cfg.admin_account.clear();
    cfg.admin_password.clear();
    cfg.hermes_enabled = false;
    cfg.hermes_bot_user_id = 900000000001LL;

    std::ifstream fin(path);
    if (!fin) {
        LOG_ERROR << "LoadConfig open file failed: " << path << ", use defaults";
        ApplySensitiveEnvOverrides(&cfg);
        return cfg;
    }

    bool has_single_topic = false;
    bool has_group_topic = false;
    bool has_legacy_push_topic = false;
    std::string line;
    while (std::getline(fin, line)) {
        Trim(line);
        if (line.empty() || line[0] == '#') {
            continue;
        }

        auto pos = line.find('=');
        if (pos == std::string::npos) {
            continue;
        }

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);
        Trim(key);
        Trim(value);
        value = ExpandEnvironment(value);

        if (key == "listen_addr") {
            cfg.listen_addr = value;
        } else if (key == "listen_port") {
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
        } else if (key == "metrics_port") {
            cfg.metrics_port = std::stoi(value);
        } else if (key == "logic_grpc_target") {
            cfg.logic_grpc_target = value;
        } else if (key == "kafka_brokers") {
            cfg.kafka_brokers = value;
        } else if (key == "kafka_single_topic") {
            cfg.kafka_single_topic = value;
            has_single_topic = true;
        } else if (key == "kafka_group_topic") {
            cfg.kafka_group_topic = value;
            has_group_topic = true;
        } else if (key == "kafka_push_topic") {
            cfg.kafka_push_topic = value;
            has_legacy_push_topic = true;
        } else if (key == "kafka_broadcast_topic") {
            cfg.kafka_broadcast_topic = value;
        } else if (key == "kafka_persist_topic") {
            cfg.kafka_persist_topic = value;
        } else if (key == "kafka_ai_request_topic") {
            cfg.kafka_ai_request_topic = value;
        } else if (key == "kafka_ai_delta_topic") {
            cfg.kafka_ai_delta_topic = value;
        } else if (key == "kafka_ai_reply_topic") {
            cfg.kafka_ai_reply_topic = value;
        } else if (key == "kafka_consumer_group") {
            cfg.kafka_consumer_group = value;
        } else if (key == "redis_host") {
            cfg.redis_host = value;
        } else if (key == "redis_port") {
            cfg.redis_port = std::stoi(value);
        } else if (key == "redis_password") {
            cfg.redis_password = value;
        } else if (key == "redis_db") {
            cfg.redis_db = std::stoi(value);
        } else if (key == "redis_pool_size") {
            cfg.redis_pool_size = std::stoi(value);
        } else if (key == "redis_max_pool_size") {
            cfg.redis_max_pool_size = std::stoi(value);
        } else if (key == "redis_connect_timeout_ms") {
            cfg.redis_connect_timeout_ms = std::stoi(value);
        } else if (key == "redis_rw_timeout_ms") {
            cfg.redis_rw_timeout_ms = std::stoi(value);
        } else if (key == "redis_idle_timeout_ms") {
            cfg.redis_idle_timeout_ms = std::stoi(value);
        } else if (key == "mysql_host") {
            cfg.mysql_host = value;
        } else if (key == "mysql_port") {
            cfg.mysql_port = std::stoi(value);
        } else if (key == "mysql_user") {
            cfg.mysql_user = value;
        } else if (key == "mysql_password") {
            cfg.mysql_password = value;
        } else if (key == "mysql_db") {
            cfg.mysql_db = value;
        } else if (key == "mysql_pool_size") {
            cfg.mysql_pool_size = std::stoi(value);
        } else if (key == "mysql_pool_min_size") {
            cfg.mysql_pool_min_size = std::stoi(value);
        } else if (key == "mysql_pool_max_size") {
            cfg.mysql_pool_max_size = std::stoi(value);
        } else if (key == "mysql_idle_timeout_ms") {
            cfg.mysql_idle_timeout_ms = std::stoi(value);
        } else if (key == "comet_id") {
            cfg.comet_id = value;
        } else if (key == "comet_targets") {
            cfg.comet_targets = value;
        } else if (key == "comet_io_threads") {
            cfg.comet_io_threads = std::stoi(value);
        } else if (key == "comet_grpc_port") {
            cfg.comet_grpc_port = std::stoi(value);
        } else if (key == "comet_grpc_pool_size") {
            cfg.comet_grpc_pool_size = std::stoi(value);
        } else if (key == "use_grpc_stream") {
            cfg.use_grpc_stream = (value == "true" || value == "1");
        } else if (key == "grpc_stream_count") {
            cfg.grpc_stream_count = std::stoi(value);
        } else if (key == "use_push_stream") {
            cfg.use_push_stream = (value == "true" || value == "1");
        } else if (key == "push_stream_queue_max") {
            cfg.push_stream_queue_max = std::stoi(value);
        } else if (key == "push_stream_reconnect_base_ms") {
            cfg.push_stream_reconnect_base_ms = std::stoi(value);
        } else if (key == "push_stream_reconnect_max_ms") {
            cfg.push_stream_reconnect_max_ms = std::stoi(value);
        } else if (key == "push_rpc_deadline_ms") {
            cfg.push_rpc_deadline_ms = std::stoi(value);
        } else if (key == "persist_kafka_timeout_ms") {
            cfg.persist_kafka_timeout_ms = std::stoi(value);
        } else if (key == "single_rate_per_sec") {
            cfg.rate_limit.single_rate_per_sec = std::stod(value);
        } else if (key == "single_burst") {
            cfg.rate_limit.single_burst = std::stod(value);
        } else if (key == "group_rate_per_sec") {
            cfg.rate_limit.group_rate_per_sec = std::stod(value);
        } else if (key == "group_burst") {
            cfg.rate_limit.group_burst = std::stod(value);
        } else if (key == "broadcast_rate_per_sec") {
            cfg.rate_limit.broadcast_rate_per_sec = std::stod(value);
        } else if (key == "broadcast_burst") {
            cfg.rate_limit.broadcast_burst = std::stod(value);
        } else if (key == "admin_account") {
            cfg.admin_account = value;
        } else if (key == "admin_password") {
            cfg.admin_password = value;
        } else if (key == "hermes_enabled") {
            cfg.hermes_enabled = (value == "true" || value == "1");
        } else if (key == "hermes_bot_user_id") {
            cfg.hermes_bot_user_id = std::stoll(value);
        }
    }

    // 环境变量优先级最高，便于容器/CI 注入敏感配置。
    // 老配置只有 kafka_push_topic 时，两个新队列先回退到旧 topic，保持兼容；
    // 根目录模板显式配置独立 topic，因此默认运行会启用隔离队列。
    if (has_legacy_push_topic && !has_single_topic) {
        cfg.kafka_single_topic = cfg.kafka_push_topic;
    }
    if (has_legacy_push_topic && !has_group_topic) {
        cfg.kafka_group_topic = cfg.kafka_push_topic;
    }
    ApplySensitiveEnvOverrides(&cfg);

    return cfg;
}

}  // namespace sparkpush
