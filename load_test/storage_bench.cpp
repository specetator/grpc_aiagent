#include "config.h"
#include "message_dao.h"
#include "mysql_pool.h"
#include "redis_pool.h"
#include "redis_store.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

struct BenchConfig {
    std::string mode;
    std::string config_path{"conf/logic.conf"};
    int operations{10000};
    int threads{8};
};

bool ParseIntValue(const std::string& value, int* out) {
    if (!out) return false;
    try {
        size_t used = 0;
        const int parsed = std::stoi(value, &used);
        if (used != value.size()) return false;
        *out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseArgs(int argc, char** argv, BenchConfig* cfg) {
    if (!cfg) return false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (i + 1 >= argc) return false;
        const std::string value = argv[++i];
        if (arg == "--mode") {
            cfg->mode = value;
        } else if (arg == "--config") {
            cfg->config_path = value;
        } else if (arg == "--operations") {
            if (!ParseIntValue(value, &cfg->operations)) return false;
        } else if (arg == "--threads") {
            if (!ParseIntValue(value, &cfg->threads)) return false;
        } else {
            return false;
        }
    }
    return (cfg->mode == "redis" || cfg->mode == "mysql") &&
           cfg->operations > 0 && cfg->threads > 0;
}

double Percentile(std::vector<double> values, double percentile) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(
        std::min<double>(values.size() - 1,
                         percentile * static_cast<double>(values.size() - 1)));
    return values[index];
}

template <typename Operation>
int RunConcurrent(const BenchConfig& cfg, const std::string& label,
                  Operation operation) {
    std::atomic<int> next{0};
    std::atomic<int> succeeded{0};
    std::atomic<int> failed{0};
    std::mutex latency_mutex;
    std::vector<double> latencies;
    latencies.reserve(static_cast<size_t>(cfg.operations));
    const auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(cfg.threads));
    for (int worker = 0; worker < cfg.threads; ++worker) {
        workers.emplace_back([&]() {
            std::vector<double> local_latencies;
            while (true) {
                const int index = next.fetch_add(1);
                if (index >= cfg.operations) break;
                const auto op_start = std::chrono::steady_clock::now();
                if (operation(index)) {
                    ++succeeded;
                } else {
                    ++failed;
                }
                const auto op_end = std::chrono::steady_clock::now();
                local_latencies.push_back(
                    std::chrono::duration<double, std::milli>(
                        op_end - op_start).count());
            }
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies.insert(latencies.end(), local_latencies.begin(),
                             local_latencies.end());
        });
    }
    for (auto& worker : workers) worker.join();
    const double seconds = std::chrono::duration<double>(
                               std::chrono::steady_clock::now() - start).count();
    std::cout << "STORAGE_RESULT mode=" << label
              << " operations=" << cfg.operations
              << " threads=" << cfg.threads
              << " succeeded=" << succeeded.load()
              << " failed=" << failed.load()
              << " duration_s=" << seconds
              << " qps=" << (seconds > 0 ? succeeded.load() / seconds : 0.0)
              << " latency_p50_ms=" << Percentile(latencies, 0.50)
              << " latency_p95_ms=" << Percentile(latencies, 0.95)
              << " latency_p99_ms=" << Percentile(latencies, 0.99) << '\n';
    return failed.load() == 0 ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
    BenchConfig bench;
    if (!ParseArgs(argc, argv, &bench)) {
        std::cerr << "用法: " << argv[0]
                  << " --mode redis|mysql [--operations 10000]"
                     " [--threads 8] [--config conf/logic.conf]\n";
        return 2;
    }

    const sparkpush::Config cfg = sparkpush::LoadConfig(bench.config_path);
    const std::string run_id =
        std::to_string(std::chrono::system_clock::now()
                           .time_since_epoch().count());

    if (bench.mode == "redis") {
        sparkpush::RedisConnectionPool pool;
        sparkpush::RedisConfig redis_cfg;
        redis_cfg.host = cfg.redis_host;
        redis_cfg.port = cfg.redis_port;
        redis_cfg.password = cfg.redis_password;
        redis_cfg.db = cfg.redis_db;
        redis_cfg.pool_size = cfg.redis_pool_size;
        redis_cfg.min_pool_size = cfg.redis_pool_size;
        redis_cfg.max_pool_size = cfg.redis_max_pool_size;
        redis_cfg.connect_timeout_ms = cfg.redis_connect_timeout_ms;
        redis_cfg.rw_timeout_ms = cfg.redis_rw_timeout_ms;
        redis_cfg.idle_timeout_ms = cfg.redis_idle_timeout_ms;
        redis_cfg.health_check_interval_ms =
            cfg.redis_health_check_interval_ms;
        if (!pool.Init(redis_cfg)) {
            std::cerr << "Redis pool 初始化失败\n";
            return 1;
        }
        sparkpush::RedisStore store(&pool);
        const std::string session_id = "perf_redis_" + run_id;
        return RunConcurrent(bench, "redis_single_hot_path", [&](int index) {
            int64_t seq = 0;
            bool is_new = false;
            return store.AllocateSessionMsgSeq(
                       session_id, 900000000012LL,
                       "redis-bench-" + run_id + "-" + std::to_string(index),
                       0, 60, &seq, &is_new) && is_new && seq > 0;
        });
    }

    sparkpush::MySqlConnectionPool pool;
    sparkpush::MySqlConfig mysql_cfg;
    mysql_cfg.host = cfg.mysql_host;
    mysql_cfg.port = cfg.mysql_port;
    mysql_cfg.user = cfg.mysql_user;
    mysql_cfg.password = cfg.mysql_password;
    mysql_cfg.db = cfg.mysql_db;
    mysql_cfg.pool_size = cfg.mysql_pool_size;
    mysql_cfg.min_pool_size = cfg.mysql_pool_min_size;
    mysql_cfg.max_pool_size = cfg.mysql_pool_max_size;
    mysql_cfg.idle_timeout_ms = cfg.mysql_idle_timeout_ms;
    if (!pool.Init(mysql_cfg)) {
        std::cerr << "MySQL pool 初始化失败\n";
        return 1;
    }
    sparkpush::MessageDao dao(&pool);
    const std::string session_id = "perf_mysql_" + run_id;
    return RunConcurrent(bench, "mysql_sync_insert", [&](int index) {
        sparkpush::Message message;
        message.session_id = session_id;
        message.msg_seq = index + 1;
        message.msg_id = session_id + "-" + std::to_string(index + 1);
        message.sender_id = 900000000012LL;
        message.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                   std::chrono::system_clock::now()
                                       .time_since_epoch()).count();
        message.msg_type = "text";
        message.content_json = "{\"text\":\"storage benchmark\"}";
        message.client_msg_id =
            "mysql-bench-" + run_id + "-" + std::to_string(index);
        std::string error;
        return dao.InsertMessage(message, &error);
    });
}
