#include "ws_client_lib.h"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

struct BenchConfig {
    Config endpoint;
    int connections{1000};
    int threads{16};
    int hold_seconds{10};
    int heartbeat_interval_ms{0};
    int rounds{1};
};

bool NextValue(int argc, char** argv, int* index, std::string* value) {
    if (!index || !value || *index + 1 >= argc) return false;
    *value = argv[++(*index)];
    return true;
}

bool ParseArgs(int argc, char** argv, BenchConfig* cfg) {
    if (!cfg) return false;
    cfg->endpoint.comet_port = 9000;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (arg == "--account") {
            if (!NextValue(argc, argv, &i, &cfg->endpoint.account)) return false;
        } else if (arg == "--password") {
            if (!NextValue(argc, argv, &i, &cfg->endpoint.password)) return false;
        } else if (arg == "--connections") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->connections)) return false;
        } else if (arg == "--threads") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->threads)) return false;
        } else if (arg == "--hold-seconds") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->hold_seconds)) return false;
        } else if (arg == "--heartbeat-interval-ms") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->heartbeat_interval_ms)) return false;
        } else if (arg == "--rounds") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->rounds)) return false;
        } else if (arg == "--logic-host") {
            if (!NextValue(argc, argv, &i, &cfg->endpoint.logic_host)) return false;
        } else if (arg == "--logic-port") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->endpoint.logic_port)) return false;
        } else if (arg == "--comet-host") {
            if (!NextValue(argc, argv, &i, &cfg->endpoint.comet_host)) return false;
        } else if (arg == "--comet-port") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->endpoint.comet_port)) return false;
        } else {
            return false;
        }
    }
    return !cfg->endpoint.account.empty() && !cfg->endpoint.password.empty() &&
           cfg->connections > 0 && cfg->threads > 0 &&
           cfg->hold_seconds >= 0 && cfg->heartbeat_interval_ms >= 0 &&
           cfg->rounds > 0;
}

void Usage(const char* program) {
    std::cerr << "用法: " << program
              << " --account A --password P [--connections 1000]"
                 " [--threads 16] [--hold-seconds 10]"
                 " [--heartbeat-interval-ms 0] [--rounds 1]\n";
}

bool SendAll(int fd, const std::string& bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t n =
            ::send(fd, bytes.data() + offset, bytes.size() - offset,
                   MSG_NOSIGNAL);
        if (n <= 0) return false;
        offset += static_cast<size_t>(n);
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    BenchConfig cfg;
    if (!ParseArgs(argc, argv, &cfg)) {
        Usage(argv[0]);
        return 2;
    }

    long long user_id = 0;
    std::string token;
    if (!HttpPostLogin(cfg.endpoint, &user_id, &token)) {
        std::cerr << "登录失败\n";
        return 1;
    }

    long long total_connected = 0;
    long long total_failed = 0;
    long long total_heartbeats = 0;
    long long total_heartbeat_failed = 0;

    for (int round = 1; round <= cfg.rounds; ++round) {
        std::vector<int> fds(static_cast<size_t>(cfg.connections), -1);
        std::atomic<int> next{0};
        std::atomic<int> connected{0};
        std::atomic<int> failed{0};
        const auto start = std::chrono::steady_clock::now();

        std::vector<std::thread> workers;
        const int thread_count = std::min(cfg.threads, cfg.connections);
        workers.reserve(static_cast<size_t>(thread_count));
        for (int worker = 0; worker < thread_count; ++worker) {
            workers.emplace_back([&]() {
                while (true) {
                    const int index = next.fetch_add(1);
                    if (index >= cfg.connections) break;
                    const int fd =
                        ConnectTcp(cfg.endpoint.comet_host, cfg.endpoint.comet_port);
                    if (fd < 0 ||
                        !WebSocketHandshake(fd, cfg.endpoint.comet_host,
                                            cfg.endpoint.comet_port, token)) {
                        if (fd >= 0) ::close(fd);
                        ++failed;
                        continue;
                    }
                    fds[static_cast<size_t>(index)] = fd;
                    ++connected;
                }
            });
        }
        for (auto& worker : workers) worker.join();

        const auto connected_at = std::chrono::steady_clock::now();
        const double connect_seconds =
            std::chrono::duration<double>(connected_at - start).count();
        std::cout << "CONNECTION_ROUND round=" << round
                  << " requested=" << cfg.connections
                  << " connected=" << connected.load()
                  << " failed=" << failed.load()
                  << " connect_duration_s=" << connect_seconds
                  << " connect_rate="
                  << (connect_seconds > 0 ? connected.load() / connect_seconds
                                          : 0.0)
                  << std::endl;

        const auto hold_deadline = connected_at +
                                   std::chrono::seconds(cfg.hold_seconds);
        if (cfg.heartbeat_interval_ms > 0) {
            const std::string heartbeat = BuildClientWebSocketTextFrame(
                "{\"type\":\"ping\",\"ts\":1}");
            while (std::chrono::steady_clock::now() < hold_deadline) {
                const auto tick = std::chrono::steady_clock::now();
                for (const int fd : fds) {
                    if (fd < 0) continue;
                    if (SendAll(fd, heartbeat)) {
                        ++total_heartbeats;
                    } else {
                        ++total_heartbeat_failed;
                    }
                }
                std::this_thread::sleep_until(
                    tick + std::chrono::milliseconds(
                               cfg.heartbeat_interval_ms));
            }
        } else if (cfg.hold_seconds > 0) {
            std::this_thread::sleep_until(hold_deadline);
        }

        for (const int fd : fds) {
            if (fd < 0) continue;
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
        }
        total_connected += connected.load();
        total_failed += failed.load();
    }

    std::cout << "CONNECTION_RESULT rounds=" << cfg.rounds
              << " requested_total="
              << static_cast<long long>(cfg.connections) * cfg.rounds
              << " connected_total=" << total_connected
              << " failed_total=" << total_failed
              << " heartbeats_sent=" << total_heartbeats
              << " heartbeat_failed=" << total_heartbeat_failed << '\n';
    return total_failed == 0 && total_heartbeat_failed == 0 ? 0 : 1;
}
