#include "ws_client_lib.h"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cctype>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

struct BenchConfig {
    Config client;
    std::string session_id;
    int threads{8};
    int requests_per_thread{500};
    int limit{50};
    int timeout_ms{5000};
};

bool NextValue(int argc, char** argv, int* index, std::string* value) {
    if (!index || !value || *index + 1 >= argc) return false;
    *value = argv[++(*index)];
    return true;
}

bool ParseArgs(int argc, char** argv, BenchConfig* cfg) {
    if (!cfg) return false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (arg == "--account") {
            if (!NextValue(argc, argv, &i, &cfg->client.account)) return false;
        } else if (arg == "--password") {
            if (!NextValue(argc, argv, &i, &cfg->client.password)) return false;
        } else if (arg == "--session-id") {
            if (!NextValue(argc, argv, &i, &cfg->session_id)) return false;
        } else if (arg == "--logic-host") {
            if (!NextValue(argc, argv, &i, &cfg->client.logic_host)) return false;
        } else if (arg == "--logic-port") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->client.logic_port)) return false;
        } else if (arg == "--threads") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->threads)) return false;
        } else if (arg == "--requests-per-thread") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->requests_per_thread)) return false;
        } else if (arg == "--limit") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->limit)) return false;
        } else if (arg == "--timeout-ms") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->timeout_ms)) return false;
        } else if (arg == "--help" || arg == "-h") {
            return false;
        } else {
            std::cerr << "未知参数: " << arg << '\n';
            return false;
        }
    }
    return !cfg->client.account.empty() && !cfg->client.password.empty() &&
           !cfg->session_id.empty() && cfg->threads > 0 &&
           cfg->requests_per_thread > 0 && cfg->limit > 0;
}

void Usage(const char* program) {
    std::cerr << "用法: " << program
              << " --account A --password P --session-id S"
                 " [--threads 8] [--requests-per-thread 500]"
                 " [--limit 50] [--logic-port 9101]\n";
}

bool SendAll(int fd, const std::string& bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t n = send(fd, bytes.data() + offset,
                               bytes.size() - offset, 0);
        if (n <= 0) return false;
        offset += static_cast<size_t>(n);
    }
    return true;
}

bool HttpHistory(const BenchConfig& cfg, const std::string& token) {
    const int fd = ConnectTcp(cfg.client.logic_host, cfg.client.logic_port);
    if (fd < 0) return false;

    nlohmann::json body_json;
    body_json["session_id"] = cfg.session_id;
    body_json["anchor_seq"] = 0;
    body_json["limit"] = cfg.limit;
    const std::string body = body_json.dump();
    std::ostringstream request;
    request << "POST /api/session/history HTTP/1.1\r\n"
            << "Host: " << cfg.client.logic_host << ':' << cfg.client.logic_port
            << "\r\nContent-Type: application/json\r\n"
            << "Authorization: Bearer " << token << "\r\n"
            << "Connection: close\r\nContent-Length: " << body.size()
            << "\r\n\r\n" << body;
    if (!SendAll(fd, request.str())) {
        close(fd);
        return false;
    }

    timeval timeout{};
    timeout.tv_sec = std::max(1, cfg.timeout_ms / 1000);
    timeout.tv_usec = (cfg.timeout_ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    std::string response;
    char buffer[4096];
    while (true) {
        const ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
        if (n == 0) break;
        if (n < 0) {
            close(fd);
            return false;
        }
        response.append(buffer, static_cast<size_t>(n));
        if (response.size() > 4 * 1024 * 1024) {
            close(fd);
            return false;
        }
    }
    close(fd);
    const size_t split = response.find("\r\n\r\n");
    if (split == std::string::npos ||
        response.substr(0, split).find(" 200 ") == std::string::npos) {
        return false;
    }
    try {
        const auto json = nlohmann::json::parse(response.substr(split + 4));
        return json.value("code", -1) == 0;
    } catch (...) {
        return false;
    }
}

double Percentile(std::vector<double> values, double percentile) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(
        std::min<double>(values.size() - 1,
                         percentile * static_cast<double>(values.size() - 1)));
    return values[index];
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
    if (!HttpPostLogin(cfg.client, &user_id, &token)) return 1;

    // 先做一次预热，同时验证会话权限和请求格式。
    if (!HttpHistory(cfg, token)) {
        std::cerr << "历史查询预热失败，请检查 session_id 和账号权限\n";
        return 1;
    }

    std::atomic<int> succeeded{0};
    std::atomic<int> failed{0};
    std::mutex latency_mutex;
    std::vector<double> latencies_ms;
    latencies_ms.reserve(static_cast<size_t>(cfg.threads) *
                         cfg.requests_per_thread);
    const auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(cfg.threads);
    for (int thread = 0; thread < cfg.threads; ++thread) {
        workers.emplace_back([&]() {
            std::vector<double> local_latencies;
            local_latencies.reserve(cfg.requests_per_thread);
            for (int i = 0; i < cfg.requests_per_thread; ++i) {
                const auto begin = std::chrono::steady_clock::now();
                const bool ok = HttpHistory(cfg, token);
                const auto end = std::chrono::steady_clock::now();
                if (ok) {
                    ++succeeded;
                    local_latencies.push_back(
                        std::chrono::duration<double, std::milli>(end - begin)
                            .count());
                } else {
                    ++failed;
                }
            }
            std::lock_guard<std::mutex> lock(latency_mutex);
            latencies_ms.insert(latencies_ms.end(), local_latencies.begin(),
                                local_latencies.end());
        });
    }
    for (auto& worker : workers) worker.join();
    const auto end = std::chrono::steady_clock::now();
    const double seconds = std::chrono::duration<double>(end - start).count();
    const double qps = seconds > 0 ? succeeded.load() / seconds : 0.0;
    std::cout << "HISTORY_RESULT success=" << succeeded.load()
              << " failed=" << failed.load() << " duration_s=" << seconds
              << " qps=" << qps
              << " latency_p50_ms=" << Percentile(latencies_ms, 0.50)
              << " latency_p95_ms=" << Percentile(latencies_ms, 0.95)
              << " latency_p99_ms=" << Percentile(latencies_ms, 0.99) << '\n';
    return failed.load() == 0 ? 0 : 1;
}
