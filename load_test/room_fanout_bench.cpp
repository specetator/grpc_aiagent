#include "ws_client_lib.h"

#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

struct BenchConfig {
    Config endpoint;
    int receivers{100};
    int messages{100};
    int timeout_ms{30000};
};

bool NextValue(int argc, char** argv, int* index, std::string* value) {
    if (!index || !value || *index + 1 >= argc) return false;
    *value = argv[++(*index)];
    return true;
}

bool ParseArgs(int argc, char** argv, BenchConfig* cfg) {
    if (!cfg) return false;
    cfg->endpoint.comet_port = 9000;
    cfg->endpoint.is_chatroom = true;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (arg == "--account") {
            if (!NextValue(argc, argv, &i, &cfg->endpoint.account)) return false;
        } else if (arg == "--password") {
            if (!NextValue(argc, argv, &i, &cfg->endpoint.password)) return false;
        } else if (arg == "--room-id") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseLongLong(value, &cfg->endpoint.room_id)) return false;
        } else if (arg == "--receivers") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->receivers)) return false;
        } else if (arg == "--messages") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->messages)) return false;
        } else if (arg == "--timeout-ms") {
            if (!NextValue(argc, argv, &i, &value) ||
                !ParseInt(value, &cfg->timeout_ms)) return false;
        } else {
            return false;
        }
    }
    return !cfg->endpoint.account.empty() && !cfg->endpoint.password.empty() &&
           cfg->endpoint.room_id > 0 && cfg->receivers > 0 &&
           cfg->messages > 0 && cfg->timeout_ms > 0;
}

void Usage(const char* program) {
    std::cerr << "用法: " << program
              << " --account A --password P --room-id ID"
                 " [--receivers 100] [--messages 100]"
                 " [--timeout-ms 30000]\n";
}

bool SendAll(int fd, const std::string& bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t n = ::send(fd, bytes.data() + offset,
                                 bytes.size() - offset, MSG_NOSIGNAL);
        if (n <= 0) return false;
        offset += static_cast<size_t>(n);
    }
    return true;
}

int ConnectAuthenticated(const Config& cfg, const std::string& token) {
    const int fd = ConnectTcp(cfg.comet_host, cfg.comet_port);
    if (fd < 0) return -1;
    if (!WebSocketHandshake(fd, cfg.comet_host, cfg.comet_port, token)) {
        ::close(fd);
        return -1;
    }
    return fd;
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
    if (!HttpPostLogin(cfg.endpoint, &user_id, &token)) {
        std::cerr << "登录失败\n";
        return 1;
    }

    const std::string join_frame = BuildClientWebSocketTextFrame(
        nlohmann::json({{"type", "chatroom_join"},
                        {"group_id", cfg.endpoint.room_id}}).dump());
    std::vector<int> receiver_fds;
    receiver_fds.reserve(static_cast<size_t>(cfg.receivers));
    for (int i = 0; i < cfg.receivers; ++i) {
        const int fd = ConnectAuthenticated(cfg.endpoint, token);
        if (fd < 0 || !SendAll(fd, join_frame)) {
            if (fd >= 0) ::close(fd);
            std::cerr << "接收连接建立或加入房间失败: " << i << '\n';
            for (int open_fd : receiver_fds) ::close(open_fd);
            return 1;
        }
        receiver_fds.push_back(fd);
    }
    const int sender_fd = ConnectAuthenticated(cfg.endpoint, token);
    if (sender_fd < 0 || !SendAll(sender_fd, join_frame)) {
        std::cerr << "发送连接建立或加入房间失败\n";
        for (int open_fd : receiver_fds) ::close(open_fd);
        if (sender_fd >= 0) ::close(sender_fd);
        return 1;
    }

    const std::string run_id =
        "room-fanout-" +
        std::to_string(std::chrono::system_clock::now()
                           .time_since_epoch().count());
    std::mutex data_mutex;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>
        send_times;
    std::unordered_set<std::string> delivery_ids;
    std::vector<double> latencies_ms;
    std::atomic<int> delivered{0};
    std::atomic<int> receivers_done{0};
    std::mutex completion_mutex;
    std::condition_variable completion_cv;
    auto completion_time = std::chrono::steady_clock::time_point{};

    std::vector<std::thread> readers;
    readers.reserve(receiver_fds.size());
    for (size_t receiver_index = 0; receiver_index < receiver_fds.size();
         ++receiver_index) {
        readers.emplace_back([&, receiver_index]() {
            int local_delivered = 0;
            const auto deadline = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(cfg.timeout_ms);
            while (std::chrono::steady_clock::now() < deadline &&
                   local_delivered < cfg.messages) {
                std::string payload;
                if (!ReadServerWebSocketTextFrame(
                        receiver_fds[receiver_index], &payload, 200)) continue;
                try {
                    const auto json = nlohmann::json::parse(payload);
                    const std::string client_id =
                        json.value("client_msg_id", std::string{});
                    if (client_id.rfind(run_id, 0) != 0) continue;
                    const auto now = std::chrono::steady_clock::now();
                    std::lock_guard<std::mutex> lock(data_mutex);
                    const std::string delivery_id =
                        client_id + "#" + std::to_string(receiver_index);
                    if (!delivery_ids.insert(delivery_id).second) continue;
                    const auto it = send_times.find(client_id);
                    if (it != send_times.end()) {
                        latencies_ms.push_back(
                            std::chrono::duration<double, std::milli>(
                                now - it->second).count());
                    }
                    ++local_delivered;
                    ++delivered;
                } catch (...) {
                }
            }
            {
                std::lock_guard<std::mutex> lock(completion_mutex);
                completion_time = std::max(completion_time,
                                           std::chrono::steady_clock::now());
                ++receivers_done;
            }
            completion_cv.notify_one();
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const auto start = std::chrono::steady_clock::now();
    int sent = 0;
    for (int i = 0; i < cfg.messages; ++i) {
        const std::string client_id = run_id + "-" + std::to_string(i);
        nlohmann::json json;
        json["type"] = "chatroom";
        json["group_id"] = cfg.endpoint.room_id;
        json["client_msg_id"] = client_id;
        json["content"] = {{"text", "room fanout benchmark"}};
        {
            std::lock_guard<std::mutex> lock(data_mutex);
            send_times.emplace(client_id, std::chrono::steady_clock::now());
        }
        if (!SendAll(sender_fd,
                     BuildClientWebSocketTextFrame(json.dump()))) break;
        ++sent;
    }

    {
        std::unique_lock<std::mutex> lock(completion_mutex);
        completion_cv.wait_for(lock, std::chrono::milliseconds(cfg.timeout_ms),
                               [&]() {
                                   return receivers_done.load() == cfg.receivers;
                               });
    }
    for (auto& reader : readers) reader.join();
    const auto end = completion_time == std::chrono::steady_clock::time_point{}
                         ? std::chrono::steady_clock::now()
                         : completion_time;

    for (const int fd : receiver_fds) {
        ::shutdown(fd, SHUT_RDWR);
        ::close(fd);
    }
    ::shutdown(sender_fd, SHUT_RDWR);
    ::close(sender_fd);

    const int expected = cfg.messages * cfg.receivers;
    const double seconds = std::chrono::duration<double>(end - start).count();
    std::cout << "ROOM_FANOUT_RESULT receivers=" << cfg.receivers
              << " messages_sent=" << sent
              << " expected_deliveries=" << expected
              << " delivered=" << delivered.load()
              << " duration_s=" << seconds
              << " delivery_qps="
              << (seconds > 0 ? delivered.load() / seconds : 0.0)
              << " latency_p50_ms=" << Percentile(latencies_ms, 0.50)
              << " latency_p95_ms=" << Percentile(latencies_ms, 0.95)
              << " latency_p99_ms=" << Percentile(latencies_ms, 0.99) << '\n';
    return sent == cfg.messages && delivered.load() == expected ? 0 : 1;
}
