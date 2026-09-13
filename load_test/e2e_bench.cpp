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
    Config sender;
    Config receiver;
    int connections{8};
    int receiver_connections{1};
    int messages_per_connection{100};
    int timeout_ms{15000};
};

bool NextValue(int argc, char** argv, int* index, std::string* value) {
    if (!index || !value || *index + 1 >= argc) return false;
    *value = argv[++(*index)];
    return true;
}

bool ParseArgs(int argc, char** argv, BenchConfig* cfg) {
    if (!cfg) return false;
    cfg->sender.comet_port = 9000;
    cfg->receiver.comet_port = 9000;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (arg == "--sender-account") {
            if (!NextValue(argc, argv, &i, &cfg->sender.account)) return false;
        } else if (arg == "--sender-password") {
            if (!NextValue(argc, argv, &i, &cfg->sender.password)) return false;
        } else if (arg == "--receiver-account") {
            if (!NextValue(argc, argv, &i, &cfg->receiver.account)) return false;
        } else if (arg == "--receiver-password") {
            if (!NextValue(argc, argv, &i, &cfg->receiver.password)) return false;
        } else if (arg == "--logic-host") {
            if (!NextValue(argc, argv, &i, &value)) return false;
            cfg->sender.logic_host = cfg->receiver.logic_host = value;
        } else if (arg == "--logic-port") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->sender.logic_port)) return false;
            cfg->receiver.logic_port = cfg->sender.logic_port;
        } else if (arg == "--comet-host") {
            if (!NextValue(argc, argv, &i, &value)) return false;
            cfg->sender.comet_host = cfg->receiver.comet_host = value;
        } else if (arg == "--comet-port") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->sender.comet_port)) return false;
            cfg->receiver.comet_port = cfg->sender.comet_port;
        } else if (arg == "--connections") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->connections)) return false;
        } else if (arg == "--messages-per-conn") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->messages_per_connection)) return false;
        } else if (arg == "--receiver-connections") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->receiver_connections)) return false;
        } else if (arg == "--timeout-ms") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->timeout_ms)) return false;
        } else if (arg == "--help" || arg == "-h") {
            return false;
        }
    }
    return !cfg->sender.account.empty() && !cfg->sender.password.empty() &&
           !cfg->receiver.account.empty() && !cfg->receiver.password.empty() &&
           cfg->connections > 0 && cfg->receiver_connections > 0 &&
           cfg->messages_per_connection > 0;
}

void Usage(const char* program) {
    std::cerr
        << "用法: " << program
        << " --sender-account A --sender-password P"
           " --receiver-account B --receiver-password P"
           " [--connections 8] [--messages-per-conn 100]"
           " [--receiver-connections 1]"
           " [--logic-port 9101] [--comet-port 9000] [--timeout-ms 15000]\n";
}

bool SendAll(int fd, const std::string& bytes) {
    size_t offset = 0;
    while (offset < bytes.size()) {
        const ssize_t sent =
            send(fd, bytes.data() + offset, bytes.size() - offset, 0);
        if (sent <= 0) return false;
        offset += static_cast<size_t>(sent);
    }
    return true;
}

int ConnectAuthenticated(const Config& cfg, long long* user_id) {
    std::string token;
    if (!HttpPostLogin(cfg, user_id, &token)) return -1;
    int fd = ConnectTcp(cfg.comet_host, cfg.comet_port);
    if (fd < 0) return -1;
    if (!WebSocketHandshake(fd, cfg.comet_host, cfg.comet_port, token)) {
        close(fd);
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

    long long receiver_id = 0;
    std::vector<int> receiver_fds;
    receiver_fds.reserve(cfg.receiver_connections);
    for (int i = 0; i < cfg.receiver_connections; ++i) {
        const int receiver_fd = ConnectAuthenticated(cfg.receiver, &receiver_id);
        if (receiver_fd < 0) {
            std::cerr << "接收方连接失败: " << i << '\n';
            for (int open_fd : receiver_fds) close(open_fd);
            return 1;
        }
        receiver_fds.push_back(receiver_fd);
    }
    cfg.sender.to_user_id = receiver_id;

    std::vector<int> sender_fds;
    sender_fds.reserve(cfg.connections);
    long long sender_id = 0;
    for (int i = 0; i < cfg.connections; ++i) {
        int fd = ConnectAuthenticated(cfg.sender, &sender_id);
        if (fd < 0) {
            std::cerr << "发送方连接建立失败: " << i << '\n';
            for (int open_fd : sender_fds) close(open_fd);
            for (int open_fd : receiver_fds) close(open_fd);
            return 1;
        }
        sender_fds.push_back(fd);
    }

    const int expected = cfg.connections * cfg.messages_per_connection;
    const int expected_deliveries = expected * cfg.receiver_connections;
    const std::string run_id =
        "e2e-" + std::to_string(std::chrono::system_clock::now()
                                     .time_since_epoch()
                                     .count());
    std::atomic<int> sent{0};
    std::atomic<int> send_failed{0};
    std::atomic<int> accepted_acks{0};
    std::atomic<int> delivered_acks{0};
    std::atomic<int> ack_errors{0};
    std::atomic<int> delivered{0};
    std::mutex timing_mutex;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>
        send_times;
    std::unordered_set<std::string> delivered_ids;
    std::unordered_set<std::string> accepted_ids;
    std::unordered_set<std::string> delivered_ack_ids;
    std::vector<double> latencies_ms;
    std::mutex completion_mutex;
    std::condition_variable completion_cv;
    int receivers_done = 0;
    auto completion_time = std::chrono::steady_clock::time_point{};

    std::vector<std::thread> receivers;
    receivers.reserve(receiver_fds.size());
    for (size_t receiver_index = 0; receiver_index < receiver_fds.size();
         ++receiver_index) {
        receivers.emplace_back([&, receiver_index]() {
            const int receiver_fd = receiver_fds[receiver_index];
            int local_delivered = 0;
            const auto deadline = std::chrono::steady_clock::now() +
                                  std::chrono::milliseconds(cfg.timeout_ms);
            while (std::chrono::steady_clock::now() < deadline &&
                   local_delivered < expected) {
                std::string payload;
                if (!ReadServerWebSocketTextFrame(receiver_fd, &payload, 200)) {
                    continue;
                }
                try {
                    const auto json = nlohmann::json::parse(payload);
                    const std::string client_id =
                        json.value("client_msg_id", std::string{});
                    if (client_id.rfind(run_id, 0) != 0) continue;
                    const auto now = std::chrono::steady_clock::now();
                    std::lock_guard<std::mutex> lock(timing_mutex);
                    const std::string delivery_id =
                        client_id + "#" + std::to_string(receiver_index);
                    if (!delivered_ids.insert(delivery_id).second) continue;
                    auto it = send_times.find(client_id);
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

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto start = std::chrono::steady_clock::now();
    std::vector<std::thread> senders;
    for (int connection = 0; connection < cfg.connections; ++connection) {
        senders.emplace_back([&, connection]() {
            const int fd = sender_fds[connection];
            int sent_on_connection = 0;
            for (int message = 0; message < cfg.messages_per_connection;
                 ++message) {
                const std::string client_id =
                    run_id + "-" + std::to_string(connection) + "-" +
                    std::to_string(message);
                nlohmann::json json;
                json["type"] = "single_chat";
                json["to_user_id"] = receiver_id;
                json["client_msg_id"] = client_id;
                json["content"] = {{"text", "e2e benchmark"}};
                const std::string frame =
                    BuildClientWebSocketTextFrame(json.dump());
                {
                    std::lock_guard<std::mutex> lock(timing_mutex);
                    send_times.emplace(client_id,
                                       std::chrono::steady_clock::now());
                }
                if (!SendAll(fd, frame)) {
                    ++send_failed;
                    break;
                }
                ++sent;
                ++sent_on_connection;
            }

            const auto ack_deadline = std::chrono::steady_clock::now() +
                                      std::chrono::milliseconds(cfg.timeout_ms);
            int received_accepted = 0;
            int received_delivered_ack = 0;
            while ((received_accepted < sent_on_connection ||
                    received_delivered_ack < sent_on_connection) &&
                   std::chrono::steady_clock::now() < ack_deadline) {
                std::string payload;
                if (!ReadServerWebSocketTextFrame(fd, &payload, 200)) continue;
                try {
                    const auto json = nlohmann::json::parse(payload);
                    const std::string type = json.value("type", std::string{});
                    const std::string client_id =
                        json.value("client_msg_id", std::string{});
                    if (type == "accepted_ack" &&
                        client_id.rfind(run_id, 0) == 0) {
                        std::lock_guard<std::mutex> lock(timing_mutex);
                        if (accepted_ids.insert(client_id).second) {
                            ++accepted_acks;
                            ++received_accepted;
                        }
                    } else if (type == "delivered_ack" &&
                               client_id.rfind(run_id, 0) == 0) {
                        std::lock_guard<std::mutex> lock(timing_mutex);
                        if (delivered_ack_ids.insert(client_id).second) {
                            ++delivered_acks;
                            ++received_delivered_ack;
                        }
                    } else if (json.value("type", std::string{}) == "error") {
                        ++ack_errors;
                        ++received_accepted;
                        ++received_delivered_ack;
                    }
                } catch (...) {
                    ++ack_errors;
                }
            }
        });
    }
    for (auto& sender : senders) sender.join();

    {
        std::unique_lock<std::mutex> lock(completion_mutex);
        completion_cv.wait_for(lock, std::chrono::milliseconds(cfg.timeout_ms),
                               [&]() {
                                   return receivers_done ==
                                          cfg.receiver_connections;
                               });
    }
    for (auto& receiver : receivers) {
        if (receiver.joinable()) receiver.join();
    }
    const auto end = completion_time == std::chrono::steady_clock::time_point{}
                         ? std::chrono::steady_clock::now()
                         : completion_time;

    for (int fd : sender_fds) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }
    for (int receiver_fd : receiver_fds) {
        shutdown(receiver_fd, SHUT_RDWR);
        close(receiver_fd);
    }

    const double seconds = std::chrono::duration<double>(end - start).count();
    const double delivery_qps = seconds > 0 ? delivered.load() / seconds : 0.0;
    std::cout << "E2E_RESULT sent=" << sent.load()
              << " accepted_ack=" << accepted_acks.load()
              << " delivered_ack=" << delivered_acks.load()
              << " delivered=" << delivered.load()
              << " send_failed=" << send_failed.load()
              << " ack_errors=" << ack_errors.load()
              << " duration_s=" << seconds
              << " delivery_qps=" << delivery_qps
              << " latency_p50_ms=" << Percentile(latencies_ms, 0.50)
              << " latency_p95_ms=" << Percentile(latencies_ms, 0.95)
              << " latency_p99_ms=" << Percentile(latencies_ms, 0.99) << '\n';

    return sent.load() == expected && accepted_acks.load() == expected &&
                   delivered_acks.load() == expected &&
                   delivered.load() == expected_deliveries &&
                   send_failed.load() == 0 && ack_errors.load() == 0
               ? 0
               : 1;
}
