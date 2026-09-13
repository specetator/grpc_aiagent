#include "config.h"
#include "kafka_producer.h"
#include "spark_push.pb.h"
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
    Config receiver;
    std::string config_path{"conf/job.conf"};
    int messages{1000};
    int timeout_ms{60000};
};

bool NextValue(int argc, char** argv, int* index, std::string* value) {
    if (!index || !value || *index + 1 >= argc) return false;
    *value = argv[++(*index)];
    return true;
}

bool ParseArgs(int argc, char** argv, BenchConfig* cfg) {
    if (!cfg) return false;
    cfg->receiver.comet_port = 9000;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        std::string value;
        if (arg == "--account") {
            if (!NextValue(argc, argv, &i, &cfg->receiver.account)) return false;
        } else if (arg == "--password") {
            if (!NextValue(argc, argv, &i, &cfg->receiver.password)) return false;
        } else if (arg == "--config") {
            if (!NextValue(argc, argv, &i, &cfg->config_path)) return false;
        } else if (arg == "--logic-host") {
            if (!NextValue(argc, argv, &i, &cfg->receiver.logic_host)) return false;
        } else if (arg == "--logic-port") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->receiver.logic_port)) return false;
        } else if (arg == "--comet-host") {
            if (!NextValue(argc, argv, &i, &cfg->receiver.comet_host)) return false;
        } else if (arg == "--comet-port") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->receiver.comet_port)) return false;
        } else if (arg == "--messages") {
            if (!NextValue(argc, argv, &i, &value) ||
                !::ParseInt(value, &cfg->messages)) return false;
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
    return !cfg->receiver.account.empty() &&
           !cfg->receiver.password.empty() && cfg->messages > 0 &&
           cfg->timeout_ms > 0;
}

void Usage(const char* program) {
    std::cerr << "用法: " << program
              << " --account A --password P [--messages 1000]"
                 " [--config conf/job.conf] [--logic-port 9101]"
                 " [--comet-port 9000] [--timeout-ms 60000]\n";
}

double Percentile(std::vector<double> values, double percentile) {
    if (values.empty()) return 0.0;
    std::sort(values.begin(), values.end());
    const size_t index = static_cast<size_t>(
        std::min<double>(values.size() - 1,
                         percentile * static_cast<double>(values.size() - 1)));
    return values[index];
}

int64_t SystemNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

}  // namespace

int main(int argc, char** argv) {
    BenchConfig bench;
    if (!ParseArgs(argc, argv, &bench)) {
        Usage(argv[0]);
        return 2;
    }

    long long receiver_id = 0;
    std::string token;
    if (!HttpPostLogin(bench.receiver, &receiver_id, &token)) return 1;
    const int receiver_fd =
        ConnectTcp(bench.receiver.comet_host, bench.receiver.comet_port);
    if (receiver_fd < 0 ||
        !WebSocketHandshake(receiver_fd, bench.receiver.comet_host,
                            bench.receiver.comet_port, token)) {
        if (receiver_fd >= 0) close(receiver_fd);
        std::cerr << "接收方 WebSocket 连接失败\n";
        return 1;
    }

    const sparkpush::Config cfg = sparkpush::LoadConfig(bench.config_path);
    const std::string topic = cfg.kafka_single_topic.empty()
                                  ? cfg.kafka_push_topic
                                  : cfg.kafka_single_topic;
    sparkpush::KafkaProducer producer;
    if (!producer.Init(cfg.kafka_brokers, topic)) {
        close(receiver_fd);
        return 1;
    }

    const std::string run_id =
        "kafka-bench-" + std::to_string(SystemNowMs());
    std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, std::chrono::steady_clock::time_point>
        send_times;
    std::unordered_set<std::string> received_ids;
    std::vector<double> latencies_ms;
    std::atomic<int> received{0};
    std::atomic<int> parse_errors{0};
    std::chrono::steady_clock::time_point completion_time{};

    std::thread receiver([&]() {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(bench.timeout_ms);
        while (std::chrono::steady_clock::now() < deadline &&
               received.load() < bench.messages) {
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
                std::lock_guard<std::mutex> lock(mutex);
                if (!received_ids.insert(client_id).second) continue;
                const auto it = send_times.find(client_id);
                if (it != send_times.end()) {
                    latencies_ms.push_back(
                        std::chrono::duration<double, std::milli>(now - it->second)
                            .count());
                }
                ++received;
                if (received.load() == bench.messages) completion_time = now;
                cv.notify_one();
            } catch (...) {
                ++parse_errors;
            }
        }
        cv.notify_one();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto start = std::chrono::steady_clock::now();
    int sent = 0;
    int send_failed = 0;
    for (int i = 0; i < bench.messages; ++i) {
        const std::string client_id = run_id + "-" + std::to_string(i);
        const auto send_time = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(mutex);
            send_times.emplace(client_id, send_time);
        }

        nlohmann::json content;
        content["type"] = "single_chat";
        content["client_msg_id"] = client_id;
        content["content"] = {{"text", "kafka delivery benchmark"}};

        sparkpush::PushToCometRequest request;
        request.set_comet_id(cfg.comet_id.empty() ? "comet-1" : cfg.comet_id);
        request.set_request_id(client_id + "@comet-1");
        request.set_scene("single");
        auto* message = request.mutable_message();
        message->set_msg_id(client_id);
        message->set_session_id("perf_kafka_" + run_id);
        message->set_msg_seq(i + 1);
        message->set_sender_id(900000000012LL);
        message->set_timestamp_ms(SystemNowMs());
        message->set_msg_type("text");
        message->set_content_json(content.dump());
        message->set_client_msg_id(client_id);
        request.add_targets()->set_user_id(receiver_id);

        std::string bytes;
        if (!request.SerializeToString(&bytes) ||
            !producer.Send(request.comet_id(), bytes)) {
            ++send_failed;
        } else {
            ++sent;
        }
    }

    {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait_for(lock, std::chrono::milliseconds(bench.timeout_ms), [&]() {
            return received.load() >= sent;
        });
    }
    shutdown(receiver_fd, SHUT_RDWR);
    close(receiver_fd);
    receiver.join();

    const auto end = completion_time == std::chrono::steady_clock::time_point{}
                         ? std::chrono::steady_clock::now()
                         : completion_time;
    const double seconds = std::chrono::duration<double>(end - start).count();
    const double qps = seconds > 0 ? received.load() / seconds : 0.0;
    std::cout << "KAFKA_DELIVERY_RESULT sent=" << sent
              << " send_failed=" << send_failed
              << " received=" << received.load()
              << " missing=" << std::max(0, sent - received.load())
              << " parse_errors=" << parse_errors.load()
              << " duration_s=" << seconds << " delivery_qps=" << qps
              << " latency_p50_ms=" << Percentile(latencies_ms, 0.50)
              << " latency_p95_ms=" << Percentile(latencies_ms, 0.95)
              << " latency_p99_ms=" << Percentile(latencies_ms, 0.99) << '\n';
    return sent == bench.messages && received.load() == sent &&
                   send_failed == 0 && parse_errors.load() == 0
               ? 0
               : 1;
}
