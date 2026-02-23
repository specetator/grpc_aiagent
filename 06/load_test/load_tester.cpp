#include "ws_client_lib.h"

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <vector>
#include <sstream>
#include <sys/socket.h>
#include <unistd.h>

namespace {

enum class Mode {
    kWsSend,
    kHttpHistory,
    kBizFlow,
};

struct LoadConfig {
    int connections = 10;        // 总连接数
    int io_threads = 4;          // 工作线程数
    int messages_per_conn = 100; // 每连接执行次数（WS: 每连接消息数；HTTP/Biz: 每连接循环次数）
    int send_interval_ms = 10;   // 每条消息/每次请求之间的间隔

    Mode mode = Mode::kWsSend;

    // HTTP 历史查询参数（mode = kHttpHistory 时使用）
    std::string history_session_id;
    long long history_anchor_seq = 0;
    int history_limit = 20;
};

/// @brief 解析压测相关参数，填充 LoadConfig。
bool ParseLoadArgs(int argc, char* argv[], LoadConfig* lc) {
    if (!lc) return false;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next_val = [&](std::string* v) -> bool {
            if (i + 1 >= argc) return false;
            *v = argv[++i];
            return true;
        };
        if (arg == "--mode") {
            std::string v;
            if (!next_val(&v)) return false;
            if (v == "ws_send" || v == "ws") {
                lc->mode = Mode::kWsSend;
            } else if (v == "http_history" || v == "http") {
                lc->mode = Mode::kHttpHistory;
            } else if (v == "biz_flow" || v == "biz") {
                lc->mode = Mode::kBizFlow;
            } else {
                std::cerr << "未知 mode: " << v
                          << "，支持: ws_send/ws, http_history/http, biz_flow/biz\n";
                return false;
            }
        } else if (arg == "--connections") {
            std::string v;
            if (!next_val(&v) || !ParseInt(v, &lc->connections)) return false;
        } else if (arg == "--io-threads") {
            std::string v;
            if (!next_val(&v) || !ParseInt(v, &lc->io_threads)) return false;
        } else if (arg == "--messages-per-conn") {
            std::string v;
            if (!next_val(&v) || !ParseInt(v, &lc->messages_per_conn)) {
                return false;
            }
        } else if (arg == "--interval-ms") {
            std::string v;
            if (!next_val(&v) || !ParseInt(v, &lc->send_interval_ms)) {
                return false;
            }
        } else if (arg == "--history-session-id") {
            std::string v;
            if (!next_val(&v)) return false;
            lc->history_session_id = v;
        } else if (arg == "--history-anchor") {
            std::string v;
            if (!next_val(&v) || !ParseLongLong(v, &lc->history_anchor_seq)) {
                return false;
            }
        } else if (arg == "--history-limit") {
            std::string v;
            if (!next_val(&v) || !ParseInt(v, &lc->history_limit)) return false;
        }
    }
    return true;
}

/// @brief 打印压测工具使用方法，涵盖模式与常用参数。
void PrintLoadUsage(const char* prog) {
    std::cout
        << "用法: " << prog
        << " [功能客户端参数...] [压测参数...]\n"
        << "常用压测参数:\n"
        << "  --mode ws_send       WebSocket 发消息压测 (默认)\n"
        << "  --mode http_history  HTTP /api/session/history 压测\n"
        << "  --mode biz_flow      注册/登录/创建聊天室 业务流程压测\n"
        << "  --connections N          总连接数 (默认 10)\n"
        << "  --io-threads M          工作线程数 (默认 4)\n"
        << "  --messages-per-conn K   每连接执行次数 (默认 100)\n"
        << "  --interval-ms T         每条消息间隔 ms (默认 10)\n"
        << "\nHTTP 历史压测附加参数 (mode=http_history/http):\n"
        << "  --history-session-id SID  会话 ID，例如 s_1_2 或 r_1001\n"
        << "  --history-anchor N        anchor_seq，默认 0\n"
        << "  --history-limit N         每次拉取条数，默认 20\n"
        << "\n说明:\n"
        << "  ws_send:  多线程 + 多连接，通过 WebSocket 连 comet 循环发消息。\n"
        << "  http_history:  多线程循环 POST /api/session/history，压测历史拉取能力。\n"
        << "  biz_flow:  每个连接作为一个虚拟用户，按“注册→登录→建聊天室→加入”流程反复执行。\n";
}

struct Counter {
    std::atomic<long long> sent{0};
    std::atomic<long long> failed{0};
};

/// @brief 发送简单 HTTP POST JSON 请求，读取并解析 JSON 响应体。
bool HttpPostJsonRaw(const Config& cfg,
                     const std::string& path,
                     const nlohmann::json& body_json,
                     nlohmann::json* out_json) {
    int fd = ConnectTcp(cfg.logic_host, cfg.logic_port);
    if (fd < 0) return false;

    std::string body = body_json.dump();

    std::ostringstream oss;
    oss << "POST " << path << " HTTP/1.1\r\n";
    oss << "Host: " << cfg.logic_host << ":" << cfg.logic_port << "\r\n";
    oss << "Content-Type: application/json\r\n";
    oss << "Connection: close\r\n";
    oss << "Content-Length: " << body.size() << "\r\n";
    oss << "\r\n";
    oss << body;

    std::string req = oss.str();
    ssize_t sent = send(fd, req.data(), req.size(), 0);
    if (sent != static_cast<ssize_t>(req.size())) {
        std::cerr << "发送 HTTP 请求失败, path=" << path << "\n";
        close(fd);
        return false;
    }

    std::string header;
    char buf[1024];
    while (header.find("\r\n\r\n") == std::string::npos) {
        ssize_t r = recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) {
            std::cerr << "读取 HTTP 响应头失败, path=" << path << "\n";
            close(fd);
            return false;
        }
        header.append(buf, buf + r);
        if (header.size() > 64 * 1024) {
            std::cerr << "HTTP 头过大, path=" << path << "\n";
            close(fd);
            return false;
        }
    }
    auto pos = header.find("\r\n\r\n");
    std::string header_part = header.substr(0, pos + 4);
    std::string body_part = header.substr(pos + 4);

    // 解析 Content-Length
    size_t content_length = 0;
    {
        std::string key = "Content-Length:";
        auto p = header_part.find(key);
        if (p != std::string::npos) {
            p += key.size();
            while (p < header_part.size() &&
                   (header_part[p] == ' ' || header_part[p] == '\t')) {
                ++p;
            }
            size_t end = p;
            while (end < header_part.size() &&
                   std::isdigit(static_cast<unsigned char>(header_part[end]))) {
                ++end;
            }
            if (end > p) {
                content_length = std::stoul(header_part.substr(p, end - p));
            }
        }
    }

    std::string full_body = body_part;
    while (full_body.size() < content_length) {
        ssize_t r = recv(fd, buf, sizeof(buf), 0);
        if (r <= 0) break;
        full_body.append(buf, buf + r);
    }
    close(fd);

    try {
        auto j = nlohmann::json::parse(full_body);
        if (out_json) {
            *out_json = std::move(j);
        }
        return true;
    } catch (const std::exception& e) {
        std::cerr << "解析 HTTP JSON 失败, path=" << path
                  << ", error=" << e.what() << "\n";
        return false;
    }
}

/// @brief WebSocket 消息发送压测，多线程建立连接并循环发包。
void RunWsSend(const Config& base_cfg,
               const LoadConfig& load_cfg,
               Counter* counter) {
    if (!counter) return;

    auto worker_fn = [&](int idx_begin, int idx_end) {
        for (int i = idx_begin; i < idx_end; ++i) {
            long long user_id = 0;
            std::string token;
            if (!HttpPostLogin(base_cfg, &user_id, &token)) {
                std::cerr << "[ws_send] 登录失败，跳过一个连接\n";
                counter->failed++;
                continue;
            }
            int fd = ConnectTcp(base_cfg.comet_host, base_cfg.comet_port);
            if (fd < 0) {
                counter->failed++;
                continue;
            }
            if (!WebSocketHandshake(fd,
                                    base_cfg.comet_host,
                                    base_cfg.comet_port,
                                    token)) {
                close(fd);
                counter->failed++;
                continue;
            }

            for (int m = 0; m < load_cfg.messages_per_conn; ++m) {
                nlohmann::json j;
                if (base_cfg.is_chatroom) {
                    j["type"] = "chatroom";
                    j["group_id"] = base_cfg.room_id;
                    j["content"] = {{"text", "hello from load_tester"}};
                } else {
                    j["type"] = "single_chat";
                    j["to_user_id"] = base_cfg.to_user_id;
                    j["content"] = {{"text", "hello from load_tester"}};
                }
                j["client_msg_id"] = std::to_string(user_id) + "-" +
                                     std::to_string(i) + "-" +
                                     std::to_string(m);
                std::string payload = j.dump();
                std::string frame = BuildClientWebSocketTextFrame(payload);
                ssize_t sent = send(fd, frame.data(), frame.size(), 0);
                if (sent != static_cast<ssize_t>(frame.size())) {
                    counter->failed++;
                    break;
                } else {
                    counter->sent++;
                }
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(load_cfg.send_interval_ms));
            }

            shutdown(fd, SHUT_WR);
            close(fd);
        }
    };

    int threads = std::max(1, load_cfg.io_threads);
    int per_thread = (load_cfg.connections + threads - 1) / threads;

    std::vector<std::thread> workers;
    int cur = 0;
    for (int t = 0; t < threads && cur < load_cfg.connections; ++t) {
        int begin = cur;
        int end = std::min(load_cfg.connections, begin + per_thread);
        cur = end;
        workers.emplace_back(worker_fn, begin, end);
    }

    for (auto& th : workers) {
        if (th.joinable()) th.join();
    }
}

/// @brief HTTP /api/session/history 压测，多线程循环调用。
void RunHttpHistory(const Config& base_cfg,
                    const LoadConfig& load_cfg,
                    Counter* counter) {
    if (!counter) return;
    if (load_cfg.history_session_id.empty()) {
        std::cerr << "mode=http_history 需要指定 --history-session-id\n";
        return;
    }

    auto worker_fn = [&](int idx_begin, int idx_end) {
        (void)idx_begin;
        (void)idx_end;
        // 当前实现中，不区分具体连接索引，每个线程只关心循环次数
        for (int i = 0; i < load_cfg.messages_per_conn; ++i) {
            nlohmann::json body;
            body["session_id"] = load_cfg.history_session_id;
            body["anchor_seq"] = load_cfg.history_anchor_seq;
            body["limit"] = load_cfg.history_limit;

            nlohmann::json resp;
            if (!HttpPostJsonRaw(base_cfg, "/api/session/history", body, &resp)) {
                counter->failed++;
            } else {
                int code = resp.value("code", -1);
                if (code == 0) {
                    counter->sent++;
                } else {
                    counter->failed++;
                }
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(load_cfg.send_interval_ms));
        }
    };

    int threads = std::max(1, load_cfg.io_threads);
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back(worker_fn, 0, 0);
    }
    for (auto& th : workers) {
        if (th.joinable()) th.join();
    }
}

/// @brief 业务流程压测：注册 -> 登录 -> 创建聊天室 -> 加入聊天室。
void RunBizFlow(const Config& base_cfg,
                const LoadConfig& load_cfg,
                Counter* counter) {
    if (!counter) return;
    if (base_cfg.account.empty() || base_cfg.password.empty()) {
        std::cerr << "mode=biz_flow 需要在 Config 中提供 account/password 作为前缀\n";
        return;
    }

    auto worker_fn = [&](int idx_begin, int idx_end) {
        for (int i = idx_begin; i < idx_end; ++i) {
            for (int loop = 0; loop < load_cfg.messages_per_conn; ++loop) {
                // 为当前虚拟用户构造账号
                Config cfg = base_cfg;
                cfg.account = base_cfg.account + std::to_string(i);

                // 1) 注册
                nlohmann::json reg_body;
                reg_body["account"] = cfg.account;
                reg_body["password"] = cfg.password;
                reg_body["name"] = cfg.account;

                nlohmann::json reg_resp;
                if (!HttpPostJsonRaw(cfg, "/api/register", reg_body, &reg_resp)) {
                    counter->failed++;
                    continue;
                }
                int reg_code = reg_resp.value("code", -1);
                // 允许账号已存在（409）作为“成功跳过注册”
                if (reg_code != 0 && reg_code != 409) {
                    counter->failed++;
                    continue;
                }
                counter->sent++;  // 注册请求计入成功

                // 2) 登录
                long long user_id = 0;
                std::string token;
                if (!HttpPostLogin(cfg, &user_id, &token)) {
                    counter->failed++;
                    continue;
                }
                counter->sent++;  // 登录

                // 3) 创建聊天室
                nlohmann::json create_body;
                create_body["name"] = std::string("load_room_") + cfg.account;
                create_body["owner_id"] = user_id;

                nlohmann::json create_resp;
                if (!HttpPostJsonRaw(cfg,
                                     "/api/admin/chatroom/create",
                                     create_body,
                                     &create_resp)) {
                    counter->failed++;
                    continue;
                }
                if (create_resp.value("code", -1) != 0) {
                    counter->failed++;
                    continue;
                }
                counter->sent++;  // 创建聊天室

                long long room_id = 0;
                try {
                    if (create_resp.contains("data")) {
                        // data 是一个 JSON 字符串
                        auto data_str = create_resp["data"].get<std::string>();
                        auto data_j = nlohmann::json::parse(data_str);
                        room_id = data_j.value("room_id", 0LL);
                    }
                } catch (const std::exception& e) {
                    std::cerr << "解析创建聊天室 data 失败: " << e.what() << "\n";
                    counter->failed++;
                    continue;
                }
                if (room_id <= 0) {
                    counter->failed++;
                    continue;
                }

                // 4) 加入聊天室
                nlohmann::json join_body;
                join_body["room_id"] = room_id;
                join_body["user_id"] = user_id;

                nlohmann::json join_resp;
                if (!HttpPostJsonRaw(cfg,
                                     "/api/chatroom/join",
                                     join_body,
                                     &join_resp)) {
                    counter->failed++;
                    continue;
                }
                if (join_resp.value("code", -1) != 0) {
                    counter->failed++;
                    continue;
                }
                counter->sent++;  // 加入聊天室

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(load_cfg.send_interval_ms));
            }
        }
    };

    int threads = std::max(1, load_cfg.io_threads);
    int per_thread = (load_cfg.connections + threads - 1) / threads;

    std::vector<std::thread> workers;
    int cur = 0;
    for (int t = 0; t < threads && cur < load_cfg.connections; ++t) {
        int begin = cur;
        int end = std::min(load_cfg.connections, begin + per_thread);
        cur = end;
        workers.emplace_back(worker_fn, begin, end);
    }
    for (auto& th : workers) {
        if (th.joinable()) th.join();
    }
}

}  // namespace

/// @brief 程序入口：先解析通用参数，再根据模式选择压测函数。
int main(int argc, char* argv[]) {
    // 先用功能客户端的解析拿到基础 Config，再单独解析压测参数
    Config base_cfg;
    ParseArgs(argc, argv, &base_cfg);

    LoadConfig load_cfg;
    if (!ParseLoadArgs(argc, argv, &load_cfg)) {
        PrintLoadUsage(argv[0]);
        return 1;
    }

    std::cout << "压测配置: connections=" << load_cfg.connections
              << " io_threads=" << load_cfg.io_threads
              << " messages_per_conn=" << load_cfg.messages_per_conn
              << " interval_ms=" << load_cfg.send_interval_ms << "\n";

    Counter counter;

    auto start_ts = std::chrono::steady_clock::now();

    switch (load_cfg.mode) {
        case Mode::kWsSend:
            RunWsSend(base_cfg, load_cfg, &counter);
            break;
        case Mode::kHttpHistory:
            RunHttpHistory(base_cfg, load_cfg, &counter);
            break;
        case Mode::kBizFlow:
            RunBizFlow(base_cfg, load_cfg, &counter);
            break;
    }

    auto end_ts = std::chrono::steady_clock::now();
    double sec =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_ts - start_ts)
            .count() /
        1000.0;

    long long total_sent = counter.sent.load();
    long long total_failed = counter.failed.load();
    double qps = sec > 0 ? (total_sent / sec) : 0.0;

    std::cout << "压测结束: sent=" << total_sent << " failed=" << total_failed
              << " duration=" << sec << "s"
              << " approx QPS=" << qps << "\n";

    return 0;
}


