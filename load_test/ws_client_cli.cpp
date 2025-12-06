#include "ws_client_lib.h"

#include <chrono>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>

#include <sys/socket.h>
#include <unistd.h>

namespace {

/// @brief 打印 CLI 使用说明，介绍所有参数及示例。
void PrintUsage(const char* prog) {
    std::cout
        << "用法: " << prog
        << " --account <账号> --password <密码> [选项]\n"
        << "必选参数:\n"
        << "  --account <str>        登录账号\n"
        << "  --password <str>       登录密码\n"
        << "可选参数:\n"
        << "  --logic-host <host>    logic HTTP 主机 (默认 127.0.0.1)\n"
        << "  --logic-port <port>    logic HTTP 端口 (默认 9101)\n"
        << "  --comet-host <host>    comet WebSocket 主机 (默认 127.0.0.1)\n"
        << "  --comet-port <port>    comet WebSocket 端口 (默认 9100)\n"
        << "  --to-user <uid>        单聊目标用户 id（默认 0，仅单聊模式使用）\n"
        << "  --room-id <id>         聊天室 room_id，配置后自动进入聊天室模式\n"
        << "\n示例:\n"
        << "  " << prog
        << " --account user1 --password 123456 --to-user 2\n"
        << "  " << prog
        << " --account user1 --password 123456 --room-id 1001\n";
}

/// @brief 解析 CLI 参数，支持 -h/--help 并校验必填项。
bool ParseCliArgs(int argc, char* argv[], Config* cfg) {
    if (!cfg) return false;
    // 先用通用解析填充 cfg 基础字段
    if (!ParseArgs(argc, argv, cfg)) {
        return false;
    }
    // 再处理 CLI 专用开关（目前只有 help）
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
    }
    if (cfg->account.empty() || cfg->password.empty()) {
        std::cerr << "必须指定 --account 和 --password\n";
        return false;
    }
    return true;
}
}  // namespace

/// @brief 主函数：登录后建立 WebSocket 连接并转发标准输入到服务端。
int main(int argc, char* argv[]) {
    Config cfg;
    if (!ParseCliArgs(argc, argv, &cfg)) {
        PrintUsage(argv[0]);
        return 1;
    }

    long long user_id = 0;
    std::string token;
    if (!HttpPostLogin(cfg, &user_id, &token)) {
        std::cerr << "登录失败，退出\n";
        return 1;
    }
    std::cout << "登录成功，user_id=" << user_id << "\n";

    int ws_fd = ConnectTcp(cfg.comet_host, cfg.comet_port);
    if (ws_fd < 0) return 1;
    if (!WebSocketHandshake(ws_fd, cfg.comet_host, cfg.comet_port, token)) {
        ::close(ws_fd);
        return 1;
    }

    // 启动接收线程
    std::thread recv_thread(ReceiveLoop, ws_fd);

    std::cout << "输入要发送的文本（每行一条，Ctrl+D 结束）...\n";
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;
        nlohmann::json j;
        if (cfg.is_chatroom) {
            j["type"] = "chatroom";
            j["group_id"] = cfg.room_id;
            j["content"] = {{"text", line}};
        } else {
            j["type"] = "single_chat";
            j["to_user_id"] = cfg.to_user_id;
            j["content"] = {{"text", line}};
        }
        // 使用时间戳拼接 client_msg_id，方便日志定位
        j["client_msg_id"] =
            std::to_string(user_id) + "-" +
            std::to_string(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count());
        std::string payload = j.dump();
        std::string frame = BuildClientWebSocketTextFrame(payload);
        ssize_t sent = send(ws_fd, frame.data(), frame.size(), 0);
        if (sent != static_cast<ssize_t>(frame.size())) {
            std::cerr << "发送 WebSocket 消息失败\n";
            break;
        }
    }

    shutdown(ws_fd, SHUT_WR);
    if (recv_thread.joinable()) {
        recv_thread.join();
    }
    close(ws_fd);
    return 0;
}


