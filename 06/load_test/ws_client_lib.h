#pragma once

#include <string>

// 依赖: POSIX socket / nlohmann::json / <thread> 等在实现文件中引入

// 基础配置: logic/http + comet/websocket + 账号/路由信息
struct Config {
    std::string logic_host = "127.0.0.1";
    int logic_port = 9101;
    std::string comet_host = "127.0.0.1";
    int comet_port = 9200;

    std::string account;
    std::string password;

    bool is_chatroom = false;
    long long to_user_id = 0;
    long long room_id = 0;
};

/// @brief 将字符串解析为 int，失败返回 false。
/// @param s 待转换的字符串
/// @param out 成功时写入转换结果
bool ParseInt(const std::string& s, int* out);

/// @brief 将字符串解析为 long long，失败返回 false。
/// @param s 待转换的字符串
/// @param out 成功时写入转换结果
bool ParseLongLong(const std::string& s, long long* out);

/// @brief 解析命令行参数到 Config，仅处理与网络/账号相关的开关。
/// @param argc 传入主函数的参数个数
/// @param argv 传入主函数的参数数组
/// @param cfg 用于输出的配置结构体
bool ParseArgs(int argc, char* argv[], Config* cfg);

/// @brief 通过 HTTP POST /api/login 获取 user_id 与 token。
/// @param cfg 逻辑服务地址及账号信息
/// @param user_id 输出登录成功的用户 ID
/// @param token 输出登录成功的 token
/// @return 成功返回 true，失败返回 false
bool HttpPostLogin(const Config& cfg, long long* user_id, std::string* token);

/// @brief 建立到目标 host:port 的 TCP 连接。
/// @return 成功返回已连接的 fd，失败返回 -1
int ConnectTcp(const std::string& host, int port);

/// @brief 构造带掩码的客户端 WebSocket 文本帧。
/// @param payload 需要发送的 JSON 文本
std::string BuildClientWebSocketTextFrame(const std::string& payload);

/// @brief 与服务器完成 WebSocket 握手，使用 token 作为查询参数。
/// @param fd 已连接的 TCP fd
/// @param host comet 主机
/// @param port comet 端口
/// @param token 登录 token
/// @return 握手成功返回 true
bool WebSocketHandshake(int fd,
                        const std::string& host,
                        int port,
                        const std::string& token);

/// @brief 简单的阻塞接收循环，打印服务端下行文本帧。
/// @param fd 已握手完成的 WebSocket 连接 fd
void ReceiveLoop(int fd);


