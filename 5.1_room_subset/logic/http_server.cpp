// ============================================================================
// HTTP API 服务器实现
//
// 实现用户认证和消息发送的 HTTP 接口
// 支持 CORS 跨域、JSON 格式交互、token 身份认证
// ============================================================================
#include "http_server.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <sstream>
#include <unordered_map>

#include "function_timer.h"
#include "logging.h"
#include "message_helper.h"
#include "spark_push.grpc.pb.h"
#include "spark_push.pb.h"

namespace sparkpush {

using namespace muduo;
using namespace muduo::net;

namespace {

// ============================================================================
// 匿名命名空间：内部辅助函数和常量定义
// ============================================================================

// 兼容性宏：标记"保留但当前未被引用"的本地辅助函数
// 避免编译器产生 -Wunused-function 警告
#if defined(__GNUC__) || defined(__clang__)
#define SPARKPUSH_UNUSED __attribute__((unused))
#else
#define SPARKPUSH_UNUSED
#endif

// ============================================================================
// 预设管理员账号配置（Demo 级别，仅用于演示和运维）
// ============================================================================
//
// 设计说明：
// 1. 管理员账号硬编码在代码中，无需数据库记录
// 2. 前端将明文密码进行 MD5 hash 后传输
// 3. 后端只对 MD5 hash 进行字符串匹配
//
// 约定配置：
//   账号：admin
//   密码明文：admin123
//   密码 MD5（小写 32 位十六进制）：0192023a7bbd73250516f069df18b500
//
// 安全警告：
// 这是 Demo 级别的实现，仅适用于本地开发和内网环境
// 生产环境必须使用更安全的方案：
// - 使用带盐的哈希算法（如 bcrypt、scrypt、Argon2）
// - 多次迭代增加破解成本
// - 启用 HTTPS 加密传输
// - 使用更强的密码策略
// ============================================================================
const std::string kAdminAccount = "admin";
const std::string kAdminPasswordHash = "0192023a7bbd73250516f069df18b500";

// 管理员使用固定的 user_id
// 用于生成 token 和标识管理员身份
// 不依赖于数据库中是否真实存在该用户记录
const int64_t kAdminUserId = 900000000000LL;

// ============================================================================
// 辅助函数：请求解析和响应处理
// ============================================================================

// 解析 x-www-form-urlencoded 格式的请求体（旧实现，保留以兼容可能的其他调用）
//
// 功能说明：
// 解析类似 "key1=value1&key2=value2" 格式的字符串
// 返回键值对的 map
//
// 注意：当前 API 使用 JSON 格式，此函数暂未使用
// 保留此函数是为了兼容可能的其他调用或未来扩展
SPARKPUSH_UNUSED std::unordered_map<std::string, std::string> ParseForm(const std::string& body) {
    std::unordered_map<std::string, std::string> result;
    std::stringstream ss(body);
    std::string item;
    // 按 '&' 分割字符串
    while (std::getline(ss, item, '&')) {
        // 查找 '=' 分隔符
        auto pos = item.find('=');
        if (pos == std::string::npos) continue;
        std::string key = item.substr(0, pos);
        std::string value = item.substr(pos + 1);
        result[key] = value;
    }
    return result;
}

static int GetQueryInt(const muduo::net::HttpRequest& req, const std::string& key, int def_val) {
    // muduo HttpContext 会把 '?a=b' 存在 req.query()（带前导 '?'），req.path() 只含路径部分。
    // HttpRequest::getQuery 内部也依赖这一点，所以这里直接复用它，避免手写解析出错。
    std::string v = req.getQuery(key, "");
    if (v.empty()) return def_val;
    try {
        return std::stoi(v);
    } catch (...) {
        return def_val;
    }
}

static bool ExtractTokenFromRequestHeaders(const HttpRequest& req, std::string* token) {
    if (!token) return false;
    token->clear();
    // Authorization: Bearer <token>
    auto it = req.headers().find("Authorization");
    if (it != req.headers().end()) {
        const std::string& auth = it->second;
        const std::string prefix = "Bearer ";
        if (auth.size() > prefix.size() && auth.compare(0, prefix.size(), prefix) == 0) {
            *token = auth.substr(prefix.size());
        }
    }
    // Token: <token>
    if (token->empty()) {
        auto th = req.headers().find("Token");
        if (th != req.headers().end()) {
            *token = th->second;
        }
    }
    return !token->empty();
}

// 通用 JSON 解析工具
//
// 功能说明：
// 将 HTTP 请求体的 JSON 字符串解析为 nlohmann::json 对象
//
// 参数说明：
// @param body: JSON 字符串
// @param out: 输出参数，解析后的 JSON 对象
// @return: 解析成功返回 true，失败返回 false
//
// 错误处理：
// - body 为空：返回 false
// - JSON 格式错误：捕获异常，记录日志，返回 false
bool ParseJsonBody(const std::string& body, nlohmann::json* out) {
    if (!out) return false;
    if (body.empty()) {
        return false;
    }
    try {
        // 使用 nlohmann::json 库解析 JSON 字符串
        *out = nlohmann::json::parse(body);
        return true;
    } catch (const nlohmann::json::exception& e) {
        LOG_ERROR << "JSON parse error: " << e.what();
        return false;
    } catch (...) {
        return false;
    }
}

// CORS（跨域资源共享）统一处理
//
// 功能说明：
// 为 HTTP 响应添加 CORS 相关的响应头，允许跨域访问
//
// CORS 配置：
// - Access-Control-Allow-Origin: * （允许所有来源）
// - Access-Control-Allow-Methods: GET, POST, OPTIONS （允许的方法）
// - Access-Control-Allow-Headers: Content-Type （允许的请求头）
// - Access-Control-Max-Age: 86400 （预检请求缓存时间，24 小时）
//
// 使用场景：
// 前端页面（如 http://localhost:3000）需要调用后端 API（如
// http://localhost:9000） 浏览器会先发送 OPTIONS 预检请求，后端需要返回 CORS
// 响应头
//
// 注意：
// 生产环境建议配置具体的允许来源，而不是 "*"
void AddCORSHeaders(HttpResponse* resp) {
    if (!resp) return;
    resp->addHeader("Access-Control-Allow-Origin", "*");
    resp->addHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    // 允许前端在跨域请求中携带 JSON、Authorization/Token 等头部
    resp->addHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, Token");
    resp->addHeader("Access-Control-Max-Age", "86400");
}

// 解析登录请求的 JSON 数据（account 和 password）
//
// 功能说明：
// 从 JSON 字符串中提取 account 和 password 字段
//
// JSON 格式示例：
// {"account": "admin", "password": "0192023a7bbd73250516f069df18b500"}
//
// 参数说明：
// @param body: JSON 字符串
// @param account: 输出参数，账号
// @param password: 输出参数，密码（MD5 hash）
// @return: 解析成功返回 true，失败返回 false
//
// 验证规则：
// - account 和 password 字段必须存在
// - 两个字段的值必须是字符串类型
SPARKPUSH_UNUSED bool ParseJsonAccountPassword(const std::string& body, std::string* account,
                                               std::string* password) {
    if (!account || !password) return false;
    *account = "";
    *password = "";

    try {
        auto j = nlohmann::json::parse(body);
        // 检查必需字段是否存在
        if (!j.contains("account") || !j.contains("password")) {
            return false;
        }
        // 检查字段类型是否正确
        if (!j["account"].is_string() || !j["password"].is_string()) {
            return false;
        }
        // 提取字段值
        *account = j["account"].get<std::string>();
        *password = j["password"].get<std::string>();
        return true;
    } catch (const nlohmann::json::exception& e) {
        LOG_ERROR << "JSON parse error: " << e.what();
        return false;
    } catch (...) {
        return false;
    }
}

// 解析注册请求的 JSON 数据（account、password 和可选的 name）
//
// 功能说明：
// 从 JSON 字符串中提取注册所需的字段
//
// JSON 格式示例：
// {"account": "user1", "password": "5f4dcc3b5aa765d61d8327deb882cf99", "name":
// "用户1"}
//
// 参数说明：
// @param body: JSON 字符串
// @param account: 输出参数，账号（必需）
// @param password: 输出参数，密码（MD5 hash，必需）
// @param name: 输出参数，昵称（可选，默认使用 account）
// @return: 解析成功返回 true，失败返回 false
//
// 验证规则：
// - account 和 password 字段必须存在且为字符串
// - name 字段可选，如果未提供或为空，则使用 account 作为昵称
bool ParseJsonRegister(const std::string& body, std::string* account, std::string* password,
                       std::string* name) {
    if (!account || !password || !name) return false;
    *account = "";
    *password = "";
    *name = "";

    try {
        auto j = nlohmann::json::parse(body);
        // 检查必需字段
        if (!j.contains("account") || !j.contains("password")) {
            return false;
        }
        if (!j["account"].is_string() || !j["password"].is_string()) {
            return false;
        }
        // 提取必需字段
        *account = j["account"].get<std::string>();
        *password = j["password"].get<std::string>();
        // 提取可选的 name 字段
        if (j.contains("name") && j["name"].is_string()) {
            *name = j["name"].get<std::string>();
        }
        // 如果没有提供 name，默认使用 account 作为昵称
        if (name->empty()) {
            *name = *account;
        }
        return true;
    } catch (...) {
        return false;
    }
}

// 统一的 JSON 响应构造函数
//
// 功能说明：
// 构造标准格式的 JSON 响应体并设置 HTTP 响应
//
// 响应格式：
// {
//   "code": 0,           // 业务状态码，0 表示成功，非 0 表示各种错误
//   "message": "ok",     // 状态描述信息
//   "data": {...}        // 业务数据，JSON 对象
// }
//
// 参数说明：
// @param resp: HTTP 响应对象
// @param code: 业务状态码
// @param message: 状态描述信息
// @param data_json: 业务数据的 JSON 字符串，默认为 "{}"
// @param http_code: HTTP 状态码，默认为 200 OK
//
// 使用示例：
// WriteJson(resp, 0, "ok", "{\"user_id\":1001}");
// 生成：{"code":0,"message":"ok","data":{"user_id":1001}}
//
// 注意事项：
// - 所有响应都会自动添加 CORS 头，支持跨域访问
// - Content-Type 设置为 application/json
void WriteJson(HttpResponse* resp, int code, const std::string& message,
               const std::string& data_json = "{}",
               HttpResponse::HttpStatusCode http_code = HttpResponse::k200Ok) {
    resp->setStatusCode(http_code);
    resp->setContentType("application/json; charset=utf-8");
    // 所有 JSON API 统一添加 CORS 响应头
    // 方便前端页面（可能运行在不同端口）直接调用 API
    AddCORSHeaders(resp);
    // 构造标准格式的 JSON 响应体
    std::ostringstream oss;
    oss << "{\"code\":" << code << ",\"message\":\"" << message << "\""
        << ",\"data\":" << data_json << "}";
    resp->setBody(oss.str());
}

}  // namespace

// ============================================================================
// HttpApiServer 成员函数实现
// ============================================================================

// 构造函数：初始化 HTTP API 服务器
//
// 实现说明：
// 1. 保存业务依赖对象的指针（UserDao、RedisStore、KafkaProducer）
// 2. 创建 muduo HttpServer 对象
// 3. 注册 HTTP 请求回调函数
//
// 参数说明：
// @param loop: muduo 事件循环对象，管理网络 I/O 事件
// @param listenAddr: 监听地址和端口，格式如 "0.0.0.0:9000"
// @param user_dao: 用户数据访问对象，用于数据库操作
// @param redis_store: Redis 存储对象，用于 token 和路由管理
// @param push_producer: Kafka 生产者，用于发送推送消息
//
// 注意事项：
// - 依赖对象的生命周期由调用方（如 main 函数）管理
// - HttpServer 使用 lambda 表达式注册回调，捕获 this 指针
HttpApiServer::HttpApiServer(EventLoop* loop, const InetAddress& listenAddr, UserDao* user_dao,
                             RoomDao* room_dao, RedisStore* redis_store,
                             KafkaProducer* push_producer, int io_threads)
    : user_dao_(user_dao),
      room_dao_(room_dao),
      redis_store_(redis_store),
      push_producer_(push_producer),
      server_(loop, listenAddr, "logic_http_server") {
    // 提升并发：muduo HttpServer 支持多 EventLoop（多 IO 线程）模型。
    // 注意：handler 仍在各自的 IO 线程中同步执行，因此 DB/Redis 慢调用仍会阻塞该线程，
    // 但多线程能显著降低“单线程排队”带来的长尾延迟。
    if (io_threads <= 0) io_threads = 1;
    server_.setThreadNum(io_threads);

    // 注册 HTTP 请求回调函数
    // muduo HttpServer 的回调签名：bool (const TcpConnectionPtr&, HttpRequest&,
    // HttpResponse*) 返回 true 表示请求处理完成
    server_.setHttpCallback(
        [this](const TcpConnectionPtr&, HttpRequest& req, HttpResponse* resp) -> bool {
            this->onRequest(req, resp);  // 调用成员函数处理请求
            return true;
        });
}

// 启动 HTTP 服务器
//
// 功能说明：
// 调用 muduo HttpServer 的 start() 方法，开始监听端口并处理请求
//
// 注意事项：
// - 此方法是非阻塞的，不会阻塞调用线程
// - 实际的事件循环由 EventLoop::loop() 驱动
// - 在调用此方法后，需要调用 loop->loop() 进入事件循环
void HttpApiServer::start() { server_.start(); }

// 从 HTTP 请求中提取用户 ID
//
// 功能说明：
// 1. 从请求头中提取 token（支持两种格式）
// 2. 通过 Redis 验证 token 并获取对应的 user_id
//
// Token 提取方式（按优先级）：
// 1. Authorization 头：格式为 "Bearer <token>"
//    示例：Authorization: Bearer tk-1001-1234567890
//
// 2. Token 头：直接传递 token 值
//    示例：Token: tk-1001-1234567890
//
// 参数说明：
// @param req: HTTP 请求对象
// @param uid: 输出参数，返回用户 ID
// @return: 验证成功返回 true，失败返回 false
//
// 失败情况：
// - 请求头中没有 token
// - token 格式错误
// - token 无效或已过期
// - Redis 不可用
bool HttpApiServer::GetUserIdFromRequest(const HttpRequest& req, int64_t* uid) {
    if (!uid || !redis_store_) return false;

    std::string token;

    // 步骤1：尝试从 Authorization 头提取 token
    // 格式：Authorization: Bearer <token>
    auto it = req.headers().find("Authorization");
    if (it != req.headers().end()) {
        const std::string& auth = it->second;
        const std::string prefix = "Bearer ";
        // 检查是否以 "Bearer " 开头
        if (auth.size() > prefix.size() && auth.compare(0, prefix.size(), prefix) == 0) {
            // 提取 "Bearer " 后面的 token 部分
            token = auth.substr(prefix.size());
        }
    }

    // 步骤2：如果未找到 Authorization，尝试从 Token 头提取
    // 格式：Token: <token>
    if (token.empty()) {
        auto th = req.headers().find("Token");
        if (th != req.headers().end()) {
            token = th->second;
        }
    }

    // 步骤3：验证 token 是否提取成功
    if (token.empty()) return false;

    // 步骤4：通过 Redis 验证 token 并获取 user_id
    return redis_store_->GetUserIdByToken(token, uid);
}

// HTTP 请求统一入口
//
// 功能说明：
// 1. 处理 CORS 预检请求（OPTIONS）
// 2. 根据请求路径分发到具体的处理函数
// 3. 对未知路径返回 404 错误
//
// 处理流程：
// 1. 检查是否为 OPTIONS 请求（CORS 预检）
// 2. 在路由表中查找对应的处理函数
// 3. 调用处理函数或返回 404
//
// CORS 预检请求：
// 浏览器在发送跨域请求前，会先发送 OPTIONS 请求询问服务器是否允许
// 服务器需要返回 CORS 响应头告知浏览器允许跨域
//
// 路由表：
// - POST /api/login: 用户登录
// - POST /api/register: 用户注册
// - POST /api/message/send: 发送单聊消息
//
// @param req: HTTP 请求对象
// @param resp: HTTP 响应对象
void HttpApiServer::onRequest(const HttpRequest& req, HttpResponse* resp) {
    // 定义成员函数指针类型，用于路由表
    using Handler = void (HttpApiServer::*)(const HttpRequest&, HttpResponse*);

    // 步骤1：处理浏览器的 CORS 预检请求（OPTIONS）
    // 预检请求不需要走具体的业务逻辑，只需要返回 CORS 响应头
    if (req.method() == HttpRequest::kOptions) {
        // 返回 200 OK + CORS 响应头
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("text/plain; charset=utf-8");
        AddCORSHeaders(resp);
        return;
    }

    // 步骤2：构造路由表
    // 使用静态变量，只初始化一次，提高性能
    // key: 请求路径，value: 对应的处理函数
    static const std::unordered_map<std::string, Handler> kRouteTable =
        {{"/api/login", &HttpApiServer::handleLogin},
         {"/api/register", &HttpApiServer::handleRegister},
         {"/api/message/send", &HttpApiServer::handleSendMessage},
         {"/api/message/send-fast", &HttpApiServer::handleSendMessageFast},
         {"/api/message/test-send", &HttpApiServer::handleTestSendMessage},
         {"/api/room/create", &HttpApiServer::handleCreateRoom},
         {"/api/room/list", &HttpApiServer::handleRoomList},
         {"/api/room/join", &HttpApiServer::handleJoinRoom},
         {"/api/room/leave", &HttpApiServer::handleLeaveRoom},
         {"/api/room/users", &HttpApiServer::handleRoomUsers},
         {"/api/room/message/send", &HttpApiServer::handleSendRoomMessage}};

    // 步骤3：在路由表中查找对应的处理函数
    // 保险处理：即使上游把 query 拼进 path，也保证路由用纯路径匹配
    std::string path = req.path();
    auto qpos = path.find('?');
    if (qpos != std::string::npos) {
        path = path.substr(0, qpos);
    }
    auto it = kRouteTable.find(path);
    if (it == kRouteTable.end()) {
        // 未找到对应的路由，返回 404 错误
        WriteJson(resp, 404, "unknown path");
        return;
    }

    // 步骤4：调用对应的处理函数
    // 使用成员函数指针调用语法：(object.*pointer)(args)
    Handler handler = it->second;
    (this->*handler)(req, resp);
}

// 用户登录处理函数
//
// API 规格：
// - 请求方法：POST
// - 请求路径：/api/login
// - 请求体格式：{"account": "admin", "password": "<MD5 hash>"}
// - 响应体格式：{"code": 0, "message": "ok", "data": {"user_id": 1001, "token":
// "...", "name": "..."}}
//
// 功能说明：
// 1. 验证账号和密码（密码为 MD5 hash）
// 2. 特殊处理管理员账号（硬编码）
// 3. 普通用户从数据库查询验证
// 4. 登录成功后生成 token 并存储到 Redis
// 5. 返回用户信息和 token
//
// 错误码：
// - 405: 请求方法不是 POST
// - 400: 请求体为空或格式错误
// - 401: 密码错误
// - 404: 用户不存在
// - 500: 服务器内部错误（Redis 或数据库错误）
void HttpApiServer::handleLogin(const HttpRequest& req, HttpResponse* resp) {
    std::string account;
    std::string password;

    // 步骤1：验证请求方法
    // 登录接口只接受 POST 方法，拒绝 GET 等其他方法
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤2：验证请求体非空
    // 空请求体无法解析，直接返回错误
    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤3：解析 JSON 请求体
    // 提取 account 和 password 字段
    // 注意：password 应该是前端计算的 MD5 hash，不是明文
    if (!ParseJsonAccountPassword(req.body(), &account, &password)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"account\":\"...\",\"password\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤4：特殊处理管理员账号
    // 管理员账号硬编码在代码中，不依赖数据库
    // 用于演示、调试和运维操作
    if (account == kAdminAccount) {
        // 验证管理员密码（MD5 hash）
        if (password != kAdminPasswordHash) {
            WriteJson(resp, 401, "invalid password");
            return;
        }

        // 生成 token
        // 格式：tk-<user_id>-<timestamp>
        // 示例：tk-900000000000-1234567890
        std::string token = "tk-" + std::to_string(kAdminUserId) + "-" +
                            std::to_string(::time(nullptr));

        // 将 token 存入 Redis，设置 24 小时有效期
        // 存储格式：token:<token> -> <user_id>
        if (!redis_store_ || !redis_store_->SetToken(token, kAdminUserId, 24 * 3600)) {
            WriteJson(resp, 500, "save token to redis failed");
            return;
        }

        // 返回管理员登录成功响应
        std::ostringstream data;
        data << "{\"user_id\":" << kAdminUserId << ",\"token\":\"" << token << "\"}";
        WriteJson(resp, 0, "ok", data.str());
        return;
    }

    // 步骤5：验证账号非空
    // 虽然 ParseJsonAccountPassword 已经提取了 account
    // 但仍需要检查是否为空字符串
    if (account.empty()) {
        WriteJson(resp, 400, "account is required", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤6：检查数据库访问对象是否已初始化
    if (!user_dao_) {
        WriteJson(resp, 500, "user dao not initialized");
        return;
    }

    // 步骤7：从数据库查询用户信息
    // 根据账号查询用户记录，获取用户 ID、密码 hash 和昵称
    User user;
    std::string err;
    if (!user_dao_->GetUserByAccount(account, &user, &err)) {
        // 用户不存在，提示注册
        WriteJson(resp, 404, "user not found, please register first", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    // 步骤8：验证密码
    // 数据库中存储的是密码 hash，与前端传来的 hash 进行比较
    // 注意：这里假设前端已经对明文密码进行了 MD5 处理
    if (user.password_hash != password) {
        WriteJson(resp, 401, "invalid password");
        return;
    }

    // 步骤9：生成 token 并存入 Redis
    // Token 格式：tk-<user_id>-<timestamp>
    // 时间戳用于保证每次登录生成不同的 token
    std::string token = "tk-" + std::to_string(user.id) + "-" + std::to_string(::time(nullptr));

    // 将 token 存入 Redis，有效期 24 小时（86400 秒）
    if (!redis_store_ || !redis_store_->SetToken(token, user.id, 24 * 3600)) {
        WriteJson(resp, 500, "save token to redis failed");
        return;
    }

    // 步骤10：返回登录成功响应
    // 响应数据包含：user_id、token、name
    // 前端收到后应保存 token，用于后续请求的身份认证
    std::ostringstream data;
    data << "{\"user_id\":" << user.id << ",\"token\":\"" << token << "\""
         << ",\"name\":\"" << user.name << "\"}";
    WriteJson(resp, 0, "ok", data.str());
}

// 用户注册处理函数
//
// API 规格：
// - 请求方法：POST
// - 请求路径：/api/register
// - 请求体格式：{"account": "user1", "password": "<MD5 hash>", "name": "用户1"}
// - 响应体格式：{"code": 0, "message": "ok", "data": {"user_id": 1001, "token":
// "...", "name": "..."}}
//
// 功能说明：
// 1. 验证请求参数（账号、密码必填，昵称可选）
// 2. 检查账号是否已存在
// 3. 创建新用户记录到数据库
// 4. 生成 token 并存储到 Redis
// 5. 返回用户信息和 token（注册即登录）
//
// 错误码：
// - 405: 请求方法不是 POST
// - 400: 请求体为空、格式错误或参数缺失
// - 409: 账号已存在或尝试注册保留账号
// - 500: 服务器内部错误（数据库或 Redis 错误）
//
// 注意事项：
// - 管理员账号（admin）是保留账号，禁止注册
// - 如果未提供 name，默认使用 account 作为昵称
// - 密码应该是前端计算的 MD5 hash
void HttpApiServer::handleRegister(const HttpRequest& req, HttpResponse* resp) {
    std::string account;
    std::string password;
    std::string name;

    // 步骤1：验证请求方法
    // 注册接口只接受 POST 方法
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤2：验证请求体非空
    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤3：解析 JSON 请求体
    // 提取 account、password 和可选的 name 字段
    if (!ParseJsonRegister(req.body(), &account, &password, &name)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"account\":\"...\",\"password\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤4：验证必填参数
    // account 和 password 不能为空
    if (account.empty() || password.empty()) {
        WriteJson(resp, 400, "account/password are required", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤5：检查是否为保留的管理员账号
    // 管理员账号硬编码在代码中，禁止通过注册接口创建
    if (account == kAdminAccount) {
        WriteJson(resp, 409, "admin account is reserved, please use login directly", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    // 步骤6：检查数据库访问对象是否已初始化
    if (!user_dao_) {
        WriteJson(resp, 500, "user dao not initialized");
        return;
    }

    // 步骤7：检查账号是否已存在
    // 尝试查询账号，如果找到则说明已被注册
    User existing;
    std::string err;
    if (user_dao_->GetUserByAccount(account, &existing, &err)) {
        // 账号已存在，不允许重复注册
        WriteJson(resp, 409, "account already exists", "{}", HttpResponse::k400BadRequest);
        return;
    } else if (!err.empty() && err != "user not found") {
        // 数据库查询出现真实错误（非"用户不存在"错误）
        WriteJson(resp, 500, "query user failed: " + err);
        return;
    }

    // 步骤8：创建新用户
    // 将用户信息插入数据库，返回新分配的 user_id
    int64_t uid = 0;
    if (!user_dao_->CreateUser(account, name, password, &uid, &err)) {
        WriteJson(resp, 500, "register user failed: " + err);
        return;
    }

    // 步骤9：构造用户对象
    // 用于后续生成 token 和响应
    User user;
    user.id = uid;
    user.account = account;
    user.name = name;
    user.password_hash = password;

    // 步骤10：生成 token 并存入 Redis
    // 注册成功后直接下发 token，实现"注册即登录"
    // Token 格式：tk-<user_id>-<timestamp>
    std::string token = "tk-" + std::to_string(user.id) + "-" + std::to_string(::time(nullptr));

    // 将 token 存入 Redis，有效期 24 小时
    if (!redis_store_ || !redis_store_->SetToken(token, user.id, 24 * 3600)) {
        WriteJson(resp, 500, "save token to redis failed");
        return;
    }

    // 步骤11：返回注册成功响应
    // 响应数据包含：user_id、token、name
    // 前端收到后可以直接使用 token，无需再次登录
    std::ostringstream data;
    data << "{\"user_id\":" << user.id << ",\"token\":\"" << token << "\""
         << ",\"name\":\"" << user.name << "\"}";
    WriteJson(resp, 0, "ok", data.str());
}

// 单聊消息发送处理函数
//
// API 规格：
// - 请求方法：POST
// - 请求路径：/api/message/send
// - 请求头：Authorization: Bearer <token> 或 Token: <token>
// - 请求体格式：
//   {
//     "msg_type": "text",           // 消息类型：text/image/video 等
//     "target_type": "single_chat", // 目标类型：single_chat（单聊）
//     "target_id": 1002,            // 目标用户 ID
//     "content": "你好",            // 消息内容
//     "client_msg_id": "...",       // 客户端消息 ID（可选）
//     // 最终协议：不再接收客户端时间戳（timestamp/client_timestamp_ms）
//   }
// - 响应体格式：
//   {
//     "code": 0,
//     "message": "ok",
//     "data": {
//       "msg_id": "msgid:1001:1002-1", // 服务端生成的消息 ID
//       "msg_seq": 1,                  // 消息序号
//       "session_id": "s_1001:1002"    // 会话 ID
//     }
//   }
//
// 功能说明：
// 1. 验证用户身份（从 token 获取发送方 user_id）
// 2. 解析并验证请求参数
// 3. 生成消息 ID 和序号（使用 Redis INCR）
// 4. 构造 ChatMessage 对象
// 5. 调用 PostProcessSingleMessage 推送消息到 Kafka
// 6. 返回消息 ID、序号和会话 ID
//
// 错误码：
// - 405: 请求方法不是 POST
// - 401: 未提供 token 或 token 无效
// - 400: 请求体格式错误或参数不合法
// - 500: 生成消息 ID 失败
//
// 注意事项：
// - 当前只支持单聊（single_chat），不支持群聊
// - 消息序号在会话级别全局递增，用于排序和去重
// - 会话 ID 格式：s_<小uid>:<大uid>
void HttpApiServer::handleSendMessage(const HttpRequest& req, HttpResponse* resp) {
    // 步骤1：验证请求方法
    // 发送消息接口只接受 POST 方法
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤2：验证用户身份
    // 从请求头的 token 中提取发送方的 user_id
    int64_t from_user = 0;
    if (!GetUserIdFromRequest(req, &from_user)) {
        // Token 无效或已过期
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }

    // 步骤3：解析 JSON 请求体
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"msg_type\":\"text\",\"target_type\":\"single_chat\",\"target_"
                  "id\":2,\"content\":\"...\",\"client_msg_id\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 步骤4：提取并验证请求参数
    // 定义变量存储提取的字段
    std::string msg_type = "text";  // 消息类型，默认为 text
    std::string target_type;        // 目标类型，必需
    int64_t target_id = 0;          // 目标用户 ID，必需
    std::string content;            // 消息内容，必需
    std::string client_msg_id;      // 客户端消息 ID，可选

    try {
        // 检查必需字段是否存在
        if (!j.contains("target_type") || !j.contains("target_id") || !j.contains("content")) {
            WriteJson(resp, 400, "target_type/target_id/content required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }

        // 提取必需字段
        target_type = j.at("target_type").get<std::string>();
        target_id = j.at("target_id").get<int64_t>();
        content = j.at("content").get<std::string>();

        // 提取可选字段
        if (j.contains("msg_type")) msg_type = j.at("msg_type").get<std::string>();
        if (j.contains("client_msg_id")) client_msg_id = j.at("client_msg_id").get<std::string>();
        // 最终协议：忽略 timestamp/client_timestamp_ms
    } catch (const nlohmann::json::exception&) {
        // JSON 字段类型不匹配（如 target_id 不是数字）
        WriteJson(resp, 400, "invalid json fields in send message", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    // 步骤5：验证参数合法性
    // 当前只支持单聊，目标 ID 必须为正数，内容不能为空
    if (target_type != "single_chat" || target_id <= 0 || content.empty()) {
        WriteJson(resp, 400, "only single_chat supported and target_id must be positive", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    // 步骤6：计算会话 ID
    // 会话 ID 由两个用户 ID 组成，按大小排序（小的在前）
    // 格式：s_<小uid>:<大uid>
    // 示例：用户 1001 和 1002 的会话 ID 为 "s_1001:1002"
    int64_t small_uid = std::min(from_user, target_id);
    int64_t large_uid = std::max(from_user, target_id);
    std::string session_id = "s_" + std::to_string(small_uid) + ":" + std::to_string(large_uid);

    // 步骤7：生成消息 ID 和序号
    // 调用 Redis INCR 生成全局唯一且递增的消息序号
    std::string msg_id;
    int64_t msg_seq = 0;
    if (redis_store_ && redis_store_->NextSingleMsgId(small_uid, large_uid, &msg_seq)) {
        // 消息 ID 格式：msgid:<小uid>:<大uid>-<序号>
        // 示例：msgid:1001:1002-1
        msg_id = "msgid:" + std::to_string(small_uid) + ":" + std::to_string(large_uid) + "-" +
                 std::to_string(msg_seq);
    } else {
        // Redis 不可用或 INCR 失败
        WriteJson(resp, 500, "generate msg_id failed");
        return;
    }

    // 步骤8：获取服务端时间戳（毫秒）
    // 用于记录消息的创建时间
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 步骤9：规范化消息内容 JSON
    // 去重同义字段，补充服务端生成的字段
    // 最终存储和传输的 content_json 包含完整的消息信息
    nlohmann::json norm;
    norm["msg_id"] = msg_id;              // 服务端生成的消息 ID
    norm["msg_seq"] = msg_seq;            // 服务端生成的消息序号
    norm["create_time"] = now_ms;         // 服务端时间戳
    norm["from_user_id"] = from_user;     // 发送方用户 ID
    norm["target_type"] = "single_chat";  // 目标类型
    norm["target_id"] = target_id;        // 接收方用户 ID
    norm["msg_type"] = msg_type;          // 消息类型
    norm["content"] = content;            // 消息内容
    // 可选字段
    if (!client_msg_id.empty()) norm["client_msg_id"] = client_msg_id;
    // 最终协议：不写 client_timestamp_ms

    // 将 JSON 对象序列化为字符串
    std::string final_json = norm.dump();

    // 步骤10：构造 ChatMessage protobuf 对象
    // 用于推送到 comet 和持久化存储
    ChatMessage cm;
    cm.set_msg_id(msg_id);
    cm.set_session_id(session_id);
    cm.set_msg_seq(msg_seq);
    cm.set_sender_id(from_user);
    cm.set_timestamp_ms(now_ms);
    cm.set_msg_type(msg_type);
    cm.set_content_json(final_json);  // 存储完整的 JSON
    cm.set_client_msg_id(client_msg_id);

    // 步骤11：调用后置处理函数
    // 负责将消息推送到目标用户的 comet 节点
    // 此处忽略返回值，因为即使推送失败，消息也已经生成成功
    std::string preview = content.size() > 50 ? content.substr(0, 50) : content;
    PostProcessSingleMessage(target_id, session_id, cm, redis_store_, push_producer_);

    // 步骤12：返回成功响应
    // 响应数据包含：msg_id、msg_seq、session_id
    // 前端可以根据这些信息更新 UI 和进行消息去重
    std::ostringstream data;
    data << "{"
         << "\"msg_id\":\"" << msg_id << "\","
         << "\"msg_seq\":" << msg_seq << ","
         << "\"session_id\":\"" << session_id << "\""
         << "}";
    WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleSendMessageFast(const HttpRequest& req, HttpResponse* resp) {
    FUNCTION_TIMER();
    // 优化版：一次 Redis 往返完成鉴权 + msg_seq 分配 + 目标路由查询（Lua/EVALSHA）
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 先解析 body，拿到 target_id 才能调用 Lua
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"msg_type\":\"text\",\"target_type\":\"single_chat\",\"target_"
                  "id\":2,\"content\":\"...\",\"client_msg_id\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    std::string msg_type = "text";
    std::string target_type;
    int64_t target_id = 0;
    std::string content;
    std::string client_msg_id;
    try {
        if (!j.contains("target_type") || !j.contains("target_id") || !j.contains("content")) {
            WriteJson(resp, 400, "target_type/target_id/content required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        target_type = j.at("target_type").get<std::string>();
        target_id = j.at("target_id").get<int64_t>();
        content = j.at("content").get<std::string>();
        if (j.contains("msg_type")) msg_type = j.at("msg_type").get<std::string>();
        if (j.contains("client_msg_id")) client_msg_id = j.at("client_msg_id").get<std::string>();
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "invalid json fields in send message", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (target_type != "single_chat" || target_id <= 0 || content.empty()) {
        WriteJson(resp, 400, "only single_chat supported and target_id must be positive", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    std::string token;
    if (!ExtractTokenFromRequestHeaders(req, &token)) {
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }

    int64_t from_user = 0;
    int64_t small_uid = 0;
    int64_t large_uid = 0;
    int64_t msg_seq = 0;
    std::vector<std::string> target_comets;
    std::string redis_err;
    if (!redis_store_ ||
        !redis_store_->AuthAllocSeqAndGetTargetComets(token, target_id, &from_user,
                                                              &small_uid, &large_uid, &msg_seq,
                                                              &target_comets, &redis_err)) {
        if (redis_err == "unauthorized") {
            WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        } else if (redis_err == "invalid_target") {
            WriteJson(resp, 400, "invalid target_id", "{}", HttpResponse::k400BadRequest);
        } else {
            WriteJson(resp, 500, "redis failed: " + redis_err, "{}",
                      HttpResponse::k500InternalServerError);
        }
        return;
    }

    std::string session_id = "s_" + std::to_string(small_uid) + ":" + std::to_string(large_uid);
    std::string msg_id = "msgid:" + std::to_string(small_uid) + ":" + std::to_string(large_uid) +
                         "-" + std::to_string(msg_seq);

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    nlohmann::json norm;
    norm["msg_id"] = msg_id;
    norm["msg_seq"] = msg_seq;
    norm["create_time"] = now_ms;
    norm["from_user_id"] = from_user;
    norm["target_type"] = "single_chat";
    norm["target_id"] = target_id;
    norm["msg_type"] = msg_type;
    norm["content"] = content;
    if (!client_msg_id.empty()) norm["client_msg_id"] = client_msg_id;
    std::string final_json = norm.dump();

    ChatMessage cm;
    cm.set_msg_id(msg_id);
    cm.set_session_id(session_id);
    cm.set_msg_seq(msg_seq);
    cm.set_sender_id(from_user);
    cm.set_timestamp_ms(now_ms);
    cm.set_msg_type(msg_type);
    cm.set_content_json(final_json);
    cm.set_client_msg_id(client_msg_id);

    PostProcessSingleMessageWithComets(target_id, session_id, cm, target_comets, push_producer_);

    std::ostringstream data;
    data << "{"
         << "\"msg_id\":\"" << msg_id << "\","
         << "\"msg_seq\":" << msg_seq << ","
         << "\"session_id\":\"" << session_id << "\""
         << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// 分段压测：按 stage 逐步启用逻辑，便于用 wrk/ab 等工具做差分分析。
void HttpApiServer::handleTestSendMessage(const HttpRequest& req, HttpResponse* resp) {
    FUNCTION_TIMER();

    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int stage = GetQueryInt(req, "stage", 0);
    if (stage < 0) stage = 0;
    if (stage > 4) stage = 4;

    // stage=0：最小路径，不解析 body、不鉴权
    if (stage == 0) {
        WriteJson(resp, 0, "ok", "{\"stage\":0}");
        return;
    }

    // stage>=1：鉴权（Redis token->uid）
    int64_t from_user = 0;
    if (!GetUserIdFromRequest(req, &from_user)) {
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }
    if (stage == 1) {
        std::ostringstream data;
        data << "{\"stage\":1,\"from_user_id\":" << from_user << "}";
        WriteJson(resp, 0, "ok", data.str());
        return;
    }

    // stage>=2：解析/校验 body（目标用户 + content）
    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    std::string msg_type = "text";
    std::string target_type;
    int64_t target_id = 0;
    std::string content;
    std::string client_msg_id;
    try {
        if (!j.contains("target_type") || !j.contains("target_id") || !j.contains("content")) {
            WriteJson(resp, 400, "target_type/target_id/content required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        target_type = j.at("target_type").get<std::string>();
        target_id = j.at("target_id").get<int64_t>();
        content = j.at("content").get<std::string>();
        if (j.contains("msg_type")) msg_type = j.at("msg_type").get<std::string>();
        if (j.contains("client_msg_id")) client_msg_id = j.at("client_msg_id").get<std::string>();
    } catch (...) {
        WriteJson(resp, 400, "invalid json fields", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (target_type != "single_chat" || target_id <= 0) {
        WriteJson(resp, 400, "only single_chat supported and target_id must be positive", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    int64_t small_uid = std::min(from_user, target_id);
    int64_t large_uid = std::max(from_user, target_id);
    std::string session_id = "s_" + std::to_string(small_uid) + ":" + std::to_string(large_uid);

    // stage>=2：分配 msg_seq/msg_id（Redis INCR）
    int64_t msg_seq = 0;
    if (!redis_store_ || !redis_store_->NextSingleMsgId(small_uid, large_uid, &msg_seq)) {
        WriteJson(resp, 500, "alloc msg_seq failed", "{}", HttpResponse::k500InternalServerError);
        return;
    }
    std::string msg_id = "msgid:" + std::to_string(small_uid) + ":" + std::to_string(large_uid) +
                         "-" + std::to_string(msg_seq);

    if (stage == 2) {
        std::ostringstream data;
        data << "{"
             << "\"stage\":2,"
             << "\"msg_id\":\"" << msg_id << "\","
             << "\"msg_seq\":" << msg_seq << ","
             << "\"session_id\":\"" << session_id << "\""
             << "}";
        WriteJson(resp, 0, "ok", data.str());
        return;
    }

    // stage>=3：构造 ChatMessage 与规范化 content_json（不投递 Kafka）
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    nlohmann::json norm;
    norm["msg_id"] = msg_id;
    norm["msg_seq"] = msg_seq;
    norm["create_time"] = now_ms;
    norm["from_user_id"] = from_user;
    norm["target_type"] = "single_chat";
    norm["target_id"] = target_id;
    norm["msg_type"] = msg_type;
    norm["content"] = content;
    if (!client_msg_id.empty()) norm["client_msg_id"] = client_msg_id;
    std::string final_json = norm.dump();

    ChatMessage cm;
    cm.set_msg_id(msg_id);
    cm.set_session_id(session_id);
    cm.set_msg_seq(msg_seq);
    cm.set_sender_id(from_user);
    cm.set_timestamp_ms(now_ms);
    cm.set_msg_type(msg_type);
    cm.set_content_json(final_json);
    cm.set_client_msg_id(client_msg_id);

    if (stage == 3) {
        std::ostringstream data;
        data << "{"
             << "\"stage\":3,"
             << "\"msg_id\":\"" << msg_id << "\","
             << "\"msg_seq\":" << msg_seq << ","
             << "\"session_id\":\"" << session_id << "\""
             << "}";
        WriteJson(resp, 0, "ok", data.str());
        return;
    }

    // stage==4：调用后置处理（Kafka 投递/后置处理）
    PostProcessSingleMessage(target_id, session_id, cm, redis_store_, push_producer_);
    std::ostringstream data;
    data << "{"
         << "\"stage\":4,"
         << "\"msg_id\":\"" << msg_id << "\","
         << "\"msg_seq\":" << msg_seq << ","
         << "\"session_id\":\"" << session_id << "\""
         << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// ============================================================================
// 房间相关 HTTP 接口实现（新增）
// ============================================================================

// 创建房间处理函数
//
// API 规格：
// - 请求方法：POST
// - 请求路径：/api/room/create
// - 请求头：Authorization: Bearer <token> 或 Token: <token>
// - 请求体：{"name": "房间名称", "group_type": 1}
// - 响应体：{"code": 0, "message": "ok", "data": {"room_id": 123}}
void HttpApiServer::handleCreateRoom(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 可以只允许特定用户创建房间（可选）
    // 暂时先不做限制，限制后不方便测试

    // 验证用户身份
    int64_t user_id = 0;
    if (!GetUserIdFromRequest(req, &user_id)) {
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }

    // 解析请求体
    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    std::string room_name;
    int group_type = 1;  // 默认聊天室
    try {
        auto j = nlohmann::json::parse(req.body());
        if (j.contains("name") && j["name"].is_string()) {
            room_name = j["name"].get<std::string>();
        }
        if (j.contains("group_type") && j["group_type"].is_number_integer()) {
            group_type = j["group_type"].get<int>();
        }
    } catch (...) {
        WriteJson(resp, 400, "invalid JSON", "{}", HttpResponse::k400BadRequest);
        return;
    }

    if (room_name.empty()) {
        WriteJson(resp, 400, "room name required", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (group_type <= 0) {
        WriteJson(resp, 400, "invalid group_type", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 创建房间（事务：创建房间 + 自动加入创建者）
    int64_t room_id = 0;
    std::string err_msg;
    if (!room_dao_ || !room_dao_->CreateRoom(room_name, user_id, group_type, &room_id, &err_msg)) {
        WriteJson(resp, 500, "create room failed: " + err_msg, "{}",
                  HttpResponse::k500InternalServerError);
        return;
    }

    // 写穿 Redis 缓存（创建者自动是成员）
    if (redis_store_) {
        redis_store_->AddRoomMemberToCache(room_id, user_id, 300);
    }

    // 返回成功响应
    std::ostringstream data;
    data << "{\"room_id\":" << room_id << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// 加入房间处理函数
//
// API 规格：
// - 请求方法：POST
// - 请求路径：/api/room/join
// - 请求头：Authorization: Bearer <token>
// - 请求体：{"room_id": 123}
// - 响应体：{"code": 0, "message": "ok", "data": {}}
void HttpApiServer::handleJoinRoom(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    if (!GetUserIdFromRequest(req, &user_id)) {
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }

    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t room_id = 0;
    try {
        auto j = nlohmann::json::parse(req.body());
        if (j.contains("room_id") && j["room_id"].is_number_integer()) {
            room_id = j["room_id"].get<int64_t>();
        }
    } catch (...) {
        WriteJson(resp, 400, "invalid JSON", "{}", HttpResponse::k400BadRequest);
        return;
    }

    if (room_id <= 0) {
        WriteJson(resp, 400, "invalid room_id", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 加入房间
    std::string err_msg;
    if (!room_dao_ || !room_dao_->JoinRoom(room_id, user_id, "member", &err_msg)) {
        // 常见场景：重复加入 -> 409
        if (err_msg.find("Duplicate") != std::string::npos ||
            err_msg.find("duplicate") != std::string::npos) {
            WriteJson(resp, 409, "already joined", "{}", HttpResponse::k400BadRequest);
        } else {
            WriteJson(resp, 500, "join room failed: " + err_msg, "{}",
                      HttpResponse::k500InternalServerError);
        }
        return;
    }

    // 写穿 Redis 缓存
    if (redis_store_) {
        redis_store_->AddRoomMemberToCache(room_id, user_id, 300);
    }

    WriteJson(resp, 0, "ok", "{}");
}

// 离开房间处理函数
//
// API 规格：
// - 请求方法：POST
// - 请求路径：/api/room/leave
// - 请求头：Authorization: Bearer <token>
// - 请求体：{"room_id": 123}
// - 响应体：{"code": 0, "message": "ok", "data": {}}
void HttpApiServer::handleLeaveRoom(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    if (!GetUserIdFromRequest(req, &user_id)) {
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }

    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t room_id = 0;
    try {
        auto j = nlohmann::json::parse(req.body());
        if (j.contains("room_id") && j["room_id"].is_number_integer()) {
            room_id = j["room_id"].get<int64_t>();
        }
    } catch (...) {
        WriteJson(resp, 400, "invalid JSON", "{}", HttpResponse::k400BadRequest);
        return;
    }

    if (room_id <= 0) {
        WriteJson(resp, 400, "invalid room_id", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 离开房间
    std::string err_msg;
    if (!room_dao_ || !room_dao_->LeaveRoom(room_id, user_id, &err_msg)) {
        if (err_msg == "not room member") {
            WriteJson(resp, 404, "not room member", "{}", HttpResponse::k400BadRequest);
        } else {
            WriteJson(resp, 500, "leave room failed: " + err_msg, "{}",
                      HttpResponse::k500InternalServerError);
        }
        return;
    }

    // 写穿 Redis 缓存
    if (redis_store_) {
        redis_store_->RemoveRoomMemberFromCache(room_id, user_id);
    }

    WriteJson(resp, 0, "ok", "{}");
}

// 查询用户已加入的房间列表处理函数
//
// API 规格：
// - 请求方法：GET
// - 请求路径：/api/room/list
// - 请求头：Authorization: Bearer <token>
// - 响应体：{"code": 0, "message": "ok", "data": {"rooms": [{"room_id":123,
// "name":"房间1"}]}}
//
// 注意：本阶段简化实现，返回用户作为成员的所有房间
void HttpApiServer::handleRoomList(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kGet) {
        WriteJson(resp, 405, "only GET allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    if (!GetUserIdFromRequest(req, &user_id)) {
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }

    if (!room_dao_) {
        WriteJson(resp, 500, "room dao not initialized", "{}",
                  HttpResponse::k500InternalServerError);
        return;
    }

    std::vector<Room> rooms;
    std::string err_msg;
    if (!room_dao_->GetUserRooms(user_id, &rooms, &err_msg)) {
        WriteJson(resp, 500, "get room list failed: " + err_msg, "{}",
                  HttpResponse::k500InternalServerError);
        return;
    }

    // 统一返回结构：data = {"rooms":[...]}
    nlohmann::json data;
    data["rooms"] = nlohmann::json::array();
    for (const auto& r : rooms) {
        nlohmann::json item;
        item["room_id"] = r.id;
        item["name"] = r.name;
        item["owner_id"] = r.owner_id;
        item["group_type"] = r.group_type;
        data["rooms"].push_back(std::move(item));
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 查询房间成员列表处理函数
//
// API 规格：
// - 请求方法：POST（匹配前端实现）
// - 请求路径：/api/room/users
// - 请求头：Authorization: Bearer <token>
// - 请求体：{"room_id": 123}
// - 响应体：{"code": 0, "message": "ok", "data": {"users": [1001, 1002]}}
void HttpApiServer::handleRoomUsers(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    if (!GetUserIdFromRequest(req, &user_id)) {
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }

    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 从请求体解析 room_id
    int64_t room_id = 0;
    try {
        auto j = nlohmann::json::parse(req.body());
        if (j.contains("room_id") && j["room_id"].is_number_integer()) {
            room_id = j["room_id"].get<int64_t>();
        }
    } catch (...) {
        WriteJson(resp, 400, "invalid JSON", "{}", HttpResponse::k400BadRequest);
        return;
    }

    if (room_id <= 0) {
        WriteJson(resp, 400, "invalid room_id", "{}", HttpResponse::k400BadRequest);
        return;
    }

    if (!room_dao_) {
        WriteJson(resp, 500, "room dao not initialized", "{}",
                  HttpResponse::k500InternalServerError);
        return;
    }

    // 成员校验：只有房间成员才能查询成员列表
    {
        std::string err;
        if (!room_dao_->IsMember(room_id, user_id, &err)) {
            if (!err.empty()) {
                WriteJson(resp, 500, "check member failed: " + err, "{}",
                          HttpResponse::k500InternalServerError);
            } else {
                WriteJson(resp, 403, "not room member", "{}", HttpResponse::k403Forbidden);
            }
            return;
        }
    }

    // 查询房间成员
    std::vector<int64_t> user_ids;
    std::string err_msg;
    if (!room_dao_->GetRoomMembers(room_id, &user_ids, &err_msg)) {
        WriteJson(resp, 500, "get room members failed: " + err_msg, "{}",
                  HttpResponse::k500InternalServerError);
        return;
    }

    // 构造 JSON 数组
    std::ostringstream oss;
    oss << "{\"users\":[";
    for (size_t i = 0; i < user_ids.size(); ++i) {
        if (i > 0) oss << ",";
        oss << user_ids[i];
    }
    oss << "]}";
    WriteJson(resp, 0, "ok", oss.str());
}

// 发送房间消息处理函数
//
// API 规格：
// - 请求方法：POST
// - 请求路径：/api/room/message/send
// - 请求头：Authorization: Bearer <token>
// -
// 请求体：{"target_type":"room","target_id":123,"msg_type":"text","content":"hello","client_msg_id":"..."}
// - 响应体：{"code": 0, "message": "ok", "data": {"msg_id": "...", "msg_seq":
// 1}}
void HttpApiServer::handleSendRoomMessage(const HttpRequest& req, HttpResponse* resp) {
    FUNCTION_TIMER();
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t from_user = 0;
    if (!GetUserIdFromRequest(req, &from_user)) {
        WriteJson(resp, 401, "unauthorized", "{}", HttpResponse::k401Unauthorized);
        return;
    }

    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    std::string target_type;
    int64_t target_id = 0;
    std::string content;
    std::string msg_type = "text";
    std::string client_msg_id;
    try {
        auto j = nlohmann::json::parse(req.body());
        if (j.contains("target_type") && j["target_type"].is_string()) {
            target_type = j["target_type"].get<std::string>();
        }
        if (j.contains("target_id") && j["target_id"].is_number_integer()) {
            target_id = j["target_id"].get<int64_t>();
        }
        if (j.contains("content") && j["content"].is_string()) {
            content = j["content"].get<std::string>();
        }
        if (j.contains("msg_type") && j["msg_type"].is_string()) {
            msg_type = j["msg_type"].get<std::string>();
        }
        if (j.contains("client_msg_id") && j["client_msg_id"].is_string()) {
            client_msg_id = j["client_msg_id"].get<std::string>();
        }
    } catch (...) {
        WriteJson(resp, 400, "invalid JSON", "{}", HttpResponse::k400BadRequest);
        return;
    }

    if (target_type != "room" || target_id <= 0 || content.empty()) {
        WriteJson(resp, 400, "target_type=room, target_id and content required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    const int64_t room_id = target_id;

    // 成员校验：发送者必须是房间成员
    if (!room_dao_) {
        WriteJson(resp, 500, "room dao not initialized", "{}",
                  HttpResponse::k500InternalServerError);
        return;
    }
    {
        // 性能优化：优先 Redis SISMEMBER(room_members:<room_id>)，miss 再回源 MySQL 并回填。
        bool cached_member = false;
        bool has_cache = false;
        if (redis_store_) {
            has_cache = redis_store_->IsRoomMemberFast(room_id, from_user, &cached_member);
        }
        if (!(has_cache && cached_member)) {
            std::string err;
            if (!room_dao_->IsMember(room_id, from_user, &err)) {
                if (!err.empty()) {
                    WriteJson(resp, 500, "check member failed: " + err, "{}",
                              HttpResponse::k500InternalServerError);
                } else {
                    WriteJson(resp, 403, "not room member", "{}", HttpResponse::k403Forbidden);
                }
                return;
            }
            // 回填 Redis（best-effort）
            if (redis_store_) {
                redis_store_->AddRoomMemberToCache(room_id, from_user, 300);
            }
        }
    }

    // 生成房间消息序号
    int64_t msg_seq = 0;
    if (!redis_store_ || !redis_store_->NextRoomMsgId(room_id, &msg_seq)) {
        WriteJson(resp, 500, "alloc msg_seq failed", "{}", HttpResponse::k500InternalServerError);
        return;
    }

    std::string msg_id = "room_msgid:" + std::to_string(room_id) + "-" + std::to_string(msg_seq);

    // 获取服务端时间戳
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 构造规范化的 content_json
    nlohmann::json j_content;
    j_content["content"] = content;
    j_content["msg_id"] = msg_id;
    j_content["msg_seq"] = msg_seq;
    j_content["create_time"] = now_ms;
    j_content["from_user_id"] = from_user;
    j_content["target_type"] = "room";
    j_content["target_id"] = room_id;
    j_content["msg_type"] = msg_type;
    if (!client_msg_id.empty()) {
        j_content["client_msg_id"] = client_msg_id;
    }
    std::string final_json = j_content.dump();

    // 构造 ChatMessage
    ChatMessage cm;
    cm.set_msg_id(msg_id);
    cm.set_session_id("room:" + std::to_string(room_id));
    cm.set_msg_seq(msg_seq);
    cm.set_sender_id(from_user);
    cm.set_timestamp_ms(now_ms);
    cm.set_msg_type(msg_type);
    cm.set_content_json(final_json);
    cm.set_client_msg_id(client_msg_id);

    // 调用房间消息后置处理（精准投递）
    auto fallback = [this](int64_t rid, std::vector<int64_t>* uids) -> bool {
        if (!room_dao_) return false;
        std::string err;
        return room_dao_->GetRoomMembers(rid, uids, &err);
    };
    PostProcessRoomMessage(room_id, cm, redis_store_, push_producer_, fallback);

    // 返回成功响应
    std::ostringstream data;
    data << "{"
         << "\"msg_id\":\"" << msg_id << "\","
         << "\"msg_seq\":" << msg_seq << "}";
    WriteJson(resp, 0, "ok", data.str());
}

}  // namespace sparkpush
