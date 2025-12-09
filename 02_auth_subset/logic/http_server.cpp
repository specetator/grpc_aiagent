// ============================================================================
// HTTP API 服务器实现
// 
// 处理用户注册、登录等 HTTP 请求，返回 JSON 格式响应
// ============================================================================
#include "http_server.h"

#include <ctime>
#include <nlohmann/json.hpp>
#include <sstream>
#include <unordered_map>

#include "logging.h"

namespace sparkpush {

using namespace muduo;
using namespace muduo::net;

namespace {

// ============================================================================
// 内部常量和辅助函数
// ============================================================================

// 预设管理员账号（用于演示和测试，生产环境应从配置读取）
const std::string kAdminAccount = "admin";
const std::string kAdminPasswordHash = "0192023a7bbd73250516f069df18b500";  // MD5("admin123")
const int64_t kAdminUserId = 900000000000LL;  // 管理员用户 ID

// 添加 CORS 响应头，支持跨域请求
// 允许任何域名的 Web 前端调用 API（生产环境应限制具体域名）
void AddCORSHeaders(HttpResponse* resp) {
    if (!resp) return;
    resp->addHeader("Access-Control-Allow-Origin", "*");
    resp->addHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    resp->addHeader("Access-Control-Allow-Headers", "Content-Type");
    resp->addHeader("Access-Control-Max-Age", "86400");  // 预检请求缓存 24 小时
}

// 构造统一格式的 JSON 响应
// 响应格式：{"code": <业务状态码>, "message": "<消息>", "data": {...}}
// @param code: 业务状态码，0 表示成功，其他值表示各种错误
// @param message: 错误或成功消息
// @param data_json: 业务数据的 JSON 字符串，默认为空对象
// @param http_code: HTTP 状态码，默认 200
void WriteJson(HttpResponse* resp, int code, const std::string& message,
               const std::string& data_json = "{}",
               HttpResponse::HttpStatusCode http_code = HttpResponse::k200Ok) {
    resp->setStatusCode(http_code);
    resp->setContentType("application/json; charset=utf-8");
    AddCORSHeaders(resp);
    
    // 手工拼接 JSON（避免引入 nlohmann::json 的序列化开销）
    std::ostringstream oss;
    oss << "{\"code\":" << code << ",\"message\":\"" << message << "\""
        << ",\"data\":" << data_json << "}";
    resp->setBody(oss.str());
}

// 解析登录请求的 JSON 请求体
// 期望格式：{"account": "...", "password": "..."}
// @return: 解析成功返回 true，失败返回 false
bool ParseJsonAccountPassword(const std::string& body, std::string* account,
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
        
        *account = j["account"].get<std::string>();
        *password = j["password"].get<std::string>();
        return true;
    } catch (...) {
        // JSON 解析异常，返回失败
        return false;
    }
}

// 解析注册请求的 JSON 请求体
// 期望格式：{"account": "...", "password": "...", "name": "..."}
// name 字段可选，未提供时默认使用 account 作为昵称
// @return: 解析成功返回 true，失败返回 false
bool ParseJsonRegister(const std::string& body, std::string* account,
                       std::string* password, std::string* name) {
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
        
        *account = j["account"].get<std::string>();
        *password = j["password"].get<std::string>();
        
        // 可选字段：昵称
        if (j.contains("name") && j["name"].is_string()) {
            *name = j["name"].get<std::string>();
        }
        
        // 如果未提供昵称，默认使用账号作为昵称
        if (name->empty()) {
            *name = *account;
        }
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

// ============================================================================
// HttpApiServer 类实现
// ============================================================================

// 构造函数：初始化 HTTP 服务器并设置请求回调
HttpApiServer::HttpApiServer(EventLoop* loop,
                             const InetAddress& listenAddr,
                             UserDao* user_dao,
                             RedisStore* redis_store)
    : user_dao_(user_dao),
      redis_store_(redis_store),
      server_(loop, listenAddr, "logic_http_server") {
    // 设置 HTTP 请求回调函数，所有请求都会路由到 onRequest
    server_.setHttpCallback([this](const TcpConnectionPtr&, HttpRequest& req,
                                   HttpResponse* resp) -> bool {
        this->onRequest(req, resp);
        return true;
    });
}

// 启动 HTTP 服务器
void HttpApiServer::start() { server_.start(); }

// HTTP 请求路由器：根据请求路径分发到对应的处理函数
void HttpApiServer::onRequest(const HttpRequest& req, HttpResponse* resp) {
    using Handler = void (HttpApiServer::*)(const HttpRequest&, HttpResponse*);
    
    // 处理 OPTIONS 预检请求（CORS 机制）
    // 浏览器在跨域 POST 前会先发送 OPTIONS 请求确认服务器是否允许跨域
    if (req.method() == HttpRequest::kOptions) {
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("text/plain; charset=utf-8");
        AddCORSHeaders(resp);
        return;
    }

    // 路由表：URL 路径 -> 处理函数
    // 使用静态变量避免每次请求都重建映射表
    static const std::unordered_map<std::string, Handler> kRouteTable = {
        {"/api/login", &HttpApiServer::handleLogin},
        {"/api/register", &HttpApiServer::handleRegister},
    };

    // 查找路径对应的处理函数
    const std::string& path = req.path();
    auto it = kRouteTable.find(path);
    if (it == kRouteTable.end()) {
        WriteJson(resp, 404, "unknown path");
        return;
    }
    
    // 调用对应的处理函数（成员函数指针调用）
    Handler handler = it->second;
    (this->*handler)(req, resp);
}

// 处理用户登录请求
// POST /api/login
// 请求体：{"account": "...", "password": "..."}
// 响应体：{"code": 0, "message": "ok", "data": {"user_id": ..., "token": "...", "name": "..."}}
void HttpApiServer::handleLogin(const HttpRequest& req, HttpResponse* resp) {
    std::string account;
    std::string password;
    
    // 1. 验证请求方法
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    
    // 2. 验证请求体非空
    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    
    // 3. 解析 JSON 请求体
    if (!ParseJsonAccountPassword(req.body(), &account, &password)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"account\":\"...\",\"password\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 4. 特殊处理：管理员账号（硬编码用于演示和运维）
    if (account == kAdminAccount) {
        // 验证密码
        if (password != kAdminPasswordHash) {
            WriteJson(resp, 401, "invalid password");
            return;
        }
        
        // 生成 token：格式为 tk-<user_id>-<timestamp>
        std::string token = "tk-" + std::to_string(kAdminUserId) + "-" +
                            std::to_string(::time(nullptr));
        
        // 将 token 存入 Redis，有效期 24 小时
        if (!redis_store_ ||
            !redis_store_->SetToken(token, kAdminUserId, 24 * 3600)) {
            WriteJson(resp, 500, "save token to redis failed");
            return;
        }
        
        // 返回成功响应
        std::ostringstream data;
        data << "{\"user_id\":" << kAdminUserId << ",\"token\":\"" << token
             << "\"}";
        WriteJson(resp, 0, "ok", data.str());
        return;
    }

    // 5. 验证账号非空
    if (account.empty()) {
        WriteJson(resp, 400, "account is required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    
    // 6. 检查数据库访问对象是否已初始化
    if (!user_dao_) {
        WriteJson(resp, 500, "user dao not initialized");
        return;
    }

    // 7. 从数据库查询用户信息
    User user;
    std::string err;
    if (!user_dao_->GetUserByAccount(account, &user, &err)) {
        WriteJson(resp, 404, "user not found, please register first", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    
    // 8. 验证密码（这里存储的是密码 hash，客户端需先做 hash 再传输）
    if (user.password_hash != password) {
        WriteJson(resp, 401, "invalid password");
        return;
    }

    // 9. 生成 token 并存入 Redis
    std::string token =
        "tk-" + std::to_string(user.id) + "-" + std::to_string(::time(nullptr));
    if (!redis_store_ || !redis_store_->SetToken(token, user.id, 24 * 3600)) {
        WriteJson(resp, 500, "save token to redis failed");
        return;
    }

    // 10. 返回登录成功响应，包含用户 ID、token 和昵称
    std::ostringstream data;
    data << "{\"user_id\":" << user.id << ",\"token\":\"" << token << "\""
         << ",\"name\":\"" << user.name << "\"}";
    WriteJson(resp, 0, "ok", data.str());
}

// 处理用户注册请求
// POST /api/register
// 请求体：{"account": "...", "password": "...", "name": "..."}
// 响应体：{"code": 0, "message": "ok", "data": {"user_id": ..., "token": "...", "name": "..."}}
void HttpApiServer::handleRegister(const HttpRequest& req,
                                   HttpResponse* resp) {
    std::string account;
    std::string password;
    std::string name;
    
    // 1. 验证请求方法
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    
    // 2. 验证请求体非空
    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    
    // 3. 解析 JSON 请求体
    if (!ParseJsonRegister(req.body(), &account, &password, &name)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"account\":\"...\",\"password\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    
    // 4. 验证必需字段非空
    if (account.empty() || password.empty()) {
        WriteJson(resp, 400, "account/password are required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    
    // 5. 防止占用管理员账号
    if (account == kAdminAccount) {
        WriteJson(resp, 409,
                  "admin account is reserved, please use login directly", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    
    // 6. 检查数据库访问对象是否已初始化
    if (!user_dao_) {
        WriteJson(resp, 500, "user dao not initialized");
        return;
    }

    // 7. 检查账号是否已存在
    User existing;
    std::string err;
    if (user_dao_->GetUserByAccount(account, &existing, &err)) {
        // 账号已存在，不能重复注册
        WriteJson(resp, 409, "account already exists", "{}",
                  HttpResponse::k400BadRequest);
        return;
    } else if (!err.empty() && err != "user not found") {
        // 数据库查询出错（非"用户不存在"错误）
        WriteJson(resp, 500, "query user failed: " + err);
        return;
    }

    // 8. 在数据库中创建新用户
    int64_t uid = 0;
    if (!user_dao_->CreateUser(account, name, password, &uid, &err)) {
        WriteJson(resp, 500, "register user failed: " + err);
        return;
    }

    // 9. 构造用户对象（用于返回）
    User user;
    user.id = uid;
    user.account = account;
    user.name = name;
    user.password_hash = password;

    // 10. 生成 token 并存入 Redis（注册成功后自动登录）
    std::string token =
        "tk-" + std::to_string(user.id) + "-" + std::to_string(::time(nullptr));
    if (!redis_store_ || !redis_store_->SetToken(token, user.id, 24 * 3600)) {
        WriteJson(resp, 500, "save token to redis failed");
        return;
    }

    // 11. 返回注册成功响应，包含用户 ID、token 和昵称
    std::ostringstream data;
    data << "{\"user_id\":" << user.id << ",\"token\":\"" << token << "\""
         << ",\"name\":\"" << user.name << "\"}";
    WriteJson(resp, 0, "ok", data.str());
}

}  // namespace sparkpush
