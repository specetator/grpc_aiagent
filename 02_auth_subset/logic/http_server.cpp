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

// 预设管理员账号（演示用途）
const std::string kAdminAccount = "admin";
const std::string kAdminPasswordHash = "0192023a7bbd73250516f069df18b500";
const int64_t kAdminUserId = 900000000000LL;

// CORS 统一处理
void AddCORSHeaders(HttpResponse* resp) {
    if (!resp) return;
    resp->addHeader("Access-Control-Allow-Origin", "*");
    resp->addHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    resp->addHeader("Access-Control-Allow-Headers", "Content-Type");
    resp->addHeader("Access-Control-Max-Age", "86400");
}

void WriteJson(HttpResponse* resp, int code, const std::string& message,
               const std::string& data_json = "{}",
               HttpResponse::HttpStatusCode http_code = HttpResponse::k200Ok) {
    resp->setStatusCode(http_code);
    resp->setContentType("application/json; charset=utf-8");
    AddCORSHeaders(resp);
    std::ostringstream oss;
    oss << "{\"code\":" << code << ",\"message\":\"" << message << "\""
        << ",\"data\":" << data_json << "}";
    resp->setBody(oss.str());
}

bool ParseJsonAccountPassword(const std::string& body, std::string* account,
                              std::string* password) {
    if (!account || !password) return false;
    *account = "";
    *password = "";
    try {
        auto j = nlohmann::json::parse(body);
        if (!j.contains("account") || !j.contains("password")) {
            return false;
        }
        if (!j["account"].is_string() || !j["password"].is_string()) {
            return false;
        }
        *account = j["account"].get<std::string>();
        *password = j["password"].get<std::string>();
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseJsonRegister(const std::string& body, std::string* account,
                       std::string* password, std::string* name) {
    if (!account || !password || !name) return false;
    *account = "";
    *password = "";
    *name = "";
    try {
        auto j = nlohmann::json::parse(body);
        if (!j.contains("account") || !j.contains("password")) {
            return false;
        }
        if (!j["account"].is_string() || !j["password"].is_string()) {
            return false;
        }
        *account = j["account"].get<std::string>();
        *password = j["password"].get<std::string>();
        if (j.contains("name") && j["name"].is_string()) {
            *name = j["name"].get<std::string>();
        }
        if (name->empty()) {
            *name = *account;
        }
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

HttpApiServer::HttpApiServer(EventLoop* loop,
                             const InetAddress& listenAddr,
                             UserDao* user_dao,
                             RedisStore* redis_store)
    : user_dao_(user_dao),
      redis_store_(redis_store),
      server_(loop, listenAddr, "logic_http_server") {
    server_.setHttpCallback([this](const TcpConnectionPtr&, HttpRequest& req,
                                   HttpResponse* resp) -> bool {
        this->onRequest(req, resp);
        return true;
    });
}

void HttpApiServer::start() { server_.start(); }

void HttpApiServer::onRequest(const HttpRequest& req, HttpResponse* resp) {
    using Handler = void (HttpApiServer::*)(const HttpRequest&, HttpResponse*);
    if (req.method() == HttpRequest::kOptions) {
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("text/plain; charset=utf-8");
        AddCORSHeaders(resp);
        return;
    }

    static const std::unordered_map<std::string, Handler> kRouteTable = {
        {"/api/login", &HttpApiServer::handleLogin},
        {"/api/register", &HttpApiServer::handleRegister},
    };

    const std::string& path = req.path();
    auto it = kRouteTable.find(path);
    if (it == kRouteTable.end()) {
        WriteJson(resp, 404, "unknown path");
        return;
    }
    Handler handler = it->second;
    (this->*handler)(req, resp);
}

void HttpApiServer::handleLogin(const HttpRequest& req, HttpResponse* resp) {
    std::string account;
    std::string password;
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!ParseJsonAccountPassword(req.body(), &account, &password)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"account\":\"...\",\"password\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    // 管理员内置账号
    if (account == kAdminAccount) {
        if (password != kAdminPasswordHash) {
            WriteJson(resp, 401, "invalid password");
            return;
        }
        std::string token = "tk-" + std::to_string(kAdminUserId) + "-" +
                            std::to_string(::time(nullptr));
        if (!redis_store_ ||
            !redis_store_->SetToken(token, kAdminUserId, 24 * 3600)) {
            WriteJson(resp, 500, "save token to redis failed");
            return;
        }
        std::ostringstream data;
        data << "{\"user_id\":" << kAdminUserId << ",\"token\":\"" << token
             << "\"}";
        WriteJson(resp, 0, "ok", data.str());
        return;
    }

    if (account.empty()) {
        WriteJson(resp, 400, "account is required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!user_dao_) {
        WriteJson(resp, 500, "user dao not initialized");
        return;
    }

    User user;
    std::string err;
    if (!user_dao_->GetUserByAccount(account, &user, &err)) {
        WriteJson(resp, 404, "user not found, please register first", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (user.password_hash != password) {
        WriteJson(resp, 401, "invalid password");
        return;
    }

    std::string token =
        "tk-" + std::to_string(user.id) + "-" + std::to_string(::time(nullptr));
    if (!redis_store_ || !redis_store_->SetToken(token, user.id, 24 * 3600)) {
        WriteJson(resp, 500, "save token to redis failed");
        return;
    }

    std::ostringstream data;
    data << "{\"user_id\":" << user.id << ",\"token\":\"" << token << "\""
         << ",\"name\":\"" << user.name << "\"}";
    WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleRegister(const HttpRequest& req,
                                   HttpResponse* resp) {
    std::string account;
    std::string password;
    std::string name;
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (req.body().empty()) {
        WriteJson(resp, 400, "empty body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!ParseJsonRegister(req.body(), &account, &password, &name)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"account\":\"...\",\"password\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (account.empty() || password.empty()) {
        WriteJson(resp, 400, "account/password are required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (account == kAdminAccount) {
        WriteJson(resp, 409,
                  "admin account is reserved, please use login directly", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!user_dao_) {
        WriteJson(resp, 500, "user dao not initialized");
        return;
    }

    User existing;
    std::string err;
    if (user_dao_->GetUserByAccount(account, &existing, &err)) {
        WriteJson(resp, 409, "account already exists", "{}",
                  HttpResponse::k400BadRequest);
        return;
    } else if (!err.empty() && err != "user not found") {
        WriteJson(resp, 500, "query user failed: " + err);
        return;
    }

    int64_t uid = 0;
    if (!user_dao_->CreateUser(account, name, password, &uid, &err)) {
        WriteJson(resp, 500, "register user failed: " + err);
        return;
    }

    User user;
    user.id = uid;
    user.account = account;
    user.name = name;
    user.password_hash = password;

    std::string token =
        "tk-" + std::to_string(user.id) + "-" + std::to_string(::time(nullptr));
    if (!redis_store_ || !redis_store_->SetToken(token, user.id, 24 * 3600)) {
        WriteJson(resp, 500, "save token to redis failed");
        return;
    }

    std::ostringstream data;
    data << "{\"user_id\":" << user.id << ",\"token\":\"" << token << "\""
         << ",\"name\":\"" << user.name << "\"}";
    WriteJson(resp, 0, "ok", data.str());
}

}  // namespace sparkpush
