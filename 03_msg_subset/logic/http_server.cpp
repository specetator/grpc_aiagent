#include "http_server.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <sstream>
#include <unordered_map>

#include "logging.h"
#include "spark_push.grpc.pb.h"
#include "spark_push.pb.h"

namespace sparkpush {

using namespace muduo;
using namespace muduo::net;

namespace {

// 兼容性 unused 标记：用于“保留但当前未被引用”的本地辅助函数，避免
// -Wunused-function 警告。
#if defined(__GNUC__) || defined(__clang__)
#define SPARKPUSH_UNUSED __attribute__((unused))
#else
#define SPARKPUSH_UNUSED
#endif

// 简单的预设管理员账号配置（Demo 级别，仅用于本地/内网环境）
// 前端会对明文密码做 MD5，后端仅对 MD5 结果做字符串匹配。
// 这里约定：
//   账号：admin
//   密码明文：admin123
//   密码 MD5（小写 32 位）：0192023a7bbd73250516f069df18b500
// 注意：真实生产环境请务必使用更安全的方案（带盐哈希、多次迭代、HTTPS 等）。
const std::string kAdminAccount = "admin";
const std::string kAdminPasswordHash = "0192023a7bbd73250516f069df18b500";
// 管理员在当前 中使用一个固定 user_id，仅用于 token 与 owner_id 标识。
// 不依赖于数据库中是否真实存在该用户记录。
const int64_t kAdminUserId = 900000000000LL;

// 解析 x-www-form-urlencoded（旧实现，保留以兼容可能的其他调用）
SPARKPUSH_UNUSED std::unordered_map<std::string, std::string> ParseForm(
    const std::string& body) {
    std::unordered_map<std::string, std::string> result;
    std::stringstream ss(body);
    std::string item;
    while (std::getline(ss, item, '&')) {
        auto pos = item.find('=');
        if (pos == std::string::npos) continue;
        std::string key = item.substr(0, pos);
        std::string value = item.substr(pos + 1);
        result[key] = value;
    }
    return result;
}

// 通用 JSON 解析工具
bool ParseJsonBody(const std::string& body, nlohmann::json* out) {
    if (!out) return false;
    if (body.empty()) {
        return false;
    }
    try {
        *out = nlohmann::json::parse(body);
        return true;
    } catch (const nlohmann::json::exception& e) {
        LOG_ERROR << "JSON parse error: " << e.what();
        return false;
    } catch (...) {
        return false;
    }
}

// CORS 统一处理：当前 允许任意来源跨域访问 logic HTTP 接口
void AddCORSHeaders(HttpResponse* resp) {
    if (!resp) return;
    resp->addHeader("Access-Control-Allow-Origin", "*");
    resp->addHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    resp->addHeader("Access-Control-Allow-Headers", "Content-Type");
    resp->addHeader("Access-Control-Max-Age", "86400");
}

// 使用 nlohmann/json 解析 account/password
SPARKPUSH_UNUSED bool ParseJsonAccountPassword(const std::string& body,
                                               std::string* account,
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
    } catch (const nlohmann::json::exception& e) {
        LOG_ERROR << "JSON parse error: " << e.what();
        return false;
    } catch (...) {
        return false;
    }
}

// 解析注册请求（包含可选的 name）
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
        // 如果没传 name，默认用 account
        if (name->empty()) {
            *name = *account;
        }
        return true;
    } catch (...) {
        return false;
    }
}

void WriteJson(HttpResponse* resp, int code, const std::string& message,
               const std::string& data_json = "{}",
               HttpResponse::HttpStatusCode http_code = HttpResponse::k200Ok) {
    resp->setStatusCode(http_code);
    resp->setContentType("application/json; charset=utf-8");
    // 所有 JSON API 统一打上 CORS 头，方便在 9010/其他端口的前端页面直接调用
    AddCORSHeaders(resp);
    std::ostringstream oss;
    oss << "{\"code\":" << code << ",\"message\":\"" << message << "\""
        << ",\"data\":" << data_json << "}";
    resp->setBody(oss.str());
}

}  // namespace

// 构造 HTTP API 服务器，注入业务依赖并注册回调
HttpApiServer::HttpApiServer(EventLoop* loop, const InetAddress& listenAddr,
                             UserDao* user_dao, RedisStore* redis_store,
                             KafkaProducer* push_producer)
    : user_dao_(user_dao),
      redis_store_(redis_store),
      push_producer_(push_producer),
      server_(loop, listenAddr, "logic_http_server") {
    // 适配新的 HttpServer 回调签名：bool (const TcpConnectionPtr&,
    // HttpRequest&, HttpResponse*)
    server_.setHttpCallback([this](const TcpConnectionPtr&, HttpRequest& req,
                                   HttpResponse* resp) -> bool {
        this->onRequest(req, resp);
        return true;
    });
}

// 启动 HTTP 服务器
void HttpApiServer::start() { server_.start(); }

bool HttpApiServer::GetUserIdFromRequest(const HttpRequest& req, int64_t* uid) {
    if (!uid || !redis_store_) return false;
    std::string token;
    auto it = req.headers().find("Authorization");
    if (it != req.headers().end()) {
        const std::string& auth = it->second;
        const std::string prefix = "Bearer ";
        if (auth.size() > prefix.size() &&
            auth.compare(0, prefix.size(), prefix) == 0) {
            token = auth.substr(prefix.size());
        }
    }
    if (token.empty()) {
        auto th = req.headers().find("Token");
        if (th != req.headers().end()) {
            token = th->second;
        }
    }
    if (token.empty()) return false;
    return redis_store_->GetUserIdByToken(token, uid);
}

// 统一入口：处理 OPTIONS/CORS 并分发业务路由
void HttpApiServer::onRequest(const HttpRequest& req, HttpResponse* resp) {
    using Handler = void (HttpApiServer::*)(const HttpRequest&, HttpResponse*);

    // 处理浏览器的 CORS 预检请求（OPTIONS），无需走具体业务路由
    if (req.method() == HttpRequest::kOptions) {
        // 对于预检请求，只需要返回 200 + CORS 头即可
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("text/plain; charset=utf-8");
        AddCORSHeaders(resp);
        return;
    }

    // 路由表：从 path 映射到成员函数指针
    static const std::unordered_map<std::string, Handler> kRouteTable = {
        {"/api/login", &HttpApiServer::handleLogin},
        {"/api/register", &HttpApiServer::handleRegister},
        {"/api/message/send", &HttpApiServer::handleSendMessage},
        {"/api/session/list_single", &HttpApiServer::handleSessionList},
        {"/api/session/unread", &HttpApiServer::handleUnread},
        {"/api/session/mark_read", &HttpApiServer::handleMarkRead},
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

// 用户登录
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

// 用户注册：创建账号并发放 token
void HttpApiServer::handleRegister(const HttpRequest& req, HttpResponse* resp) {
    std::string account;
    std::string password;
    std::string name;

    // 商用接口：只允许 POST，参数通过 JSON body 传递
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

    // 预设管理员账号保留，禁止通过注册接口覆盖/创建同名账号。
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
        // 数据库真实错误
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

    // 注册成功后直接下发 token，让前端“注册即登录”
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

// 单聊发送消息（HTTP 入口）
void HttpApiServer::handleSendMessage(const HttpRequest& req,
                                      HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    int64_t from_user = 0;
    if (!GetUserIdFromRequest(req, &from_user)) {
        WriteJson(resp, 401, "unauthorized");
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(
            resp, 400,
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
    int64_t client_ts_ms = 0;
    try {
        if (!j.contains("target_type") || !j.contains("target_id") ||
            !j.contains("content")) {
            WriteJson(resp, 400, "target_type/target_id/content required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        target_type = j.at("target_type").get<std::string>();
        target_id = j.at("target_id").get<int64_t>();
        content = j.at("content").get<std::string>();
        if (j.contains("msg_type"))
            msg_type = j.at("msg_type").get<std::string>();
        if (j.contains("client_msg_id"))
            client_msg_id = j.at("client_msg_id").get<std::string>();
        if (j.contains("timestamp"))
            client_ts_ms = j.at("timestamp").get<int64_t>();
        if (j.contains("client_timestamp_ms"))
            client_ts_ms = j.at("client_timestamp_ms").get<int64_t>();
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "invalid json fields in send message", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    if (target_type != "single_chat" || target_id <= 0 || content.empty()) {
        WriteJson(resp, 400,
                  "only single_chat supported and target_id must be positive",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t small_uid = std::min(from_user, target_id);
    int64_t large_uid = std::max(from_user, target_id);
    std::string session_id =
        "s_" + std::to_string(small_uid) + ":" + std::to_string(large_uid);

    // 生成 msg_id/msg_seq
    std::string msg_id;
    int64_t msg_seq = 0;
    if (redis_store_ &&
        redis_store_->NextSingleMsgId(small_uid, large_uid, &msg_seq)) {
        msg_id = "msgid:" + std::to_string(small_uid) + ":" +
                 std::to_string(large_uid) + "-" + std::to_string(msg_seq);
    } else {
        WriteJson(resp, 500, "generate msg_id failed");
        return;
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 规范化 content_json：去重同义字段，补充服务端字段
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
    if (client_ts_ms > 0) norm["client_timestamp_ms"] = client_ts_ms;

    std::string final_json = norm.dump();

    // 构造 ChatMessage
    ChatMessage cm;
    cm.set_msg_id(msg_id);
    cm.set_session_id(session_id);
    cm.set_msg_seq(msg_seq);
    cm.set_sender_id(from_user);
    cm.set_timestamp_ms(now_ms);
    cm.set_msg_type(msg_type);
    cm.set_content_json(final_json);
    cm.set_client_msg_id(client_msg_id);

    // 更新 Redis 会话/未读/last_msg
    if (redis_store_) {
        redis_store_->AddUserSession(from_user, session_id);
        redis_store_->AddUserSession(target_id, session_id);
        redis_store_->SetSessionLastSeq(session_id, msg_seq);
        redis_store_->IncrUnreadCount(target_id, session_id, 1);
        redis_store_->SetUserSessionMeta(
            from_user, session_id, msg_id, msg_seq, msg_type, now_ms,
            content.size() > 50 ? content.substr(0, 50) : content);
        redis_store_->SetUserSessionMeta(
            target_id, session_id, msg_id, msg_seq, msg_type, now_ms,
            content.size() > 50 ? content.substr(0, 50) : content);
    }

    // 推 Kafka（在线推送）
    if (push_producer_) {
        std::vector<std::string> comets;
        if (redis_store_ &&
            redis_store_->GetUserConnectionComets(target_id, &comets) &&
            !comets.empty()) {
            for (const auto& cid : comets) {
                PushToCometRequest preq;
                preq.set_comet_id(cid);
                *preq.mutable_message() = cm;
                auto* t = preq.add_targets();
                t->set_user_id(target_id);
                std::string payload;
                if (preq.SerializeToString(&payload)) {
                    push_producer_->Send(cid, payload);
                }
            }
        } else {
            // 离线占位
            PushToCometRequest preq;
            preq.set_comet_id("");
            *preq.mutable_message() = cm;
            // 仍然填充 targets，便于下游（job）在 comet_id 缺失时进行广播兜底，
            // 由各 comet 根据本机连接集合决定是否真正下发。
            auto* t = preq.add_targets();
            t->set_user_id(target_id);
            std::string payload;
            if (preq.SerializeToString(&payload)) {
                push_producer_->Send("", payload);
            }
        }
    }

    std::ostringstream data;
    data << "{"
         << "\"msg_id\":\"" << msg_id << "\","
         << "\"msg_seq\":" << msg_seq << ","
         << "\"session_id\":\"" << session_id << "\""
         << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// 查询用户单聊会话列表
void HttpApiServer::handleSessionList(const HttpRequest& req,
                                      HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    if (!GetUserIdFromRequest(req, &user_id)) {
        WriteJson(resp, 401, "unauthorized");
        return;
    }

    if (!redis_store_) {
        WriteJson(resp, 500, "redis store not initialized");
        return;
    }

    std::vector<std::string> session_ids;
    if (!redis_store_->ListUserSessions(user_id, &session_ids)) {
        WriteJson(resp, 500, "list sessions failed");
        return;
    }

    std::ostringstream data;
    data << "{\"sessions\":[";
    bool first = true;
    for (const auto& sid : session_ids) {
        // 解析 peer_id
        int64_t user1 = 0, user2 = 0;
        if (sid.rfind("s_", 0) == 0) {
            auto pos = sid.find(':');
            if (pos != std::string::npos) {
                try {
                    user1 = std::stoll(sid.substr(2, pos - 2));
                    user2 = std::stoll(sid.substr(pos + 1));
                } catch (...) {
                }
            }
        }
        int64_t peer =
            (user1 == user_id) ? user2 : (user2 == user_id ? user1 : 0);
        int64_t last_seq = 0;
        redis_store_->GetSessionLastSeq(sid, &last_seq);

        std::string last_msg_id, last_msg_type, last_preview;
        int64_t last_msg_seq = 0;
        int64_t last_time_ms = 0;
        redis_store_->GetUserSessionMeta(user_id, sid, &last_msg_id,
                                         &last_msg_seq, &last_msg_type,
                                         &last_time_ms, &last_preview);

        if (!first) data << ",";
        first = false;
        data << "{"
             << "\"session_id\":\"" << sid << "\","
             << "\"peer_user_id\":" << peer << ","
             << "\"last_msg_seq\":" << last_seq << ","
             << "\"last_msg_id\":\"" << last_msg_id << "\","
             << "\"last_msg_type\":\"" << last_msg_type << "\","
             << "\"last_time_ms\":" << last_time_ms << ","
             << "\"last_preview\":\"" << last_preview << "\"}";
    }
    data << "]}";
    WriteJson(resp, 0, "ok", data.str());
}

// 查询会话未读消息数
void HttpApiServer::handleUnread(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    if (!GetUserIdFromRequest(req, &user_id)) {
        WriteJson(resp, 401, "unauthorized");
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"session_id\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    std::string session_id;
    try {
        if (!j.contains("session_id")) {
            WriteJson(resp, 400, "session_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        session_id = j.at("session_id").get<std::string>();
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "invalid json fields", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    if (user_id <= 0 || session_id.empty()) {
        WriteJson(resp, 400, "user_id/session_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    if (!redis_store_) {
        WriteJson(resp, 500, "redis store not initialized");
        return;
    }

    int64_t unread = 0;
    if (!redis_store_->GetUnreadCount(user_id, session_id, &unread)) {
        WriteJson(resp, 500, "get unread failed");
        return;
    }

    std::ostringstream data;
    data << "{\"session_id\":\"" << session_id << "\","
         << "\"unread_count\":" << unread << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// 标记会话已读：清零 Redis 未读计数，并可选记录 read_seq
void HttpApiServer::handleMarkRead(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    int64_t user_id = 0;
    if (!GetUserIdFromRequest(req, &user_id)) {
        WriteJson(resp, 401, "unauthorized");
        return;
    }
    if (!redis_store_) {
        WriteJson(resp, 500, "redis store not initialized");
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"session_id\":\"...\",\"read_seq\":0}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    std::string session_id;
    int64_t read_seq = 0;
    try {
        if (!j.contains("session_id")) {
            WriteJson(resp, 400, "session_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        session_id = j.at("session_id").get<std::string>();
        if (j.contains("read_seq")) {
            read_seq = j.at("read_seq").get<int64_t>();
        }
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "invalid json fields", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (user_id <= 0 || session_id.empty()) {
        WriteJson(resp, 400, "user_id/session_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    if (read_seq > 0) {
        // 记录已读位置（可选）
        redis_store_->SetUserReadSeq(user_id, session_id, read_seq);
    }
    if (!redis_store_->ClearUnreadCount(user_id, session_id)) {
        WriteJson(resp, 500, "clear unread failed");
        return;
    }

    std::ostringstream data;
    data << "{\"session_id\":\"" << session_id << "\","
         << "\"cleared\":true}";
    WriteJson(resp, 0, "ok", data.str());
}

}  // namespace sparkpush
