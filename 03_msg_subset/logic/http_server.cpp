#include "http_server.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <sstream>
#include <unordered_map>

#include "logging.h"
#include "spark_push.pb.h"

namespace sparkpush {

using namespace muduo;
using namespace muduo::net;

namespace {

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
std::unordered_map<std::string, std::string> ParseForm(
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
                             ConversationStore* store)
    : user_dao_(user_dao),
      redis_store_(redis_store),
      store_(store),
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
 

// 用户登录：当前精简版本仅提示未实现，保留路由占位
void HttpApiServer::handleLogin(const HttpRequest& /*req*/,
                               HttpResponse* resp) {
    WriteJson(resp, 501, "login not implemented");
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

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(
            resp, 400,
            "invalid json body, expect "
            "{\"from_user_id\":...,\"to_user_id\":...,\"content\":\"...\"}",
            "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t from_user = 0;
    int64_t to_user = 0;
    std::string content;
    try {
        if (!j.contains("from_user_id") || !j.contains("to_user_id") ||
            !j.contains("content")) {
            WriteJson(resp, 400, "from_user_id/to_user_id/content required",
                      "{}", HttpResponse::k400BadRequest);
            return;
        }
        from_user = j.at("from_user_id").get<int64_t>();
        to_user = j.at("to_user_id").get<int64_t>();
        content = j.at("content").get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in message send", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    if (from_user <= 0 || to_user <= 0 || content.empty()) {
        WriteJson(resp, 400, "from_user_id/to_user_id/content required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    // 客户端自定义的 msg_seq（可选）
    int64_t client_msg_seq = 0;
    bool has_client_msg_seq = false;
    if (j.contains("msg_seq")) {
        try {
            client_msg_seq = j.at("msg_seq").get<int64_t>();
            has_client_msg_seq = true;
        } catch (const nlohmann::json::exception&) {
        }
    }

    // 构造单聊会话 id（s_<small>_<large>）
    int64_t small_uid = std::min(from_user, to_user);
    int64_t large_uid = std::max(from_user, to_user);
    Session session;
    session.type = SessionType::kSingle;
    session.user1_id = small_uid;
    session.user2_id = large_uid;
    session.id = "s_" + std::to_string(small_uid) + "_" +
                 std::to_string(large_uid);

    // 服务端生成 msg_id
    std::string msg_id;
    int64_t msgid_seq = 0;
    if (redis_store_ &&
        redis_store_->NextSingleMsgId(small_uid, large_uid, &msgid_seq)) {
        msg_id = "msgid:" + std::to_string(small_uid) + ":" +
                 std::to_string(large_uid) + "-" + std::to_string(msgid_seq);
    } else {
        WriteJson(resp, 500, "generate msg_id failed");
        return;
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    // 组装返回的消息（此处不落持久化，仅按规则回包）
    Message msg;
    msg.session_id = session.id;
    msg.msg_id = msg_id;
    msg.msg_seq = has_client_msg_seq ? client_msg_seq : 0;
    msg.sender_id = from_user;
    msg.msg_type = "text";
    msg.timestamp_ms = now_ms;
    msg.content_json = content;
    msg.client_msg_id = "";  // HTTP 暂不透传

    std::ostringstream data;
    data << "{\"session_id\":\"" << msg.session_id << "\",\"msg_id\":\""
         << msg.msg_id << "\",\"msg_seq\":" << msg.msg_seq << "}";
    WriteJson(resp, 0, "ok", data.str());
}

#if 0  // legacy chatroom/history APIs (disabled)
// 拉取会话历史消息
void HttpApiServer::handleHistory(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"session_id\":\"...\",\"anchor_seq\":0,\"limit\":20}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    std::string session_id;
    int64_t anchor_seq = 0;
    int limit = 20;
    try {
        if (!j.contains("session_id")) {
            WriteJson(resp, 400, "session_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        session_id = j.at("session_id").get<std::string>();
        if (j.contains("anchor_seq")) {
            anchor_seq = j.at("anchor_seq").get<int64_t>();
        }
        if (j.contains("limit")) {
            limit = j.at("limit").get<int>();
        }
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in history", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (session_id.empty()) {
        WriteJson(resp, 400, "session_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!store_) {
        WriteJson(resp, 500, "conversation store not initialized");
        return;
    }
    std::vector<Message> msgs;
    std::string err;
    if (!store_->GetHistory(session_id, anchor_seq, limit, &msgs, &err)) {
        WriteJson(resp, 500, "query history failed: " + err);
        return;
    }
    std::ostringstream data;
    data << "{\"messages\":[";
    bool first = true;
    for (const auto& m : msgs) {
        if (!first) data << ",";
        first = false;
        data << "{" << "\"msg_id\":\"" << m.msg_id << "\","
             << "\"client_msg_id\":\"" << m.client_msg_id << "\","
             << "\"msg_seq\":" << m.msg_seq << ","
             << "\"sender_id\":" << m.sender_id << ","
             << "\"timestamp_ms\":" << m.timestamp_ms << ","
             << "\"msg_type\":\"" << m.msg_type << "\"," << "\"content\":"
             << (m.content_json.empty() ? "\"\"" : m.content_json) << "}";
    }
    data << "]}";
    WriteJson(resp, 0, "ok", data.str());
}

// 标记会话已读
void HttpApiServer::handleMarkRead(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"user_id\":...,\"session_id\":\"...\",\"read_seq\":...}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    std::string session_id;
    int64_t read_seq = 0;
    try {
        if (!j.contains("user_id") || !j.contains("session_id") ||
            !j.contains("read_seq")) {
            WriteJson(resp, 400, "user_id/session_id/read_seq required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        user_id = j.at("user_id").get<int64_t>();
        session_id = j.at("session_id").get<std::string>();
        read_seq = j.at("read_seq").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in mark_read", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (user_id <= 0 || session_id.empty() || read_seq <= 0) {
        WriteJson(resp, 400, "user_id/session_id/read_seq required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!store_) {
        WriteJson(resp, 500, "conversation store not initialized");
        return;
    }
    std::string err;
    if (!store_->MarkRead(user_id, session_id, read_seq, &err)) {
        WriteJson(resp, 500, "mark read failed: " + err);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// 查询未读数
void HttpApiServer::handleUnread(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"user_id\":...,\"session_id\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    std::string session_id;
    try {
        if (!j.contains("user_id") || !j.contains("session_id")) {
            WriteJson(resp, 400, "user_id/session_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        user_id = j.at("user_id").get<int64_t>();
        session_id = j.at("session_id").get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in unread", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (user_id <= 0 || session_id.empty()) {
        WriteJson(resp, 400, "user_id/session_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!store_) {
        WriteJson(resp, 500, "conversation store not initialized");
        return;
    }
    int64_t unread = 0;
    std::string err;
    if (!store_->GetUnread(user_id, session_id, &unread, &err)) {
        WriteJson(resp, 500, "get unread failed: " + err);
        return;
    }
    std::ostringstream data;
    data << "{\"unread\":" << unread << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// 列出单聊会话列表
void HttpApiServer::handleSingleSessionList(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body, expect {\"user_id\":...}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    try {
        if (!j.contains("user_id")) {
            WriteJson(resp, 400, "user_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        user_id = j.at("user_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in list_single", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (user_id <= 0) {
        WriteJson(resp, 400, "user_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!store_) {
        WriteJson(resp, 500, "conversation store not initialized");
        return;
    }
    std::vector<Session> sessions;
    std::string err;
    if (!store_->ListUserSingleSessions(user_id, &sessions, &err)) {
        WriteJson(resp, 500, "list sessions failed: " + err);
        return;
    }
    std::ostringstream data;
    data << "{\"sessions\":[";
    bool first = true;
    for (const auto& s : sessions) {
        int64_t peer_id = (s.user1_id == user_id) ? s.user2_id : s.user1_id;
        if (peer_id <= 0) continue;
        if (!first) data << ",";
        first = false;
        data << "{" << "\"session_id\":\"" << s.id << "\","
             << "\"peer_user_id\":" << peer_id << ","
             << "\"last_msg_seq\":" << s.last_msg_seq << "}";
    }
    data << "]}";
    WriteJson(resp, 0, "ok", data.str());
}

// 列出用户订阅的聊天室
void HttpApiServer::handleChatroomList(const HttpRequest& req,
                                       HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body, expect {\"user_id\":...}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    try {
        if (!j.contains("user_id")) {
            WriteJson(resp, 400, "user_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        user_id = j.at("user_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in chatroom list", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (user_id <= 0) {
        WriteJson(resp, 400, "user_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!group_member_dao_) {
        WriteJson(resp, 500, "group member dao not initialized");
        return;
    }
    if (!group_dao_) {
        WriteJson(resp, 500, "group dao not initialized");
        return;
    }
    std::vector<int64_t> room_ids;
    std::string err;
    if (!group_member_dao_->ListUserChatrooms(user_id, &room_ids, &err)) {
        WriteJson(resp, 500, "list user chatrooms failed: " + err);
        return;
    }
    if (!store_) {
        WriteJson(resp, 500, "conversation store not initialized");
        return;
    }
    std::ostringstream data;
    data << "{\"rooms\":[";
    bool first = true;
    for (int64_t room_id : room_ids) {
        Session s;
        if (!store_->GetOrCreateRoomSession(room_id, &s, &err)) {
            LOG_ERROR << "GetOrCreateRoomSession failed: " << err;
            continue;
        }

        ImGroup g;
        std::string g_err;
        std::string name = "Unknown";
        if (group_dao_->GetGroup(room_id, &g, &g_err)) {
            name = g.name;
        }

        if (!first) data << ",";
        first = false;
        data << "{" << "\"session_id\":\"" << s.id << "\","
             << "\"room_id\":" << s.group_id << "," << "\"name\":\"" << name
             << "\"," << "\"last_msg_seq\":" << s.last_msg_seq << "}";
    }
    data << "]}";
    WriteJson(resp, 0, "ok", data.str());
}

// 加入聊天室（订阅关系）
void HttpApiServer::handleChatroomJoin(const HttpRequest& req,
                                       HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"room_id\":...,\"user_id\":...}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t room_id = 0;
    int64_t user_id = 0;
    try {
        if (!j.contains("room_id") || !j.contains("user_id")) {
            WriteJson(resp, 400, "room_id/user_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        room_id = j.at("room_id").get<int64_t>();
        user_id = j.at("user_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in chatroom join", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (room_id <= 0 || user_id <= 0) {
        WriteJson(resp, 400, "room_id/user_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!group_dao_ || !group_member_dao_) {
        WriteJson(resp, 500, "group dao not initialized");
        return;
    }
    // 确认聊天室存在且类型正确
    ImGroup g;
    std::string err;
    if (!group_dao_->GetGroup(room_id, &g, &err)) {
        WriteJson(resp, 404, "chatroom not found: " + err);
        return;
    }
    if (g.group_type != 1) {
        WriteJson(resp, 400, "group is not chatroom", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!group_member_dao_->AddOrUpdateMember(room_id, user_id, 0, &err)) {
        WriteJson(resp, 500, "add member failed: " + err);
        return;
    }

    WriteJson(resp, 0, "ok");
}

// 离开聊天室（在线层）
void HttpApiServer::handleChatroomLeave(const HttpRequest& req,
                                        HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"room_id\":...,\"user_id\":...}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t room_id = 0;
    int64_t user_id = 0;
    try {
        if (!j.contains("room_id") || !j.contains("user_id")) {
            WriteJson(resp, 400, "room_id/user_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        room_id = j.at("room_id").get<int64_t>();
        user_id = j.at("user_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in chatroom leave", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (room_id <= 0 || user_id <= 0) {
        WriteJson(resp, 400, "room_id/user_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    // 这里的 leave 仅表示“当前不再接收该房间的实时消息”，
    // 实际的在线状态由 WebSocket + comet/Redis 驱动。
    // 订阅关系交由 /api/chatroom/unsubscribe
    // 维护，这里不修改内存模型中的订阅集合。
    WriteJson(resp, 0, "ok");
}

// 取消订阅聊天室（从成员表移除）
void HttpApiServer::handleChatroomUnsubscribe(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"room_id\":...,\"user_id\":...}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t room_id = 0;
    int64_t user_id = 0;
    try {
        if (!j.contains("room_id") || !j.contains("user_id")) {
            WriteJson(resp, 400, "room_id/user_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        room_id = j.at("room_id").get<int64_t>();
        user_id = j.at("user_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in chatroom unsubscribe",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (room_id <= 0 || user_id <= 0) {
        WriteJson(resp, 400, "room_id/user_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!group_member_dao_) {
        WriteJson(resp, 500, "group member dao not initialized");
        return;
    }
    std::string err;
    if (!group_member_dao_->RemoveMember(room_id, user_id, &err)) {
        WriteJson(resp, 500, "remove member failed: " + err);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// 管理员创建聊天室
void HttpApiServer::handleAdminCreateChatroom(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(
            resp, 400,
            "invalid json body, expect {\"name\":\"...\",\"owner_id\":...}",
            "{}", HttpResponse::k400BadRequest);
        return;
    }

    std::string name;
    int64_t owner_id = 0;
    try {
        if (!j.contains("name") || !j.contains("owner_id")) {
            WriteJson(resp, 400, "name/owner_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        name = j.at("name").get<std::string>();
        owner_id = j.at("owner_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in admin create chatroom",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (name.empty() || owner_id <= 0) {
        WriteJson(resp, 400, "name/owner_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!group_dao_) {
        WriteJson(resp, 500, "group dao not initialized");
        return;
    }
    int64_t group_id = 0;
    std::string err;
    if (!group_dao_->CreateChatroom(name, owner_id, &group_id, &err)) {
        WriteJson(resp, 500, "create chatroom failed: " + err);
        return;
    }
    std::ostringstream data;
    data << "{\"room_id\":" << group_id << ",\"name\":\"" << name << "\"}";
    WriteJson(resp, 0, "ok", data.str());
}

// 管理员列出聊天室
void HttpApiServer::handleAdminListChatroom(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"offset\":0,\"limit\":20}", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    int offset = 0;
    int limit = 20;
    try {
        if (j.contains("offset")) {
            offset = j.at("offset").get<int>();
        }
        if (j.contains("limit")) {
            limit = j.at("limit").get<int>();
        }
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in admin list chatroom", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!group_dao_) {
        WriteJson(resp, 500, "group dao not initialized");
        return;
    }
    std::vector<ImGroup> groups;
    std::string err;
    if (!group_dao_->ListChatrooms(offset, limit, &groups, &err)) {
        WriteJson(resp, 500, "list chatrooms failed: " + err);
        return;
    }
    std::ostringstream data;
    data << "{\"rooms\":[";
    bool first = true;
    for (const auto& g : groups) {
        if (!first) data << ",";
        first = false;
        data << "{" << "\"room_id\":" << g.id << "," << "\"name\":\"" << g.name
             << "\"," << "\"owner_id\":" << g.owner_id << "}";
    }
    data << "]}";
    WriteJson(resp, 0, "ok", data.str());
}

// 管理员广播
void HttpApiServer::handleAdminBroadcast(const HttpRequest& req,
                                         HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"scope\":\"all|chatroom\",\"room_id\":0,\"text\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    std::string scope = "all";
    int64_t room_id = 0;
    std::string text;
    try {
        if (j.contains("scope")) {
            scope = j.at("scope").get<std::string>();
        }
        if (j.contains("room_id")) {
            room_id = j.at("room_id").get<int64_t>();
        }
        if (!j.contains("text")) {
            WriteJson(resp, 400, "text is required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        text = j.at("text").get<std::string>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in admin broadcast", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (text.empty()) {
        WriteJson(resp, 400, "text is required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (scope != "all" && scope != "chatroom") {
        WriteJson(resp, 400, "scope must be all or chatroom", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (scope == "chatroom" && room_id <= 0) {
        WriteJson(resp, 400, "room_id required when scope=chatroom", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    if (!broadcast_producer_) {
        WriteJson(resp, 500, "broadcast producer not initialized");
        return;
    }

    // 构造广播任务内容，与 LogicServiceImpl::Broadcast 使用的
    // BroadcastTaskRequest 保持一致
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    std::string task_id =
        "bcast-" + std::to_string(now_ms) + "-" + std::to_string(rand());

    std::string real_scope = (scope == "all") ? "all" : "group";
    int64_t group_id = (scope == "chatroom") ? room_id : 0;

    // 构造 JSON 内容：一个简单的系统广播文本
    std::ostringstream content_oss;
    content_oss << "{\"type\":\"system_broadcast\"" << ",\"scope\":\""
                << real_scope << "\"";
    if (group_id > 0) {
        content_oss << ",\"group_id\":" << group_id;
    }
    content_oss << ",\"content\":{\"text\":\"" << text << "\"}}";
    std::string content_json = content_oss.str();

    BroadcastTaskRequest task;
    task.set_task_id(task_id);
    task.set_scope(real_scope);
    task.set_group_id(group_id);
    task.set_content_json(content_json);

    std::string payload;
    if (!task.SerializeToString(&payload)) {
        WriteJson(resp, 500, "serialize BroadcastTaskRequest failed");
        return;
    }
    LOG_INFO << "Admin broadcast task created: task_id=" << task_id
             << ", scope=" << real_scope << ", group_id=" << group_id;
    if (!broadcast_producer_->Send(task_id, payload)) {
        WriteJson(resp, 500, "send broadcast task to kafka failed");
        return;
    }

    std::ostringstream data;
    data << "{\"task_id\":\"" << task_id << "\"" << ",\"scope\":\""
         << real_scope << "\"" << ",\"group_id\":" << group_id << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// 查询聊天室在线人数
void HttpApiServer::handleChatroomOnlineCount(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body, expect {\"room_id\":...}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t room_id = 0;
    try {
        if (!j.contains("room_id")) {
            WriteJson(resp, 400, "room_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        room_id = j.at("room_id").get<int64_t>();
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in chatroom online_count",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (room_id <= 0) {
        WriteJson(resp, 400, "room_id required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    int64_t count = 0;
    bool ok = false;
    if (redis_store_) {
        ok = redis_store_->GetRoomOnlineCount(room_id, &count);
    }
    if (!ok && group_member_dao_) {
        std::vector<int64_t> members;
        std::string err;
        if (group_member_dao_->ListRoomMembers(room_id, &members, &err)) {
            count = static_cast<int64_t>(members.size());
            ok = true;
        } else {
            LOG_ERROR << "ListRoomMembers failed when fallback online count: "
                      << err;
        }
    }
    std::ostringstream data;
    data << "{\"room_id\":" << room_id << ",\"online_count\":" << count << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// end of disabled handlers
#endif

// 查询用户单聊会话列表
void HttpApiServer::handleSessionList(const HttpRequest& req,
                                       HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"user_id\":...}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    try {
        if (!j.contains("user_id")) {
            WriteJson(resp, 400, "user_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        user_id = j.at("user_id").get<int64_t>();
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "invalid json fields", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    if (user_id <= 0) {
        WriteJson(resp, 400, "user_id must be positive", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    if (!store_) {
        WriteJson(resp, 500, "conversation store not initialized");
        return;
    }

    std::vector<Session> sessions;
    std::string err;
    if (!store_->ListUserSingleSessions(user_id, &sessions, &err)) {
        WriteJson(resp, 500, "list sessions failed: " + err);
        return;
    }

    std::ostringstream data;
    data << "{\"sessions\":[";
    bool first = true;
    for (const auto& s : sessions) {
        if (!first) data << ",";
        first = false;
        data << "{" << "\"session_id\":\"" << s.id << "\","
             << "\"user1_id\":" << s.user1_id << ","
             << "\"user2_id\":" << s.user2_id << ","
             << "\"last_msg_seq\":" << s.last_msg_seq << "}";
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

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"user_id\":...,\"session_id\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    std::string session_id;
    try {
        if (!j.contains("user_id") || !j.contains("session_id")) {
            WriteJson(resp, 400, "user_id/session_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        user_id = j.at("user_id").get<int64_t>();
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

    if (!store_) {
        WriteJson(resp, 500, "conversation store not initialized");
        return;
    }

    int64_t unread = 0;
    std::string err;
    if (!store_->GetUnread(user_id, session_id, &unread, &err)) {
        WriteJson(resp, 500, "get unread failed: " + err);
        return;
    }

    std::ostringstream data;
    data << "{\"session_id\":\"" << session_id << "\","
         << "\"unread_count\":" << unread << "}";
    WriteJson(resp, 0, "ok", data.str());
}

}  // namespace sparkpush
