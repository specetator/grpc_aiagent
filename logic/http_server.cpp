#include "http_server.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "image_attachment.h"
#include "logging.h"
#include "metrics.h"

namespace sparkpush {

using namespace muduo;
using namespace muduo::net;

namespace {

// 解析 x-www-form-urlencoded（旧实现，保留以兼容可能的其他调用）
std::unordered_map<std::string, std::string> ParseForm(const std::string& body) {
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
    resp->addHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
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

bool ParseUserStatus(const nlohmann::json& value, int* status) {
    if (!status) return false;
    if (value.is_number_integer()) {
        *status = value.get<int>();
        return *status >= kUserStatusActive && *status <= kUserStatusDeleted;
    }
    if (!value.is_string()) return false;
    const std::string name = value.get<std::string>();
    if (name == "active") {
        *status = kUserStatusActive;
    } else if (name == "disabled") {
        *status = kUserStatusDisabled;
    } else if (name == "deleted") {
        *status = kUserStatusDeleted;
    } else {
        return false;
    }
    return true;
}

void WriteJson(HttpResponse* resp, int code, const std::string& message,
               const std::string& data_json = "{}",
               HttpResponse::HttpStatusCode http_code = HttpResponse::k200Ok) {
    resp->setStatusCode(http_code);
    resp->setContentType("application/json; charset=utf-8");
    // 所有 JSON API 统一打上 CORS 头，方便在 9010/其他端口的前端页面直接调用
    AddCORSHeaders(resp);
    nlohmann::json data = nlohmann::json::object();
    try {
        data = nlohmann::json::parse(data_json);
    } catch (...) {
        LOG_ERROR << "WriteJson received invalid data JSON";
    }
    nlohmann::json body;
    body["code"] = code;
    body["message"] = message;
    body["data"] = std::move(data);
    resp->setBody(body.dump());
}

// HTTP mapping remains at the transport boundary; preserve legacy success-
// status responses for storage/unavailability failures.
void WriteServiceError(HttpResponse* resp, const ServiceStatus& status) {
    auto http = HttpResponse::k200Ok;
    if (status.code == 400)
        http = HttpResponse::k400BadRequest;
    else if (status.code == 401)
        http = HttpResponse::k401Unauthorized;
    else if (status.code == 403)
        http = HttpResponse::k403Forbidden;
    else if (status.code == 404)
        http = HttpResponse::k404NotFound;
    WriteJson(resp, status.code, status.message, "{}", http);
}

void WriteAdminServiceError(HttpResponse* resp, const ServiceStatus& status) {
    WriteJson(resp, status.code, status.message, "{}",
              status.code == 400 || status.code == 404 || status.code == 503
                  ? HttpResponse::k400BadRequest
                  : HttpResponse::k200Ok);
}

}  // namespace

// 构造 HTTP API 服务器，注入业务依赖并注册回调
HttpApiServer::HttpApiServer(EventLoop* loop, const InetAddress& listenAddr,
                             LogicHttpServices services)
    : user_service_(&services.users),
      conversation_service_(&services.conversations),
      attachment_service_(&services.attachments),
      message_service_(&services.messages),
      room_service_(&services.rooms),
      admin_service_(&services.admin),
      danmaku_service_(&services.danmaku),
      knowledge_service_(&services.knowledge),
      room_agent_service_(services.room_agents),
      operator_task_service_(services.operator_tasks),
      server_(loop, listenAddr, "logic_http_server") {
    server_.setHttpCallback(
        [this](const TcpConnectionPtr&, HttpRequest& req, HttpResponse* resp) -> bool {
            onRequest(req, resp);
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
        {"/api/attachment/upload", &HttpApiServer::handleAttachmentUpload},
        {"/api/attachment/get", &HttpApiServer::handleAttachmentGet},
        {"/api/session/history", &HttpApiServer::handleHistory},
        {"/api/knowledge/document", &HttpApiServer::handleKnowledgeDocument},
        {"/api/session/mark_read", &HttpApiServer::handleMarkRead},
        {"/api/session/unread", &HttpApiServer::handleUnread},
        {"/api/chatroom/join", &HttpApiServer::handleChatroomJoin},
        {"/api/chatroom/leave", &HttpApiServer::handleChatroomLeave},
        {"/api/chatroom/unsubscribe", &HttpApiServer::handleChatroomUnsubscribe},
        {"/api/chatroom/online_count", &HttpApiServer::handleChatroomOnlineCount},
        {"/api/danmaku/send", &HttpApiServer::handleDanmakuSend},
        {"/api/danmaku/list", &HttpApiServer::handleDanmakuList},
        {"/api/session/list_single", &HttpApiServer::handleSingleSessionList},
        {"/api/chatroom/list", &HttpApiServer::handleChatroomList},
        {"/api/chatroom/agent/configure", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/agent/task", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/agent/checkpoint", &HttpApiServer::handleRoomAgent},
        {"/api/admin/chatroom/create", &HttpApiServer::handleAdminCreateChatroom},
        {"/api/admin/chatroom/list", &HttpApiServer::handleAdminListChatroom},
        {"/api/admin/broadcast", &HttpApiServer::handleAdminBroadcast},
        {"/api/admin/user/list", &HttpApiServer::handleAdminListUsers},
        {"/api/admin/user/status", &HttpApiServer::handleAdminSetUserStatus},
        {"/api/admin/user/disable", &HttpApiServer::handleAdminSetUserStatus},
        {"/api/admin/user/restore", &HttpApiServer::handleAdminSetUserStatus},
        {"/api/admin/user/delete", &HttpApiServer::handleAdminSetUserStatus},
        {"/api/admin/user/update", &HttpApiServer::handleAdminUpdateUser},
        {"/api/admin/user/revoke_tokens", &HttpApiServer::handleAdminRevokeUserTokens},
        {"/api/admin/audit/list", &HttpApiServer::handleAdminListAuditLogs},
    };

    const std::string& path = req.path();
    if (path == "/metrics") {
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("text/plain; version=0.0.4; charset=utf-8");
        resp->setBody(MetricsRegistry::Instance().RenderPrometheus());
        return;
    }
    auto it = kRouteTable.find(path);
    if (it == kRouteTable.end()) {
        WriteJson(resp, 404, "unknown path");
        return;
    }

    Handler handler = it->second;
    (this->*handler)(req, resp);
}

void HttpApiServer::handleKnowledgeDocument(const HttpRequest& req,
                                            HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int64_t user_id = 0;
    if (!requireUser(req, resp, &user_id)) return;
    (void)user_id;

    nlohmann::json body;
    if (!ParseJsonBody(req.body(), &body) || !body.is_object()) {
        WriteJson(resp, 400, "invalid json body, expect doc_id and optional chunk_id",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    const std::string doc_id = body.value("doc_id", "");
    const std::string chunk_id = body.value("chunk_id", "");
    nlohmann::json document;
    const auto status = knowledge_service_->Document(doc_id, chunk_id, &document);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    WriteJson(resp, 0, "ok", document.dump());
}

// 发送弹幕：校验 token 后写库并推送 Kafka
void HttpApiServer::handleDanmakuSend(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    const auto available = danmaku_service_->SendAvailable();
    if (!available.ok()) {
        WriteServiceError(resp, available);
        return;
    }
    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect "
                  "{\"user_id\":...,\"video_id\":\"...\",\"timeline_ms\":...,"
                  "\"text\":\"...\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t user_id = 0;
    int64_t timeline_ms = 0;
    std::string video_id;
    std::string text;
    std::string token;
    try {
        if (!j.contains("video_id") || !j.contains("timeline_ms") ||
            !j.contains("text")) {
            WriteJson(resp, 400, "video_id/timeline_ms/text required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        // user_id 字段将被 token 解析结果覆盖，仅作为兼容保留
        if (j.contains("user_id")) {
            user_id = j.at("user_id").get<int64_t>();
        }
        timeline_ms = j.at("timeline_ms").get<int64_t>();
        video_id = j.at("video_id").get<std::string>();
        text = j.at("text").get<std::string>();
        if (j.contains("token") && j["token"].is_string()) {
            token = j["token"].get<std::string>();
        }
    } catch (const nlohmann::json::exception& e) {
        WriteJson(resp, 400, "invalid json fields in danmaku send", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    const auto result = danmaku_service_->Send(token, video_id, timeline_ms, text);
    if (!result.status.ok()) {
        // Legacy danmaku puts the token in its body and returns HTTP 400 for
        // authentication failure; do not change this during structural refactoring.
        WriteJson(resp, result.status.code, result.status.message, "{}",
                  result.status.code == 401 || result.status.code == 400
                      ? HttpResponse::k400BadRequest
                      : HttpResponse::k200Ok);
        return;
    }
    WriteJson(
        resp, 0, "ok",
        nlohmann::json{{"msg_id", result.message.msg_id}, {"pushed", result.pushed}}
            .dump());
}

// 账号登录：普通用户使用 PBKDF2；可选管理员口令由环境变量注入。
void HttpApiServer::handleLogin(const HttpRequest& req, HttpResponse* resp) {
    std::string account;
    std::string password;

    // 商用接口：只允许 POST，参数通过 JSON body 传递
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
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

    LoginResult result;
    const auto status = user_service_->Login(account, password, &result);
    if (!status.ok()) {
        // Preserve the existing login/register HTTP status convention.
        WriteJson(resp, status.code, status.message, "{}",
                  status.code == 400 || status.code == 404 ||
                          status.reason == ServiceReason::kInactiveAccount
                      ? HttpResponse::k400BadRequest
                      : HttpResponse::k200Ok);
        return;
    }
    nlohmann::json data = {{"user_id", result.user_id}, {"token", result.token}};
    if (!result.administrator) data["name"] = result.name;
    WriteJson(resp, 0, "ok", data.dump());
}

// 用户注册：创建账号并发放 token
void HttpApiServer::handleRegister(const HttpRequest& req, HttpResponse* resp) {
    std::string account;
    std::string password;
    std::string name;

    // 商用接口：只允许 POST，参数通过 JSON body 传递
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
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

    LoginResult result;
    const auto status = user_service_->Register(account, password, name, &result);
    if (!status.ok()) {
        // Preserve the existing login/register HTTP status convention.
        WriteJson(resp, status.code, status.message, "{}",
                  status.code == 400 || status.code == 409
                      ? HttpResponse::k400BadRequest
                      : HttpResponse::k200Ok);
        return;
    }
    nlohmann::json data = {{"user_id", result.user_id}, {"token", result.token}};
    if (!result.administrator) data["name"] = result.name;
    WriteJson(resp, 0, "ok", data.dump());
}

// 单聊发送消息（HTTP 入口）
void HttpApiServer::handleSendMessage(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
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
            WriteJson(resp, 400, "from_user_id/to_user_id/content required", "{}",
                      HttpResponse::k400BadRequest);
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

    const auto result = message_service_->SendLegacyText(from_user, to_user, content);
    if (!result.status.ok()) {
        WriteServiceError(resp, result.status);
        return;
    }
    const auto& msg = result.message;
    std::ostringstream data;
    data << "{\"session_id\":\"" << msg.session_id << "\",\"msg_id\":\"" << msg.msg_id
         << "\",\"msg_seq\":" << msg.msg_seq << "}";
    WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleAttachmentUpload(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int64_t user_id = 0;
    if (!requireUser(req, resp, &user_id)) return;
    if (!attachment_service_->Available()) {
        WriteJson(resp, 503, "image attachments are not enabled");
        return;
    }
    if (req.body().size() > 8 * 1024 * 1024) {
        WriteJson(resp, 413, "image upload exceeds 4 MiB", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    nlohmann::json body;
    if (!ParseJsonBody(req.body(), &body) || !body.is_object()) {
        WriteJson(resp, 400, "invalid json body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    const std::string session_id = body.value("session_id", "");
    const std::string name = body.value("name", "image");
    const std::string encoded = body.value("data", "");
    StoredAttachment stored;
    const auto status =
        attachment_service_->Upload(user_id, session_id, name, encoded, &stored);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    nlohmann::json data = {
        {"id", stored.id},       {"name", stored.display_name}, {"mime", stored.mime},
        {"bytes", stored.bytes}, {"sha256", stored.sha256},
    };
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleAttachmentGet(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int64_t user_id = 0;
    if (!requireUser(req, resp, &user_id)) return;
    if (!attachment_service_->Available()) {
        WriteJson(resp, 503, "image attachments are not enabled");
        return;
    }
    nlohmann::json body;
    if (!ParseJsonBody(req.body(), &body) || !body.is_object()) {
        WriteJson(resp, 400, "invalid json body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    const std::string id = body.value("id", "");
    AttachmentContents contents;
    const auto status = attachment_service_->Read(user_id, id, &contents);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    const auto& stored = contents.metadata;
    const auto& bytes = contents.bytes;
    std::string encoded;
    if (!EncodeBase64(bytes, &encoded)) {
        WriteJson(resp, 500, "failed to encode image");
        return;
    }
    nlohmann::json data = {
        {"id", stored.id},     {"name", stored.display_name},
        {"mime", stored.mime}, {"bytes", stored.bytes},
        {"data", encoded},
    };
    WriteJson(resp, 0, "ok", data.dump());
}

// 拉取会话历史消息
void HttpApiServer::handleHistory(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t requester_id = 0;
    if (!requireUser(req, resp, &requester_id)) return;

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
        WriteJson(resp, 400, "session_id required", "{}", HttpResponse::k400BadRequest);
        return;
    }
    std::vector<Message> msgs;
    const auto status = conversation_service_->History(requester_id, session_id,
                                                       anchor_seq, limit, &msgs);
    if (!status.ok()) {
        WriteServiceError(resp, status);
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
             << "\"msg_type\":\"" << m.msg_type << "\","
             << "\"content\":" << (m.content_json.empty() ? "\"\"" : m.content_json)
             << "}";
    }
    data << "]}";
    WriteJson(resp, 0, "ok", data.str());
}

// 标记会话已读
void HttpApiServer::handleMarkRead(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t authenticated_id = 0;
    if (!requireUser(req, resp, &authenticated_id)) return;

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
    const auto status = conversation_service_->MarkRead(authenticated_id, user_id,
                                                        session_id, read_seq);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// 查询未读数
void HttpApiServer::handleUnread(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t authenticated_id = 0;
    if (!requireUser(req, resp, &authenticated_id)) return;

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
    int64_t unread = 0;
    const auto status =
        conversation_service_->Unread(authenticated_id, user_id, session_id, &unread);
    if (!status.ok()) {
        WriteServiceError(resp, status);
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
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t authenticated_id = 0;
    if (!requireUser(req, resp, &authenticated_id)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body, expect {\"user_id\":...}", "{}",
                  HttpResponse::k400BadRequest);
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
    std::vector<Session> sessions;
    const auto status =
        conversation_service_->ListSingles(authenticated_id, user_id, &sessions);
    if (!status.ok()) {
        WriteServiceError(resp, status);
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

// 拉取弹幕列表
void HttpApiServer::handleDanmakuList(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body", "{}", HttpResponse::k400BadRequest);
        return;
    }

    std::string video_id;
    try {
        if (!j.contains("video_id")) {
            WriteJson(resp, 400, "video_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        video_id = j.at("video_id").get<std::string>();
    } catch (...) {
        WriteJson(resp, 400, "invalid json fields", "{}", HttpResponse::k400BadRequest);
        return;
    }

    if (video_id.empty()) {
        WriteJson(resp, 400, "video_id cannot be empty", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    // 可选参数：按时间范围和数量限制分页
    int64_t from_ms = 0;
    int64_t to_ms = 0;
    int limit = 2000;  // 单次最多返回 2000 条，避免一次性拉取过多
    try {
        if (j.contains("from_ms")) {
            from_ms = j.at("from_ms").get<int64_t>();
            if (from_ms < 0) from_ms = 0;
        }
        if (j.contains("to_ms")) {
            to_ms = j.at("to_ms").get<int64_t>();
            if (to_ms < 0) to_ms = 0;
        }
        if (j.contains("limit")) {
            limit = j.at("limit").get<int>();
        }
    } catch (...) {
        WriteJson(resp, 400, "invalid from_ms/to_ms/limit", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }

    std::vector<DanmakuItem> list;
    const auto status = danmaku_service_->List(video_id, from_ms, to_ms, limit, &list);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    nlohmann::json resp_j;
    resp_j["danmaku"] = nlohmann::json::array();
    for (const auto& item : list) {
        std::string text = "";
        try {
            auto jj = nlohmann::json::parse(item.content_json);
            if (jj.contains("content") && jj["content"].contains("text")) {
                text = jj["content"]["text"].get<std::string>();
            }
        } catch (...) {
        }

        nlohmann::json entry;
        entry["timeline_ms"] = item.timeline_ms;
        entry["text"] = text;
        resp_j["danmaku"].push_back(entry);
    }

    // 方便前端做增量拉取的简单标记
    resp_j["from_ms"] = from_ms;
    resp_j["to_ms"] = to_ms;
    resp_j["limit"] = limit;
    resp_j["has_more"] = (static_cast<int>(list.size()) >= limit && limit > 0);

    WriteJson(resp, 0, "ok", resp_j.dump());
}

// 列出用户订阅的聊天室
void HttpApiServer::handleChatroomList(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t authenticated_id = 0;
    if (!requireUser(req, resp, &authenticated_id)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body, expect {\"user_id\":...}", "{}",
                  HttpResponse::k400BadRequest);
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
    std::vector<RoomSummary> rooms;
    const auto status =
        room_service_->ListSubscriptions(authenticated_id, user_id, &rooms);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    nlohmann::json data = {{"rooms", nlohmann::json::array()}};
    for (const auto& room : rooms) {
        data["rooms"].push_back({{"session_id", room.session.id},
                                 {"room_id", room.session.group_id},
                                 {"name", room.name},
                                 {"last_msg_seq", room.session.last_msg_seq}});
    }
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleRoomAgent(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int64_t actor = 0;
    if (!requireUser(req, resp, &actor)) return;
    if (!room_agent_service_ || !operator_task_service_) {
        WriteJson(resp, 503, "room Agent unavailable", "{}", static_cast<HttpResponse::HttpStatusCode>(503));
        return;
    }
    try {
        nlohmann::json input;
        if (!ParseJsonBody(req.body(), &input)) throw std::invalid_argument("body");
        const auto room = input.at("room_id").get<int64_t>();
        nlohmann::json output;
        ServiceStatus status;
        if (req.path() == "/api/chatroom/agent/configure")
            status = room_agent_service_->Configure(actor, room, input, &output);
        else if (req.path() == "/api/chatroom/agent/checkpoint")
            status = operator_task_service_->Checkpoint(actor, room, input.at("revision").get<int64_t>(),
                                                       input.at("patch"), &output);
        else status = room_agent_service_->View(actor, room, &output);
        if (!status.ok()) { WriteServiceError(resp, status); return; }
        WriteJson(resp, 0, "ok", output.dump());
    } catch (const std::exception&) {
        WriteJson(resp, 400, "invalid room Agent fields", "{}", HttpResponse::k400BadRequest);
    }
}

// 加入聊天室（订阅关系）
void HttpApiServer::handleChatroomJoin(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t authenticated_id = 0;
    if (!requireUser(req, resp, &authenticated_id)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"room_id\":...,\"user_id\":...}", "{}",
                  HttpResponse::k400BadRequest);
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
    const auto status = room_service_->Join(authenticated_id, user_id, room_id);
    if (!status.ok()) {
        // Missing chatroom historically uses HTTP 200 with business code 404.
        if (status.code == 404)
            WriteJson(resp, status.code, status.message);
        else
            WriteServiceError(resp, status);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// 离开聊天室（在线层）
void HttpApiServer::handleChatroomLeave(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t authenticated_id = 0;
    if (!requireUser(req, resp, &authenticated_id)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"room_id\":...,\"user_id\":...}", "{}",
                  HttpResponse::k400BadRequest);
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
    const auto status = room_service_->Leave(authenticated_id, user_id, room_id);
    if (!status.ok()) {
        // Missing chatroom historically uses HTTP 200 with business code 404.
        if (status.code == 404)
            WriteJson(resp, status.code, status.message);
        else
            WriteServiceError(resp, status);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// 取消订阅聊天室（从成员表移除）
void HttpApiServer::handleChatroomUnsubscribe(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t authenticated_id = 0;
    if (!requireUser(req, resp, &authenticated_id)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"room_id\":...,\"user_id\":...}", "{}",
                  HttpResponse::k400BadRequest);
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
        WriteJson(resp, 400, "invalid json fields in chatroom unsubscribe", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    const auto status = room_service_->Unsubscribe(authenticated_id, user_id, room_id);
    if (!status.ok()) {
        // Missing chatroom historically uses HTTP 200 with business code 404.
        if (status.code == 404)
            WriteJson(resp, status.code, status.message);
        else
            WriteServiceError(resp, status);
        return;
    }
    WriteJson(resp, 0, "ok");
}

bool HttpApiServer::requireUser(const HttpRequest& req, HttpResponse* resp,
                                int64_t* user_id) {
    if (!user_id) {
        WriteJson(resp, 500, "user output is null");
        return false;
    }
    *user_id = 0;
    if (!user_service_ || !user_service_->AuthenticationAvailable()) {
        WriteJson(resp, 503, "user authentication is unavailable");
        return false;
    }
    const std::string authorization = req.getHeader("Authorization");
    constexpr const char* kBearer = "Bearer ";
    if (authorization.rfind(kBearer, 0) != 0 ||
        authorization.size() <= std::char_traits<char>::length(kBearer)) {
        WriteJson(resp, 401, "user bearer token required", "{}",
                  HttpResponse::k401Unauthorized);
        return false;
    }
    const std::string token =
        authorization.substr(std::char_traits<char>::length(kBearer));
    const auto status = user_service_->Authenticate(
        token, UserService::TokenAudience::kHttpUser, user_id);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return false;
    }
    return true;
}

bool HttpApiServer::requireAdmin(const HttpRequest& req, HttpResponse* resp) {
    if (!user_service_ || !user_service_->AdminEnabled()) {
        WriteJson(resp, 503, "admin login is disabled");
        return false;
    }
    std::string authorization = req.getHeader("Authorization");
    constexpr const char* kBearer = "Bearer ";
    if (authorization.rfind(kBearer, 0) != 0 ||
        authorization.size() <= std::char_traits<char>::length(kBearer)) {
        WriteJson(resp, 401, "admin bearer token required", "{}",
                  HttpResponse::k400BadRequest);
        return false;
    }
    const std::string token =
        authorization.substr(std::char_traits<char>::length(kBearer));
    const auto status = user_service_->AuthenticateAdmin(token);
    if (!status.ok()) {
        WriteJson(resp, status.code, status.message, "{}",
                  HttpResponse::k400BadRequest);
        return false;
    }
    return true;
}

// 管理员创建聊天室
void HttpApiServer::handleAdminCreateChatroom(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!requireAdmin(req, resp)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"name\":\"...\",\"owner_id\":...}", "{}",
                  HttpResponse::k400BadRequest);
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
        WriteJson(resp, 400, "invalid json fields in admin create chatroom", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    int64_t group_id = 0;
    const auto status = room_service_->Create(kAdminUserId, name, owner_id, &group_id);
    if (!status.ok()) {
        WriteServiceError(resp, status);
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
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!requireAdmin(req, resp)) return;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body, expect {\"offset\":0,\"limit\":20}",
                  "{}", HttpResponse::k400BadRequest);
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
    std::vector<ImGroup> groups;
    const auto status = room_service_->List(kAdminUserId, offset, limit, &groups);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    std::ostringstream data;
    data << "{\"rooms\":[";
    bool first = true;
    for (const auto& g : groups) {
        if (!first) data << ",";
        first = false;
        data << "{" << "\"room_id\":" << g.id << "," << "\"name\":\"" << g.name << "\","
             << "\"owner_id\":" << g.owner_id << "}";
    }
    data << "]}";
    WriteJson(resp, 0, "ok", data.str());
}

// 管理员广播
void HttpApiServer::handleAdminBroadcast(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!requireAdmin(req, resp)) return;

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
    const auto result =
        message_service_->BroadcastAdmin(kAdminUserId, scope, room_id, text);
    if (!result.status.ok()) {
        WriteServiceError(resp, result.status);
        return;
    }
    WriteJson(resp, 0, "ok",
              nlohmann::json{{"task_id", result.task_id},
                             {"scope", result.scope},
                             {"group_id", result.group_id}}
                  .dump());
}

void HttpApiServer::handleAdminListUsers(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!requireAdmin(req, resp)) return;
    if (!admin_service_->UsersAvailable()) {
        WriteJson(resp, 500, "user dao not initialized");
        return;
    }

    nlohmann::json body = nlohmann::json::object();
    if (!req.body().empty() && !ParseJsonBody(req.body(), &body)) {
        WriteJson(resp, 400, "invalid json body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int offset = 0;
    int limit = 20;
    int status = 0;
    try {
        if (body.contains("offset")) offset = body.at("offset").get<int>();
        if (body.contains("limit")) limit = body.at("limit").get<int>();
        if (body.contains("status")) {
            if (body.at("status").is_number_integer() &&
                body.at("status").get<int>() == 0) {
                status = 0;
            } else if (!ParseUserStatus(body.at("status"), &status)) {
                WriteJson(resp, 400, "status must be active/disabled/deleted or 0",
                          "{}", HttpResponse::k400BadRequest);
                return;
            }
        }
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "invalid user list fields", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    std::vector<User> users;
    int total = 0;
    const auto outcome =
        admin_service_->ListUsers(kAdminUserId, offset, limit, status, &users, &total);
    if (!outcome.ok()) {
        WriteServiceError(resp, outcome);
        return;
    }
    nlohmann::json data;
    data["total"] = total;
    data["offset"] = offset;
    data["limit"] = limit;
    data["users"] = nlohmann::json::array();
    for (const auto& user : users) {
        nlohmann::json item = {
            {"user_id", user.id},         {"account", user.account},
            {"name", user.name},          {"status", UserStatusName(user.status)},
            {"status_code", user.status}, {"created_at", user.created_at},
        };
        if (user.deleted_at.empty()) {
            item["deleted_at"] = nullptr;
        } else {
            item["deleted_at"] = user.deleted_at;
        }
        data["users"].push_back(std::move(item));
    }
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleAdminSetUserStatus(const HttpRequest& req,
                                             HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!requireAdmin(req, resp)) return;
    if (!admin_service_->CenterAvailable()) {
        WriteJson(resp, 500, "user center dependencies not initialized");
        return;
    }

    nlohmann::json body;
    if (!ParseJsonBody(req.body(), &body)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"user_id\":1,\"status\":\"disabled\"}",
                  "{}", HttpResponse::k400BadRequest);
        return;
    }
    int64_t user_id = 0;
    int status = 0;
    std::string reason;
    try {
        if (!body.contains("user_id")) {
            WriteJson(resp, 400, "user_id required", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        user_id = body.at("user_id").get<int64_t>();
        const std::string path = req.path();
        if (path == "/api/admin/user/disable") {
            status = kUserStatusDisabled;
        } else if (path == "/api/admin/user/restore") {
            status = kUserStatusActive;
        } else if (path == "/api/admin/user/delete") {
            status = kUserStatusDeleted;
        } else if (!body.contains("status") ||
                   !ParseUserStatus(body.at("status"), &status)) {
            WriteJson(resp, 400, "status must be active/disabled/deleted", "{}",
                      HttpResponse::k400BadRequest);
            return;
        }
        if (body.contains("reason")) reason = body.at("reason").get<std::string>();
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "invalid user status fields", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    nlohmann::json data;
    const auto outcome =
        admin_service_->SetStatus(kAdminUserId, user_id, status, reason, &data);
    if (!outcome.ok()) {
        WriteAdminServiceError(resp, outcome);
        return;
    }
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleAdminUpdateUser(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!requireAdmin(req, resp)) return;
    if (!admin_service_->UsersAvailable()) {
        WriteJson(resp, 500, "user dao not initialized");
        return;
    }
    nlohmann::json body;
    if (!ParseJsonBody(req.body(), &body)) {
        WriteJson(resp, 400,
                  "invalid json body, expect {\"user_id\":1,\"name\":\"...\"}", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    int64_t user_id = 0;
    std::string name;
    try {
        user_id = body.at("user_id").get<int64_t>();
        name = body.at("name").get<std::string>();
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "user_id/name required", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    nlohmann::json data;
    const auto outcome = admin_service_->UpdateName(kAdminUserId, user_id, name, &data);
    if (!outcome.ok()) {
        WriteAdminServiceError(resp, outcome);
        return;
    }
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleAdminRevokeUserTokens(const HttpRequest& req,
                                                HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!requireAdmin(req, resp)) return;
    if (!admin_service_->CenterAvailable()) {
        WriteJson(resp, 500, "user center dependencies not initialized");
        return;
    }
    nlohmann::json body;
    if (!ParseJsonBody(req.body(), &body)) {
        WriteJson(resp, 400, "invalid json body, expect {\"user_id\":1}", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    int64_t user_id = 0;
    try {
        user_id = body.at("user_id").get<int64_t>();
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "user_id required", "{}", HttpResponse::k400BadRequest);
        return;
    }
    nlohmann::json data;
    const auto outcome = admin_service_->RevokeTokens(kAdminUserId, user_id, &data);
    if (!outcome.ok()) {
        WriteAdminServiceError(resp, outcome);
        return;
    }
    WriteJson(resp, 0, "ok", data.dump());
}

void HttpApiServer::handleAdminListAuditLogs(const HttpRequest& req,
                                             HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    if (!requireAdmin(req, resp)) return;
    if (!admin_service_->AuditAvailable()) {
        WriteJson(resp, 500, "audit log dao not initialized");
        return;
    }
    nlohmann::json body = nlohmann::json::object();
    if (!req.body().empty() && !ParseJsonBody(req.body(), &body)) {
        WriteJson(resp, 400, "invalid json body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int offset = 0;
    int limit = 20;
    int64_t target_user_id = 0;
    try {
        if (body.contains("offset")) offset = body.at("offset").get<int>();
        if (body.contains("limit")) limit = body.at("limit").get<int>();
        if (body.contains("target_user_id")) {
            target_user_id = body.at("target_user_id").get<int64_t>();
        }
    } catch (const nlohmann::json::exception&) {
        WriteJson(resp, 400, "invalid audit list fields", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    std::vector<AuditLogRecord> records;
    int total = 0;
    const auto status = admin_service_->ListAudit(kAdminUserId, offset, limit,
                                                  target_user_id, &records, &total);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    nlohmann::json data = {{"total", total}};
    data["offset"] = offset;
    data["limit"] = limit;
    data["logs"] = nlohmann::json::array();
    for (const auto& record : records) {
        data["logs"].push_back({
            {"id", record.id},
            {"actor_user_id", record.actor_user_id},
            {"target_user_id", record.target_user_id},
            {"action", record.action},
            {"reason", record.reason},
            {"metadata", record.metadata_json},
            {"created_at", record.created_at},
        });
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 查询聊天室在线人数
void HttpApiServer::handleChatroomOnlineCount(const HttpRequest& req,
                                              HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }

    int64_t authenticated_id = 0;
    if (!requireUser(req, resp, &authenticated_id)) return;
    (void)authenticated_id;

    nlohmann::json j;
    if (!ParseJsonBody(req.body(), &j)) {
        WriteJson(resp, 400, "invalid json body, expect {\"room_id\":...}", "{}",
                  HttpResponse::k400BadRequest);
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
        WriteJson(resp, 400, "invalid json fields in chatroom online_count", "{}",
                  HttpResponse::k400BadRequest);
        return;
    }
    int64_t count = 0;
    const auto status = room_service_->OnlineCount(room_id, &count);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    WriteJson(resp, 0, "ok",
              nlohmann::json{{"room_id", room_id}, {"online_count", count}}.dump());
}

}  // namespace sparkpush
