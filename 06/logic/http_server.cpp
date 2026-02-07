#include "http_server.h"

#include "logging.h"
#include "spark_push.pb.h"

#include <chrono>
#include <fstream>
#include <random>
#include <unordered_map>
#include <sstream>
#include <nlohmann/json.hpp>

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
    LogError("JSON parse error: " + std::string(e.what()));
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
bool ParseJsonAccountPassword(const std::string& body,
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
    LogError("JSON parse error: " + std::string(e.what()));
    return false;
  } catch (...) {
    return false;
  }
}

// 解析注册请求（包含可选的 name）
bool ParseJsonRegister(const std::string& body,
                       std::string* account,
                       std::string* password,
                       std::string* name) {
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

// 生成密码学安全的随机 token（读取 /dev/urandom）
std::string GenerateSecureToken() {
  static constexpr int kTokenBytes = 24;  // 192 bit → 32 chars hex
  unsigned char buf[kTokenBytes];
  std::ifstream urandom("/dev/urandom", std::ios::binary);
  if (urandom.good()) {
    urandom.read(reinterpret_cast<char*>(buf), kTokenBytes);
  } else {
    // 降级：使用 std::random_device
    std::random_device rd;
    for (int i = 0; i < kTokenBytes; i += 4) {
      uint32_t val = rd();
      std::memcpy(buf + i, &val, std::min(4, kTokenBytes - i));
    }
  }
  static const char hex[] = "0123456789abcdef";
  std::string token;
  token.reserve(kTokenBytes * 2);
  for (int i = 0; i < kTokenBytes; ++i) {
    token.push_back(hex[buf[i] >> 4]);
    token.push_back(hex[buf[i] & 0x0f]);
  }
  return token;
}

void WriteJson(HttpResponse* resp, int code, const std::string& message,
               const std::string& data_json = "{}",
               HttpResponse::HttpStatusCode http_code = HttpResponse::k200Ok) {
  resp->setStatusCode(http_code);
  resp->setContentType("application/json; charset=utf-8");
  // 所有 JSON API 统一打上 CORS 头，方便在 9010/其他端口的前端页面直接调用
  AddCORSHeaders(resp);
  std::ostringstream oss;
  oss << "{\"code\":" << code
      << ",\"message\":\"" << message << "\""
      << ",\"data\":" << data_json << "}";
  resp->setBody(oss.str());
}

}  // namespace

HttpApiServer::HttpApiServer(EventLoop* loop,
                             const InetAddress& listenAddr,
                             ConversationStore* store,
                             UserDao* user_dao,
                             GroupDao* group_dao,
                             GroupMemberDao* group_member_dao,
                             RedisStore* redis_store,
                             KafkaProducer* kafka_producer,
                             KafkaProducer* broadcast_producer,
                             DanmakuDao* danmaku_dao)
    : store_(store),
      user_dao_(user_dao),
      group_dao_(group_dao),
      group_member_dao_(group_member_dao),
      redis_store_(redis_store),
      kafka_producer_(kafka_producer),
      broadcast_producer_(broadcast_producer),
      danmaku_dao_(danmaku_dao),
      server_(loop, listenAddr, "logic_http_server") {
  // 适配新的 HttpServer 回调签名：bool (const TcpConnectionPtr&, HttpRequest&, HttpResponse*)
  server_.setHttpCallback(
      [this](const TcpConnectionPtr&,
             HttpRequest& req,
             HttpResponse* resp) -> bool {
        this->onRequest(req, resp);
        return true;
      });
}

void HttpApiServer::start() {
  server_.start();
}

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
      {"/api/session/history", &HttpApiServer::handleHistory},
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
      {"/api/admin/chatroom/create", &HttpApiServer::handleAdminCreateChatroom},
      {"/api/admin/chatroom/list", &HttpApiServer::handleAdminListChatroom},
      {"/api/admin/broadcast", &HttpApiServer::handleAdminBroadcast},
  };

  const std::string& path = req.path();

  // ---- 运维端点（无需鉴权） ----
  if (path == "/health") {
    resp->setStatusCode(HttpResponse::k200Ok);
    resp->setContentType("application/json; charset=utf-8");
    AddCORSHeaders(resp);
    // 检查关键依赖是否可用
    bool redis_ok = (redis_store_ != nullptr);
    bool store_ok = (store_ != nullptr);
    std::string status = (redis_ok && store_ok) ? "ok" : "degraded";
    resp->setBody("{\"status\":\"" + status + "\""
                  ",\"redis\":" + std::string(redis_ok ? "true" : "false") +
                  ",\"store\":" + std::string(store_ok ? "true" : "false") + "}");
    return;
  }
  if (path == "/metrics") {
    resp->setStatusCode(HttpResponse::k200Ok);
    resp->setContentType("text/plain; charset=utf-8");
    AddCORSHeaders(resp);
    // Prometheus-compatible 简易指标（可扩展）
    std::ostringstream m;
    m << "# HELP sparkpush_up Whether the service is up\n"
      << "# TYPE sparkpush_up gauge\n"
      << "sparkpush_up 1\n";
    resp->setBody(m.str());
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

// ---- 统一 Token 鉴权 ----
// 从 Authorization: Bearer <token> 头提取 token，降级到 JSON body 的 "token" 字段。
// 校验成功返回 user_id (>0)；失败则写入 401 响应并返回 0。
int64_t HttpApiServer::AuthenticateRequest(const HttpRequest& req,
                                           HttpResponse* resp) {
  std::string token;

  // 1) 优先从 Authorization 头获取
  std::string auth_header = req.getHeader("Authorization");
  if (auth_header.size() > 7 &&
      auth_header.substr(0, 7) == "Bearer ") {
    token = auth_header.substr(7);
  }

  // 2) 降级：从 JSON body 的 "token" 字段获取
  if (token.empty()) {
    try {
      auto j = nlohmann::json::parse(req.body());
      if (j.contains("token") && j["token"].is_string()) {
        token = j["token"].get<std::string>();
      }
    } catch (...) {
      // body 不是合法 JSON 或无 token 字段，忽略
    }
  }

  if (token.empty()) {
    WriteJson(resp, 401, "authentication required: provide Authorization header or token field",
              "{}", HttpResponse::k400BadRequest);
    return 0;
  }

  if (!redis_store_) {
    WriteJson(resp, 500, "redis store not initialized");
    return 0;
  }

  int64_t user_id = 0;
  if (!redis_store_->GetUserIdByToken(token, &user_id) || user_id <= 0) {
    WriteJson(resp, 401, "invalid or expired token",
              "{}", HttpResponse::k400BadRequest);
    return 0;
  }

  return user_id;
}

void HttpApiServer::handleDanmakuSend(const HttpRequest& req,
                                      HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp, 405, "only POST allowed", "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (!store_) {
    WriteJson(resp, 500, "conversation store not initialized");
    return;
  }
  if (!kafka_producer_) {
    WriteJson(resp, 500, "kafka producer not initialized");
    return;
  }

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"user_id\":...,\"video_id\":\"...\",\"timeline_ms\":...,\"text\":\"...\"}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权：通过 token 确定 user_id，禁止客户端伪造
  int64_t user_id = AuthenticateRequest(req, resp);
  if (user_id <= 0) return;

  int64_t timeline_ms = 0;
  std::string video_id;
  std::string text;
  try {
    if (!j.contains("video_id") ||
        !j.contains("timeline_ms") || !j.contains("text")) {
      WriteJson(resp,
                400,
                "video_id/timeline_ms/text required",
                "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    timeline_ms = j.at("timeline_ms").get<int64_t>();
    video_id = j.at("video_id").get<std::string>();
    text = j.at("text").get<std::string>();
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in danmaku send",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  if (video_id.empty() || text.empty()) {
    WriteJson(resp,
              400,
              "video_id/text required",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 对 demo 而言，用固定的 room_id 承载该视频的弹幕聊天室。
  // 如果未来支持多视频，可约定 video_id 与 room_id 的映射策略。
  const int64_t kDanmakuRoomId = 1001;

  // 构造下行给前端的 JSON 内容
  std::ostringstream content_oss;
  content_oss << "{\"type\":\"danmaku\""
              << ",\"video_id\":\"" << video_id << "\""
              << ",\"timeline_ms\":" << timeline_ms
              << ",\"content\":{\"text\":\"" << text << "\"}}";
  std::string content_json = content_oss.str();

  // 写入 MySQL 的 video_danmaku 表（如果已配置）
  if (danmaku_dao_) {
    int64_t row_id = 0;
    std::string err;
    if (!danmaku_dao_->InsertDanmaku(
            video_id, timeline_ms, user_id, content_json, &row_id, &err)) {
      // 中：写库失败只打日志，不中断实时推送
      LogError("InsertDanmaku failed: " + err);
    }
  }

  // 写入内存模型（会话 & 消息），并更新 Redis 中的 last_seq
  Session session;
  std::string err;
  if (!store_->GetOrCreateRoomSession(kDanmakuRoomId, &session, &err)) {
    WriteJson(resp, 500, "get room session failed: " + err);
    return;
  }
  int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  Message msg;
  if (!store_->AppendMessage(session,
                             user_id,
                             "danmaku",
                             content_json,
                             now_ms,
                             "",  // client_msg_id (HTTP 暂不透传)
                             &msg,
                             &err)) {
    WriteJson(resp, 500, "append danmaku failed: " + err);
    return;
  }

  // 构造 ChatMessage
  ChatMessage cm;
  cm.set_msg_id(msg.msg_id);
  cm.set_session_id(msg.session_id);
  cm.set_msg_seq(msg.msg_seq);
  cm.set_sender_id(msg.sender_id);
  cm.set_timestamp_ms(msg.timestamp_ms);
  cm.set_msg_type(msg.msg_type);
  cm.set_content_json(msg.content_json);

  // 计算需要推送到哪些 comet
  std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
  std::vector<std::string> room_comets;
  bool use_room_comets = false;
  if (redis_store_ &&
      redis_store_->GetRoomComets(kDanmakuRoomId, &room_comets) &&
      !room_comets.empty()) {
    use_room_comets = true;
  }

  if (use_room_comets) {
    // 仅按 comet 维度 fanout，targets 留空，由 comet 自己按房间成员二次分发
    for (const auto& cid : room_comets) {
      comet_to_users[cid];  // 占位
    }
  } else {
    // 回退：根据房间成员+用户路由做精确 fanout
    std::vector<int64_t> members;
    if (group_member_dao_) {
      if (!group_member_dao_->ListRoomMembers(kDanmakuRoomId, &members, &err)) {
        LogError("ListRoomMembers(danmaku) failed: " + err);
      }
    }
    for (int64_t uid : members) {
      std::vector<std::string> comets;
      if (!redis_store_ ||
          !redis_store_->GetUserRoutes(uid, &comets)) {
        continue;
      }
      for (const auto& cid : comets) {
        comet_to_users[cid].push_back(uid);
      }
    }
  }

  if (comet_to_users.empty()) {
    // 没有在线用户，返回成功但不推送
    std::ostringstream data;
    data << "{\"msg_id\":\"" << msg.msg_id << "\",\"pushed\":false}";
    WriteJson(resp, 0, "ok", data.str());
    return;
  }

  // 通过 Kafka 将 PushToCometRequest 写入 push topic
  for (const auto& kv : comet_to_users) {
    const std::string& comet_id = kv.first;
    const auto& users = kv.second;

    PushToCometRequest req_pb;
    req_pb.set_comet_id(comet_id);
    *req_pb.mutable_message() = cm;
    for (int64_t uid : users) {
      auto* t = req_pb.add_targets();
      t->set_user_id(uid);
    }

    std::string payload;
    if (!req_pb.SerializeToString(&payload)) {
      LogError("Serialize PushToCometRequest(danmaku) failed");
      continue;
    }
    if (!kafka_producer_->Send(comet_id, payload)) {
      LogError("Kafka send danmaku failed for comet " + comet_id);
    }
  }

  std::ostringstream data;
  data << "{\"msg_id\":\"" << msg.msg_id << "\",\"pushed\":true}";
  WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleLogin(const HttpRequest& req, HttpResponse* resp) {
  std::string account;
  std::string password;

  // 商用接口：只允许 POST，参数通过 JSON body 传递
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  if (req.body().empty()) {
    WriteJson(resp,
              400,
              "empty body",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  if (!ParseJsonAccountPassword(req.body(), &account, &password)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"account\":\"...\",\"password\":\"...\"}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 特殊处理：预设管理员账号走内存校验，不依赖数据库中是否存在该账号。
  if (account == kAdminAccount) {
    if (password != kAdminPasswordHash) {
      WriteJson(resp, 401, "invalid password");
      return;
    }
    // 生成安全随机 token
    std::string token = GenerateSecureToken();
    if (!redis_store_ ||
        !redis_store_->SetToken(token, kAdminUserId, 24 * 3600)) {
      WriteJson(resp, 500, "save token to redis failed");
      return;
    }

    std::ostringstream data;
    data << "{\"user_id\":" << kAdminUserId << ",\"token\":\"" << token << "\"}";
    WriteJson(resp, 0, "ok", data.str());
    return;
  }

  if (account.empty()) {
    WriteJson(resp, 400, "account is required", "{}", HttpResponse::k400BadRequest);
    return;
  }

  if (!user_dao_) {
    WriteJson(resp, 500, "user dao not initialized");
    return;
  }

  User user;
  std::string err;
  if (!user_dao_->GetUserByAccount(account, &user, &err)) {
    // 严格登录模式：不存在则提示先注册，而不是自动创建
    WriteJson(resp,
              404,
              "user not found, please register first",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 这里约定 password 已经是前端做过 MD5 的 hash（示例实现，真实业务还需加盐、迭代等）
  if (user.password_hash != password) {
    WriteJson(resp, 401, "invalid password");
    return;
  }

  // 生成安全随机 token
  std::string token = GenerateSecureToken();
  if (!redis_store_ ||
      !redis_store_->SetToken(token, user.id, 24 * 3600)) {
    WriteJson(resp, 500, "save token to redis failed");
    return;
  }

  std::ostringstream data;
  data << "{\"user_id\":" << user.id 
       << ",\"token\":\"" << token << "\""
       << ",\"name\":\"" << user.name << "\"}";
  WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleRegister(const HttpRequest& req,
                                   HttpResponse* resp) {
  std::string account;
  std::string password;
  std::string name;

  // 商用接口：只允许 POST，参数通过 JSON body 传递
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  if (req.body().empty()) {
    WriteJson(resp,
              400,
              "empty body",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  if (!ParseJsonRegister(req.body(), &account, &password, &name)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"account\":\"...\",\"password\":\"...\"}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  if (account.empty() || password.empty()) {
    WriteJson(resp,
              400,
              "account/password are required",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 预设管理员账号保留，禁止通过注册接口覆盖/创建同名账号。
  if (account == kAdminAccount) {
    WriteJson(resp,
              409,
              "admin account is reserved, please use login directly",
              "{}",
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
    WriteJson(resp,
              409,
              "account already exists",
              "{}",
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
  std::string token = GenerateSecureToken();
  if (!redis_store_ ||
      !redis_store_->SetToken(token, user.id, 24 * 3600)) {
    WriteJson(resp, 500, "save token to redis failed");
    return;
  }

  std::ostringstream data;
  data << "{\"user_id\":" << user.id 
       << ",\"token\":\"" << token << "\""
       << ",\"name\":\"" << user.name << "\"}";
  WriteJson(resp, 0, "ok", data.str());
}

// HTTP 消息发送接口
// 优化前路径（5-6 次 MySQL）: HTTP → GetOrCreateSession(MySQL) → AllocateSeq(MySQL)
//                            → InsertMessage(MySQL) → GetUserById(MySQL) → Kafka
// 优化后路径（2 次 Redis）:   HTTP → 构造 session_id → IncrMsgSeq(Redis)
//                            → GetUserRoutes(Redis/缓存) → Kafka
// 优化效果：QPS 从 ~82 提升到 ~5710（70 倍），具体见 performance_optimization.md
void HttpApiServer::handleSendMessage(const HttpRequest& req,
                                      HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp, 405, "only POST allowed", "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权：from_user 由 token 决定，禁止客户端伪造
  int64_t from_user = AuthenticateRequest(req, resp);
  if (from_user <= 0) return;

  // 发送限流（已注释：保留代码供面试讲解，实际压测时关闭以减少 Redis 开销）
  // if (redis_store_) {
  //   std::string rate_key = "rate:msg:" + std::to_string(from_user);
  //   if (!redis_store_->CheckRateLimit(rate_key, 1000, 20)) {
  //     WriteJson(resp, 429, "rate limit exceeded, try again later", "{}",
  //               HttpResponse::k400BadRequest);
  //     return;
  //   }
  // }

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"to_user_id\":...,\"content\":\"...\"}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  int64_t to_user = 0;
  std::string content;
  try {
    if (!j.contains("to_user_id") || !j.contains("content")) {
      WriteJson(resp,
                400,
                "to_user_id/content required",
                "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    to_user = j.at("to_user_id").get<int64_t>();
    content = j.at("content").get<std::string>();
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in message send",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  if (to_user <= 0 || content.empty()) {
    WriteJson(resp, 400, "to_user_id/content required", "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  if (!redis_store_ || !kafka_producer_) {
    WriteJson(resp, 500, "redis or kafka not initialized");
    return;
  }

  // 构造 session_id（与 gRPC 路径一致，无 MySQL）
  // 统一 session_id 命名格式，与 gRPC 一致：single:{min_id}:{max_id}
  int64_t u1 = std::min(from_user, to_user);
  int64_t u2 = std::max(from_user, to_user);
  std::string session_id = "single:" + std::to_string(u1) + ":" + std::to_string(u2);

  // 使用 Redis INCR 获取 msg_seq（无 MySQL）
  int64_t msg_seq = 0;
  if (!redis_store_->IncrSessionMsgSeq(session_id, &msg_seq)) {
    WriteJson(resp, 500, "allocate msg_seq failed");
    return;
  }

  // 构造 msg_id
  std::string msg_id = session_id + "-" + std::to_string(msg_seq);

  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

  // 构造推送 JSON
  nlohmann::json push_j;
  push_j["type"] = "single_chat";
  push_j["msg_id"] = msg_id;
  push_j["msg_seq"] = msg_seq;
  push_j["from_user_id"] = from_user;
  push_j["to_user_id"] = to_user;
  push_j["content"] = content;
  push_j["create_time"] = now_ms;
  std::string push_json = push_j.dump();

  // 获取接收方路由
  std::vector<std::string> comets;
  bool has_route = redis_store_->GetUserRoutes(to_user, &comets);

  // 构造并发送 PushToCometRequest（带持久化标记）
  if (has_route && !comets.empty()) {
    for (const auto& comet_id : comets) {
      PushToCometRequest push_req;
      push_req.set_comet_id(comet_id);
      push_req.set_need_persist(true);
      
      ChatMessage* cm = push_req.mutable_message();
      cm->set_msg_id(msg_id);
      cm->set_session_id(session_id);
      cm->set_msg_seq(msg_seq);
      cm->set_sender_id(from_user);
      cm->set_timestamp_ms(now_ms);
      cm->set_msg_type("text");
      cm->set_content_json(push_json);
      
      auto* target = push_req.add_targets();
      target->set_user_id(to_user);

      std::string payload;
      if (push_req.SerializeToString(&payload)) {
        kafka_producer_->Send(comet_id, payload);
      }
    }
  } else {
    // 无路由，仅持久化
    PushToCometRequest push_req;
    push_req.set_comet_id("persist");
    push_req.set_need_persist(true);
    
    ChatMessage* cm = push_req.mutable_message();
    cm->set_msg_id(msg_id);
    cm->set_session_id(session_id);
    cm->set_msg_seq(msg_seq);
    cm->set_sender_id(from_user);
    cm->set_timestamp_ms(now_ms);
    cm->set_msg_type("text");
    cm->set_content_json(push_json);
    
    auto* target = push_req.add_targets();
    target->set_user_id(to_user);

    std::string payload;
    if (push_req.SerializeToString(&payload)) {
      kafka_producer_->Send("persist", payload);
    }
  }

  std::ostringstream data;
  data << "{\"session_id\":\"" << session_id
       << "\",\"msg_id\":\"" << msg_id
       << "\",\"msg_seq\":" << msg_seq << "}";
  WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleHistory(const HttpRequest& req,
                                  HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权：确保请求来自已登录用户
  int64_t auth_user_id = AuthenticateRequest(req, resp);
  if (auth_user_id <= 0) return;

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"session_id\":\"...\",\"anchor_seq\":0,\"limit\":20}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  std::string session_id;
  int64_t anchor_seq = 0;
  int limit = 20;
  try {
    if (!j.contains("session_id")) {
      WriteJson(resp, 400, "session_id required", "{}", HttpResponse::k400BadRequest);
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
    WriteJson(resp,
              400,
              "invalid json fields in history",
              "{}",
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
    data << "{"
         << "\"msg_id\":\"" << m.msg_id << "\","
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

void HttpApiServer::handleMarkRead(const HttpRequest& req,
                                   HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权
  int64_t user_id = AuthenticateRequest(req, resp);
  if (user_id <= 0) return;

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"session_id\":\"...\",\"read_seq\":...}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  std::string session_id;
  int64_t read_seq = 0;
  try {
    if (!j.contains("session_id") || !j.contains("read_seq")) {
      WriteJson(resp, 400, "session_id/read_seq required", "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    session_id = j.at("session_id").get<std::string>();
    read_seq = j.at("read_seq").get<int64_t>();
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in mark_read",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (session_id.empty() || read_seq <= 0) {
    WriteJson(resp, 400, "session_id/read_seq required", "{}",
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

void HttpApiServer::handleUnread(const HttpRequest& req,
                                 HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权
  int64_t user_id = AuthenticateRequest(req, resp);
  if (user_id <= 0) return;

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"session_id\":\"...\"}",
              "{}",
              HttpResponse::k400BadRequest);
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
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in unread",
              "{}",
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

void HttpApiServer::handleSingleSessionList(const HttpRequest& req,
                                            HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权：user_id 由 token 决定
  int64_t user_id = AuthenticateRequest(req, resp);
  if (user_id <= 0) return;
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
    data << "{"
         << "\"session_id\":\"" << s.id << "\","
         << "\"peer_user_id\":" << peer_id << ","
         << "\"last_msg_seq\":" << s.last_msg_seq
         << "}";
  }
  data << "]}";
  WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleDanmakuList(const HttpRequest& req,
                                      HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp, 405, "only POST allowed", "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权
  int64_t auth_user_id = AuthenticateRequest(req, resp);
  if (auth_user_id <= 0) return;

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp, 400, "invalid json body", "{}",
              HttpResponse::k400BadRequest);
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
    WriteJson(resp, 400, "invalid json fields", "{}",
              HttpResponse::k400BadRequest);
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

  if (!danmaku_dao_) {
    WriteJson(resp, 500, "danmaku dao not initialized");
    return;
  }

  std::vector<DanmakuItem> list;
  std::string err;
  if (!danmaku_dao_->ListDanmaku(video_id, &list, &err, from_ms, to_ms, limit)) {
    WriteJson(resp, 500, "list danmaku failed: " + err);
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

void HttpApiServer::handleChatroomList(const HttpRequest& req,
                                       HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权：user_id 由 token 决定
  int64_t user_id = AuthenticateRequest(req, resp);
  if (user_id <= 0) return;
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
      LogError("GetOrCreateRoomSession failed: " + err);
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
    data << "{"
         << "\"session_id\":\"" << s.id << "\","
         << "\"room_id\":" << s.group_id << ","
         << "\"name\":\"" << name << "\","
         << "\"last_msg_seq\":" << s.last_msg_seq
         << "}";
  }
  data << "]}";
  WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleChatroomJoin(const HttpRequest& req,
                                       HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权
  int64_t user_id = AuthenticateRequest(req, resp);
  if (user_id <= 0) return;

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"room_id\":...}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  int64_t room_id = 0;
  try {
    if (!j.contains("room_id")) {
      WriteJson(resp,
                400,
                "room_id required",
                "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    room_id = j.at("room_id").get<int64_t>();
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in chatroom join",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (room_id <= 0) {
    WriteJson(resp, 400, "room_id required", "{}",
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

void HttpApiServer::handleChatroomLeave(const HttpRequest& req,
                                        HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权
  int64_t user_id = AuthenticateRequest(req, resp);
  if (user_id <= 0) return;

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"room_id\":...}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  int64_t room_id = 0;
  try {
    if (!j.contains("room_id")) {
      WriteJson(resp,
                400,
                "room_id required",
                "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    room_id = j.at("room_id").get<int64_t>();
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in chatroom leave",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (room_id <= 0) {
    WriteJson(resp, 400, "room_id required", "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  // 这里的 leave 仅表示“当前不再接收该房间的实时消息”，
  // 实际的在线状态由 WebSocket + comet/Redis 驱动。
  // 订阅关系交由 /api/chatroom/unsubscribe 维护，这里不修改内存模型中的订阅集合。
  WriteJson(resp, 0, "ok");
}

void HttpApiServer::handleChatroomUnsubscribe(const HttpRequest& req,
                                              HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权
  int64_t user_id = AuthenticateRequest(req, resp);
  if (user_id <= 0) return;

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"room_id\":...}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  int64_t room_id = 0;
  try {
    if (!j.contains("room_id")) {
      WriteJson(resp,
                400,
                "room_id required",
                "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    room_id = j.at("room_id").get<int64_t>();
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in chatroom unsubscribe",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (room_id <= 0) {
    WriteJson(resp, 400, "room_id required", "{}",
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

void HttpApiServer::handleAdminCreateChatroom(const HttpRequest& req,
                                              HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 管理员鉴权：必须是 admin 用户
  int64_t auth_user_id = AuthenticateRequest(req, resp);
  if (auth_user_id <= 0) return;
  if (auth_user_id != kAdminUserId) {
    WriteJson(resp, 403, "admin privilege required", "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"name\":\"...\"}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  std::string name;
  int64_t owner_id = auth_user_id;  // 创建者即为管理员
  try {
    if (!j.contains("name")) {
      WriteJson(resp,
                400,
                "name required",
                "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    name = j.at("name").get<std::string>();
    // 可选覆盖 owner_id
    if (j.contains("owner_id")) {
      owner_id = j.at("owner_id").get<int64_t>();
    }
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in admin create chatroom",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (name.empty()) {
    WriteJson(resp,
              400,
              "name required",
              "{}",
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
  data << "{\"room_id\":" << group_id
       << ",\"name\":\"" << name << "\"}";
  WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleAdminListChatroom(const HttpRequest& req,
                                            HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 管理员鉴权
  int64_t auth_user_id = AuthenticateRequest(req, resp);
  if (auth_user_id <= 0) return;
  if (auth_user_id != kAdminUserId) {
    WriteJson(resp, 403, "admin privilege required", "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"offset\":0,\"limit\":20}",
              "{}",
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
    WriteJson(resp,
              400,
              "invalid json fields in admin list chatroom",
              "{}",
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
    data << "{"
         << "\"room_id\":" << g.id << ","
         << "\"name\":\"" << g.name << "\","
         << "\"owner_id\":" << g.owner_id
         << "}";
  }
  data << "]}";
  WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleAdminBroadcast(const HttpRequest& req,
                                         HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 管理员鉴权
  int64_t auth_user_id = AuthenticateRequest(req, resp);
  if (auth_user_id <= 0) return;
  if (auth_user_id != kAdminUserId) {
    WriteJson(resp, 403, "admin privilege required", "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"scope\":\"all|chatroom\",\"room_id\":0,\"text\":\"...\"}",
              "{}",
              HttpResponse::k400BadRequest);
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
      WriteJson(resp,
                400,
                "text is required",
                "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    text = j.at("text").get<std::string>();
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in admin broadcast",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (text.empty()) {
    WriteJson(resp,
              400,
              "text is required",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (scope != "all" && scope != "chatroom") {
    WriteJson(resp,
              400,
              "scope must be all or chatroom",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (scope == "chatroom" && room_id <= 0) {
    WriteJson(resp,
              400,
              "room_id required when scope=chatroom",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }
  if (!broadcast_producer_) {
    WriteJson(resp, 500, "broadcast producer not initialized");
    return;
  }

  // 构造广播任务内容，与 LogicServiceImpl::Broadcast 使用的 BroadcastTaskRequest 保持一致
  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  std::string task_id =
      [&]() {
        thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> dist(0, 99999);
        return "bcast-" + std::to_string(now_ms) + "-" + std::to_string(dist(rng));
      }();

  std::string real_scope = (scope == "all") ? "all" : "group";
  int64_t group_id = (scope == "chatroom") ? room_id : 0;

  // 构造 JSON 内容：一个简单的系统广播文本
  std::ostringstream content_oss;
  content_oss << "{\"type\":\"system_broadcast\""
              << ",\"scope\":\"" << real_scope << "\"";
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
  if (!broadcast_producer_->Send(task_id, payload)) {
    WriteJson(resp, 500, "send broadcast task to kafka failed");
    return;
  }

  std::ostringstream data;
  data << "{\"task_id\":\"" << task_id << "\""
       << ",\"scope\":\"" << real_scope << "\""
       << ",\"group_id\":" << group_id << "}";
  WriteJson(resp, 0, "ok", data.str());
}

void HttpApiServer::handleChatroomOnlineCount(const HttpRequest& req,
                                              HttpResponse* resp) {
  if (req.method() != HttpRequest::kPost) {
    WriteJson(resp,
              405,
              "only POST allowed",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  // 统一鉴权
  int64_t auth_user_id = AuthenticateRequest(req, resp);
  if (auth_user_id <= 0) return;

  nlohmann::json j;
  if (!ParseJsonBody(req.body(), &j)) {
    WriteJson(resp,
              400,
              "invalid json body, expect {\"room_id\":...}",
              "{}",
              HttpResponse::k400BadRequest);
    return;
  }

  int64_t room_id = 0;
  try {
    if (!j.contains("room_id")) {
      WriteJson(resp,
                400,
                "room_id required",
                "{}",
                HttpResponse::k400BadRequest);
      return;
    }
    room_id = j.at("room_id").get<int64_t>();
  } catch (const nlohmann::json::exception& e) {
    WriteJson(resp,
              400,
              "invalid json fields in chatroom online_count",
              "{}",
              HttpResponse::k400BadRequest);
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
      LogError("ListRoomMembers failed when fallback online count: " + err);
    }
  }
  std::ostringstream data;
  data << "{\"room_id\":" << room_id
       << ",\"online_count\":" << count << "}";
  WriteJson(resp, 0, "ok", data.str());
}

}  // namespace sparkpush


