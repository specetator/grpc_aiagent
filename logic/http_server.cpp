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

// =====================================================================
// Logic 的 HTTP 适配层（监听 9101）。本文件是全工程最大的一个，
// 但它本质上只有三件事：解请求 -> 调 service -> 写响应。
// 任何看起来像“业务规则”的代码都不应该出现在这里。
//
// 【响应包络】
//   所有 JSON 接口统一回 {"code":.., "message":.., "data":{..}}，code=0 表示成功。
//   **HTTP 状态码与 body.code 不一一对应**，这是本文件最容易踩的坑：
//     WriteServiceError 只把 400/401/403/404 映射到同名 HTTP 码，
//     其余（500/503/409/413/429）一律以 **HTTP 200** 返回，真实码只在 body 里。
//   这是历史兼容（见下面 WriteServiceError 上方的英文注释）：
//   早期客户端只看 body.code，把存储故障改成 HTTP 500 会让它们误判为
//   “网关挂了”而重试风暴。新客户端应该一律读 body.code。
//
// 【鉴权】
//   requireUser  -> Authenticate(kHttpUser)，输出 user_id 当 actor
//   requireAdmin -> AuthenticateAdmin（id 必须 == kAdminUserId），**不输出 id**，
//                   handler 直接把常量 kAdminUserId 当 actor 传下去
//   actor 永远不从请求 JSON 里取。若改成“从 body 里读 actor_user_id”，
//   就等于把鉴权交给了客户端。service 层的 RequireAdmin(actor) 只是第二道保险。
//
// 【异常安全】
//   handler 里大量使用 j.at("x").get<int64_t>()，缺字段或类型不对会抛。
//   兼容网在构造函数里的回调包装：nlohmann 异常 -> 400，
//   其余 std::exception -> 500 + 日志。因此 handler 里可以不写 try/catch；
//   写了的（如 handleRoomAgent）只是为了给出更准确的错误文案。
//
// 【并发】
//   io_threads 可以 > 1，handler 会在多个 IO 线程上并发跑。
//   本文件不能引入任何静态可变状态（下面的路由表是 static **const**，
//   靠 C++11 的 magic statics 保证只初始化一次且初始化本身线程安全）。
// =====================================================================

using namespace muduo;
using namespace muduo::net;

namespace {

// 解析 x-www-form-urlencoded（旧实现，保留以兼容可能的其他调用）
//
// 现状（诚实标注）：**全仓库没有任何调用方**，只有这里的定义。
// 所有接口都已经改成收 JSON 了。而且它也不完整：没有做 URL 解码
//（%xx 与 '+' 都不会被还原），因此即使拿去用也是错的。
// 阅读时可以直接跳过，也不要拿它当参考实现。
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
//
// 用 try/catch 而不是 json::parse(body, nullptr, false)：后者失败时返回
// discarded 值，调用方得记得查 is_discarded()，漏一处就会在后面抛。
// 抛异常版本把“合法性”收敛到了返回值上，反而更难用错。
// 代价是必须有人接异常——构造函数里的回调包装就是那个兜底。
// HTTP field names are case-insensitive. The bundled request map preserves
// spelling; browsers/proxies commonly send lowercase authorization.
std::string AuthorizationHeader(const HttpRequest& request) {
    std::string value; bool seen=false;
    for (const auto& header : request.headers()) {
        auto name=header.first;
        for (auto& c:name) if(c>='A' && c<='Z') c=static_cast<char>(c-'A'+'a');
        if(name!="authorization")continue;
        if(seen)return {}; // Ambiguous credentials fail closed.
        seen=true;value=header.second;
    }
    return value;
}
bool ParseJsonBody(const std::string& body, nlohmann::json* out) {
    if (!out) return false;
    // 空 body 提前拒：json::parse("") 会抛“unexpected end of input”，
    // 而空 body 绝大多数情况不是攻击而是客户端忘了带请求体，
    // 提前返回可以避免在日志里刷一堆没意义的 LOG_ERROR。
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
//
// 为何 `*` 在这里是可接受的（以及什么时候就不行了）：
//   本项目的凭证走 **Authorization 头**，不是 Cookie。浏览器不会自动
//   带上 Authorization，而 `Access-Control-Allow-Origin: *` 也不允许
//   credentials 模式，因此恶意站点无法“借着用户的登录态”调本接口。
//   它只能用自己已经拿到的 token（那就是 token 泄露问题，不是 CORS 问题）。
//   但若以后改成 Cookie 会话，必须立刻换成白名单式 Origin +
//   Allow-Credentials，否则就是一个真的跨站请求伪造漏洞。
// Max-Age 86400：让浏览器缓存预检结果一天，避开每个请求都多一次 OPTIONS。
void AddCORSHeaders(HttpResponse* resp) {
    if (!resp) return;
    resp->addHeader("Access-Control-Allow-Origin", "*");
    resp->addHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    resp->addHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    resp->addHeader("Access-Control-Max-Age", "86400");
}

// 使用 nlohmann/json 解析 account/password
//
// 两个字段都先查 contains 再查 is_string，而不是直接 get：
// get<std::string>() 对非字符串会抛 type_error，虽然最终也会被
// 回调包装的 catch 接住并回 400，但那样会在日志里多一条 LOG_ERROR。
// 登录/注册是最高频的两个接口，客户端传错格式很常见，不应该当异常事件记。
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
//
// 与 ParseJsonAccountPassword 的唯一区别就是 name 可选，且空时回退到
// account（让“只填账号密码”的最简注册流程也能得到一个可展示的昵称）。
// 不合并成一个带可选参数的函数，是为了让两个接口各自“读哪几个字段”
// 在代码上一目了然。
// 先清空三个输出再解析：失败时调用方拿到的是空串而不是上一轮的残留值。
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

// 把客户端传的状态值（数字或字符串）归一到 kUserStatus* 常量。
//
// 两种形式都收是为了前端方便：管理后台下拉框传 "disabled" 这种字符串，
// 而脚本/测试传 1/2/3。但两条路最终都落到同一个枚举上，
// 不会让 service 层看到两种表示。
//
// 数字分支的范围校验 [kUserStatusActive, kUserStatusDeleted] 即 [1,3]：
// 0 不是合法状态（它意味着“未设置”），而 protobuf/JSON 里缺字段时
// 默认值恰好是 0，因此这个下界同时拦住了“忘传 status”。
// 未知值一律返回 false（fail-closed），不会静默降级成 active。
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

// 写一个统一包络的 JSON 响应。
//
// data_json 收的是**已序列化的字符串**而不是 nlohmann::json，这是个
// 有意的取舍：调用方已经拼好了 json 对象，再传对象过来就得多一次拷贝；
// 传字符串则可以直接 std::move 进包络。
// 代价是得多一次 parse（下面），以及“传进来一个非法 JSON 串”这种
// 可能——此时降级成空对象并记 LOG_ERROR，而不是让整个响应失败。
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
        // 不重抛：宁可回一个 data 为空的响应，也不要让一个序列化 bug
        // 变成客户端看到的 500（此时业务其实已经做成了）。
        LOG_ERROR << "WriteJson received invalid data JSON";
    }
    nlohmann::json body;
    // code/message 放在顶层而不是嵌在 data 里：客户端可以在不关心
    // 具体业务结构的情况下先判成败。
    body["code"] = code;
    body["message"] = message;
    body["data"] = std::move(data);
    resp->setBody(body.dump());
}

// HTTP mapping remains at the transport boundary; preserve legacy success-
// status responses for storage/unavailability failures.
//
// 译：HTTP 状态码的映射留在传输边界；对存储/不可用类失败，
// 保留旧的“成功状态码”响应行为。
//
// 只映射四个码的原因：这四个在 HTTP 语义上客户端会真的改变行为
//（401 弹登录、403 提示无权限、404 当不存在、400 当参数错）。
// 而 500/503 如果也映射过去，很多 HTTP 客户端库/代理会自动重试，
// 对一个“消息已入 Kafka 但落库超时”的请求重试就是重复发送。
// 因此宁可让它们看起来像 200，由业务码控制客户端行为。
//
// 409/413/429 也落到 200：它们不在上面的 if 链里。这是有意还是遗漏
// 已经无从考据，但改动它属于**协议变更**，必须同步改所有客户端，
// 不要顺手“修正”。
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

// 管理面接口的错误映射，与 WriteServiceError **故意不同**：
//   400/404/503 -> HTTP 400，其余 -> HTTP 200
// 把 503 归到 400 看起来奇怪，但管理后台的前端只区分“请求没成”与
// “请求成了”，而且它需要一个非 200 才能弹出错误提示框。
// 管理面没有“重试风暴”风险（人手点击），因此可以与用户面用不同策略。
void WriteAdminServiceError(HttpResponse* resp, const ServiceStatus& status) {
    WriteJson(resp, status.code, status.message, "{}",
              status.code == 400 || status.code == 404 || status.code == 503
                  ? HttpResponse::k400BadRequest
                  : HttpResponse::k200Ok);
}

}  // namespace

// 构造 HTTP API 服务器，注入业务依赖并注册回调
//
// 注意这里把 LogicHttpServices 里的引用**转存成指针**：因为房间 Agent
// 那两个本来就是可空指针，存成指针能让十个成员类型一致，handler 里
// 统一用 `if (!xxx_service_)` 判定。引用成员在构造时已经保证非空。
//
// setThreadNum 里的 std::max(1, io_threads)：muduo 传 0 意味着“只用主
// EventLoop 线程”，而配置里写个 0 或负数是很常见的错误；钳到 1 避开
// “所有请求堵在主循环上、连定时器都跑不了”。实际默认值是
// cfg.logic_http_threads（config.h 里为 4）。
HttpApiServer::HttpApiServer(EventLoop* loop, const InetAddress& listenAddr,
                             LogicHttpServices services, int io_threads)
    : agent_connection_service_(services.agent_connections),
      user_service_(&services.users),
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
    server_.setThreadNum(std::max(1, io_threads));
    // 回调返回 bool，语义是“是否已同步处理完”（本工程用的是改造过的
    // muduo，上游版本的 HttpCallback 返回 void）：
    //   true   muduo 立即把 resp 序列化并发送
    //   false  本次不发送，由业务代码以后自己异步回（大文件上传场景）
    // 这里**恒返回 true**：所有 handler 都在 IO 线程上同步跑完。
    // 后果要心里有数：一个慢查询（例如管理面分页拉审计日志）会占满
    // 一个 IO 线程，而实时消息的上行走的是 gRPC 而不是这里，
    // 因此不会阻塞消息链路。若以后要把重查询改成异步，
    // 必须同时改这里返回 false 并在完成时自己写连接。
    server_.setHttpCallback(
        [this](const TcpConnectionPtr&, HttpRequest& req, HttpResponse* resp) -> bool {
            // 异常兼容网。两层 catch 的顺序不能颠倒：nlohmann 的异常类
            // 继承自 std::exception，先写 std::exception 就永远进不到 400 分支。
            try {
                onRequest(req, resp);
            } catch (const nlohmann::json::exception&) {
                // 单独归为 400：handler 里的 .at()/.get<>() 失败绝大多数是
                // 客户端少传了字段或类型不对，属于请求错误而不是服务端故障。
                // 不记日志：这类错误量大且无信息量，记了只会淹没真故障。
                WriteJson(resp, 400, "invalid json fields", "{}", HttpResponse::k400BadRequest);
            } catch (const std::exception&) {
                // 其余异常是真故障，必须记下 path 供排查。注意这里
                // **不能**把 e.what() 回给客户端：它可能包含 SQL 片段、
                // 文件路径或内部结构信息。
                LOG_ERROR << "HTTP handler failed path=" << req.path();
                WriteJson(resp, 500, "internal server error");
            }
            return true;
        });
}

// 启动 HTTP 服务器
void HttpApiServer::start() { server_.start(); }

// 统一入口：处理 OPTIONS/CORS 并分发业务路由
void HttpApiServer::onRequest(const HttpRequest& req, HttpResponse* resp) {
    using Handler = void (HttpApiServer::*)(const HttpRequest&, HttpResponse*);

    // 处理浏览器的 CORS 预检请求（OPTIONS），无需走具体业务路由
    //
    // 必须在路由表**之前**处理：预检请求不带 Authorization 头，
    // 如果让它落到具体 handler 就会被 requireUser 拦成 401，
    // 而浏览器拿到 401 后会直接放弃真实请求（并报一个很难看懂的
    // CORS 错误）。预检也不能回 404，否则同样会失败。
    if (req.method() == HttpRequest::kOptions) {
        // 对于预检请求，只需要返回 200 + CORS 头即可
        resp->setStatusCode(HttpResponse::k200Ok);
        resp->setContentType("text/plain; charset=utf-8");
        AddCORSHeaders(resp);
        return;
    }

    // 路由表：从 path 映射到成员函数指针
    //
    // static const + 局部变量：靠 C++11 的 magic statics 保证多线程下
    // 只初始化一次且初始化过程本身线程安全（io_threads 默认 4，
    // 多个 IO 线程会同时进本函数）。改成非 static 就会每请求重建
    // 一张 30 项的哈希表，白白浪费。
    //
    // 两个有意识的多对一映射：
    //   /api/admin/user/{status,disable,restore,delete} -> handleAdminSetUserStatus
    //     四个语义上等价于“把状态改成 X”，handler 内部按 path 推导目标状态，
    //     因此前端可以只传 user_id 而不传 status（disable/restore/delete 路径）。
    //   /api/chatroom/agent/{configure,task,checkpoint} -> handleRoomAgent
    //     三个端点共用鉴权与解包代码，只有最后一步分派不同。
    static const std::unordered_map<std::string, Handler> kRouteTable = {
        {"/api/spaces/create", &HttpApiServer::handleSpaces},
        {"/api/spaces/list", &HttpApiServer::handleSpaces},
        {"/api/spaces/discover", &HttpApiServer::handleSpaces},
        {"/api/users/lookup", &HttpApiServer::handleContactLookup},
        {"/api/spaces/update", &HttpApiServer::handleSpaces},
        {"/api/spaces/members", &HttpApiServer::handleSpaces},
        {"/api/spaces/invite", &HttpApiServer::handleSpaces},
        {"/api/spaces/invitations", &HttpApiServer::handleSpaces},
        {"/api/spaces/accept", &HttpApiServer::handleSpaces},
        {"/api/spaces/role", &HttpApiServer::handleSpaces},
        {"/api/spaces/remove", &HttpApiServer::handleSpaces},
        {"/api/spaces/leave", &HttpApiServer::handleSpaces},
        {"/api/login", &HttpApiServer::handleLogin},
        {"/api/register", &HttpApiServer::handleRegister},
        {"/api/agent/connections", &HttpApiServer::handleAgentConnections},
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
        {"/api/chatroom/tasks/list", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/tasks/create", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/tasks/update", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/agents/control", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/agents/list", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/workspace/list", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/workspace/set", &HttpApiServer::handleRoomAgent},
        {"/internal/room-agent/authorize", &HttpApiServer::handleWorkspaceAuthorization},
        {"/api/chatroom/agents/install", &HttpApiServer::handleRoomAgent},
        {"/api/chatroom/agents/uninstall", &HttpApiServer::handleRoomAgent},

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
    // /ready 与 /metrics 在路由表之前处理，因此它们既不需要鉴权、
    // 也不受 method 限制。两者都是运维接口：
    //   /ready    读 spark_push_service_ready 这个 gauge（由 main.cpp 在
    //             依赖全部就绪后置 1），不就绪时**同时**写 body.code=503
    //             和 HTTP 503。这里破例写真 HTTP 码是因为健康检查探针
    //             （docker/k8s/脚本）只看状态码，不会解 JSON。
    //   /metrics  Prometheus 文本格式，直接 RenderPrometheus()。
    // 部署注意：它们与业务 API 共用 9101 端口，因此 /metrics 是**未鉴权
    // 对内网暴露**的。里面没有消息正文，但有在线数、QPS、队列长度等
    // 运营指标，不应该直接暴露到公网。
    if (path == "/ready") {
        const bool ready = MetricsRegistry::Instance().GaugeValue("spark_push_service_ready") == 1;
        WriteJson(resp, ready ? 0 : 503, ready ? "ready" : "not ready");
        if (!ready) {
            // setStatusCode 之后再改 statusMessage：WriteJson 已经把状态码
            // 设成 200 了，这里要把它连同文案一起覆写成 503。
            resp->setStatusCode(static_cast<HttpResponse::HttpStatusCode>(503));
            resp->setStatusMessage("Service Unavailable");
        }
        return;
    }
    if (path == "/metrics") {
        resp->setStatusCode(HttpResponse::k200Ok);
        // version=0.0.4 是 Prometheus 文本格式的版本声明，不是本项目的
        // 版本；写错了 Prometheus 会拒收整个采集。
        resp->setContentType("text/plain; version=0.0.4; charset=utf-8");
        resp->setBody(MetricsRegistry::Instance().RenderPrometheus());
        return;
    }
    // 未知路径回 body.code=404 但 HTTP 200（WriteJson 的默认参数）。
    // 与 /ready 不同，这里不需要真 HTTP 404：它不是探针路径。
    auto it = kRouteTable.find(path);
    if (it == kRouteTable.end()) {
        WriteJson(resp, 404, "unknown path");
        return;
    }

    // 成员函数指针调用。注意 method（GET/POST）不在路由表里，
    // 而是每个 handler 自己开头判：因为同一个 path 在不同 method 下
    // 的错误文案不一样（例如 405 要说明只允许 POST）。
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
    // 同 handleChatroomOnlineCount：**要求登录但不使用身份**。
    // 知识库内容被当成已登录用户共享的只读资料（引用栏里点开
    // 就要能看原文），因此不做逐文档的权限判定。
    // 真正的防线在 KnowledgeService/knowledge_catalog：id 必须是
    // doc_<24 hex> 形式，因此无法用它做路径穿越或枚举任意文件。
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
    // 400 与 404 的分工由 KnowledgeService 定：id 形式非法 -> 400，
    // 形式合法但内容不存在 -> 404。本层不重新判定，直接透传。
    // 注意 WriteServiceError 会把 400/404 映射到同名 HTTP 码，
    // 这是本文件里少数几个真的会回非 200 的业务错误。
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
        //
        // 实情比这句注释还要再彻底一点（诚实标注）：下面解出的 user_id
        // **从未被使用过**。真正传给 DanmakuService::Send 的只有
        // (token, video_id, timeline_ms, text)，发送者身份完全由 service
        // 内部解 token 得出。这个安全属性是对的（不信任请求体），
        // 但留着一个赋了值却没人读的变量很容易让读者误以为它参与了校验。
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
        //
        // 译：旧版弹幕接口把 token 放在请求体里（而不是 Authorization 头），
        // 并且鉴权失败时返回 HTTP 400；结构重构期间不要改这个行为。
        //
        // 两个历史包袱都在这里：
        //   token 走 body  因为弹幕页面（web_demo/static/danmaku.html）是
        //                   早期写的，没有统一的请求头注入。安全上等价
        //                   （body 与头都在同一个 TLS 信道里），但会进
        //                   访问日志，因此不如放头里干净。
        //   401 也回 400  旧前端只看“是不是 400”来判断要不要重登。
        // 因此本 handler 不用 requireUser，也不用 WriteServiceError。
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
        //
        // 译：保留现有的登录/注册 HTTP 状态码约定。
        //
        // 三个条件都映射到 HTTP 400，其余（包括 401 密码错、503 服务不可用）
        // 都是 HTTP 200 + body.code。注意第三个条件看的是
        // status.**reason** 而不是 code：“账号被禁用/已删除”在 body 里是
        // 403 + kInactiveAccount，但在 HTTP 层面同样归到 400。
        // 不要把这三行“整理”成直接映射 status.code，那会改前端行为。
        WriteJson(resp, status.code, status.message, "{}",
                  status.code == 400 || status.code == 404 ||
                          status.reason == ServiceReason::kInactiveAccount
                      ? HttpResponse::k400BadRequest
                      : HttpResponse::k200Ok);
        return;
    }
    nlohmann::json data = {{"user_id", result.user_id}, {"token", result.token}};
    // 管理员登录不回 name：内置管理员账号**不在 user 表里**
    //（Login 里走的是“环境变量口令 + ConstantTimeEquals”分支），
    // 因此 result.name 本来就是空的。与其回一个空字符串让前端展示空白，
    // 不如不回，前端自己写“管理员”。
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
        // 注册只把 400（参数不合法）与 409（账号已存在）映射到 HTTP 400。
        // 409 归到 400 是因为旧前端把“注册失败”当一类处理，不区分原因；
        // 真实区分靠 body.code 与 body.message。
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

    int64_t actor = 0;
    if (!requireUser(req, resp, &actor)) return;
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

    // 本 handler 最关键的一行：**from_user_id 是被校验而不是被信任的**。
    //
    // 字段仍然要求客户端传（历史接口形状），但必须等于 Bearer token 解出
    // 的 actor，否则 403。少了这一行，任何人拿自己的 token 就能冒充
    // 任意用户发消息——而 from_user_id 直接决定了 session_id 与 sender_id。
    //
    // 注意这里回的是 **HTTP 200 + body.code=403**（WriteJson 默认参数），
    // 而不是真 HTTP 403，与上面 WriteServiceError 的行为一致。
    //
    // 后面传给 SendLegacyText 的是 actor 而不是 from_user：两者已证实相等，
    // 但用 actor 能把“不信任请求体”这个意图写在代码上。
    if (from_user != actor) {
        WriteJson(resp, 403, "sender does not match authenticated user");
        return;
    }
    const auto result = message_service_->SendLegacyText(actor, to_user, content);
    if (!result.status.ok()) {
        WriteServiceError(resp, result.status);
        return;
    }
    const auto& msg = result.message;
    // 手拼 JSON 字符串而不是用 nlohmann。
    //
    // 这里**恰好**是安全的：session_id 是 "s_<uid1>_<uid2>"、msg_id 是
    // "<session_id>-<seq>"，两者都由服务端用整数拼出，不可能包含引号
    // 或反斜杠。但这是一个靠上游不变量兜住的写法：若以后 session_id
    // 形式变了（例如引入自定义房间名），这里就会拼出非法 JSON。
    // 新代码应该用 nlohmann::json{{...}}.dump()，与本文件其余 handler 一致。
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
    // 两层体积限制，不要把它们看成一个：
    //   8 MiB  本行，限的是 **HTTP 请求体**（JSON 包络 + base64 文本）
    //   4 MiB  kMaxImageBytes（common/image_attachment.h），限的是**解码后的图片**
    // 两者差一倍是有道理的：base64 会把体积放大约 4/3，一张卡在上限的
    // 4 MiB 图片编成 base64 后约 5.3 MiB，再加 JSON 字段开销，8 MiB 是个
    // 宽裕但不离谱的外层上限。
    // 因此下面的错误文案写“4 MiB”而不是“8 MiB”：那才是用户真正需要
    // 知道的限制（图片太大），8 MiB 只是一个防滥用兜底。
    // 先查体积再 ParseJsonBody 也很重要：反过来就会先把一个 8 MiB 的
    // 字符串完整解析成 DOM（内存翻好几倍），再把它丢掉。
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
    // session_id 是**必填**的（为空时 Upload 直接回 400）：附件一创建就
    // 绑定到会话，不存在“先上传再绑定”的中间态。因此上传前
    // Upload 里会先跑一次 CheckAccess(create_single=true)，确认调用方
    // 真的能往那个会话里发东西。
    // name 缺省 "image"：客户端不传文件名也能上传，展示名由 service 兼顾。
    // 三个字段都用 body.value(key, 默认值) 而不是 at()：缺字段是
    // 客户端常见错误，应该回一个清楚的 400 而不是抛到外层兼容网。
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
    // 回 sha256 而不回图片内容：客户端拿它做本地缓存去重，
    // 也方便排查“上传的图与展示的图不一致”类问题。
    // 上传后真正的引用走消息正文里的 attachment 结构（见
    // common/image_attachment.h），本接口只负责把字节存下来并给一个 id。
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
    // 访问控制在 service 里做（上传者本人 或 所属会话成员），
    // 不在本层重复。本层只负责把已鉴权的 user_id 传下去。
    // 注意 404（不存在）与 403（无权）在 service 里被有意地
    // 统一成一句 "attachment is not visible"，避开用 id 做存在性探测。
    const auto status = attachment_service_->Read(user_id, id, &contents);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    const auto& stored = contents.metadata;
    const auto& bytes = contents.bytes;
    // 图片以 base64 嵌在 JSON 里回，而不是用二进制响应体 + Content-Type。
    //
    // 取舍：好处是客户端（包括 Android 端）只需一个 JSON 解析路径，
    // 不需要处理二进制流与 MIME 协商；代价是 base64 把体积放大约 4/3，
    // 一张卡在上限的 4 MiB 图片会变成 ~5.3 MiB 的响应体。
    // 因此它只适合“聊天里偶尔一张图”，不适合当图片 CDN 用。
    // 真要改二进制下载，必须同时避开“未鉴权的 <img src> 直接拉图”
    // 这个坑（浏览器不会给 img 请求带 Authorization 头）。
    std::string encoded;
    if (!EncodeBase64(bytes, &encoded)) {
        WriteJson(resp, 500, "failed to encode image");
        return;
    }
    // bytes 回的是**原始字节数**（不是 base64 后的长度），
    // 客户端可以拿它与 data.length 做一致性校验。
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
        // anchor_seq 与 limit 都是可选的：anchor 缺省 0 表示“取最新一页”，
        // limit 缺省 20。
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
    // 已知缺陷（诚实标注）：limit **在整条链路上都没有上限钳制**。
    //   handleHistory           直接用客户端传的值
    //   ConversationService::History  不钳（对比：Sync/SyncOffline 走 SyncLimit，钳到 [1,1000]）
    //   ConversationStore::GetHistory 不钳
    //   MessageDao::ListMessages      只把 <=0 归为 50，**没有上界**
    // 因此一个已登录用户可以发 {"session_id":"s_1_2","limit":100000000}
    // 把整个会话历史一次拉完，放大成一次大查询 + 一个大 JSON 响应。
    // 与 Sync 路径的对比很说明问题：那边除了 SyncLimit 还有 BoundPageBytes
    // 的 2 MiB 预算（为了不撞 gRPC 的 4 MiB 上限），而 HTTP 这边两道都没有。
    // 修法：在这里加一行 `limit = std::clamp(limit, 1, 1000);` 即可，
    // 不需要动下面三层。注意它不属于本次“只加注释”的范围，因此未修。
    //
    // 权限上没问题：History 走 CheckAccess，非会话成员会被 403，
    // 因此这里只是资源放大，不是数据泄露。
    std::vector<Message> msgs;
    const auto status = conversation_service_->History(requester_id, session_id,
                                                       anchor_seq, limit, &msgs);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    nlohmann::json messages = nlohmann::json::array();
    for (const auto& message : msgs) {
        // content 存的是一个 JSON 字符串，这里把它**解成对象**再放进响应，
        // 避开客户端拿到一个需要二次 parse 的转义字符串。
        // 解析失败时降级成原字符串（而不是报错）：早期数据里 content_json
        // 存的可能就是纯文本，那些行仍然应该能拉出来展示。
        // parse 的第三参 false = 不抛异常，因此这里不需要 try/catch。
        auto content = nlohmann::json::parse(message.content_json, nullptr, false);
        if (content.is_discarded()) content = message.content_json;
        messages.push_back({{"msg_id", message.msg_id}, {"client_msg_id", message.client_msg_id},
            {"msg_seq", message.msg_seq}, {"sender_id", message.sender_id},
            {"timestamp_ms", message.timestamp_ms}, {"msg_type", message.msg_type}, {"content", content}});
    }
    // 注意响应里**没有** has_more / next_seq：本接口是“向上翻历史”，
    // 客户端靠 anchor_seq 自己推进（拿本页最小 msg_seq 当下一次的 anchor）。
    // anchor_seq <= 0 在 DAO 里被当成“没有游标”，直接取最新一页
    //（不能把 0 当条件传下去，因为 msg_seq < 0 永远不成立，会回空页）。
    //
    // 与 SyncAfter（向下追增量，带 has_more/next_seq/safe_seq）是两套
    // 不同的契约，不要混用。两者返回给客户端的顺序都是 ASC（旧->新），
    // 但 SQL 方向相反：ListMessages 是 `msg_seq < anchor ORDER BY DESC LIMIT n`
    // 取完再 reverse（因为要的是 anchor 之前**最新的** n 条），
    // ListMessagesAfter 则是 `msg_seq > after ORDER BY ASC LIMIT n` 直接取。
    WriteJson(resp, 0, "ok", nlohmann::json{{"messages", messages}}.dump());
}

// 标记会话已读
//
// 【下面三个 handler 共用的 actor/user_id 双参数模式】
// handleMarkRead / handleUnread / handleSingleSessionList 都是：
//   authenticated_id  从 Bearer token 解出，**可信**
//   user_id           从请求体里读，**不可信**
// 两个一起传给 service，由 service 做 `if (actor != user) return 403`。
//
// 为什么不直接忽略 body 里的 user_id、只用 authenticated_id？
// 因为那会静默改变接口语义：客户端传错 user_id 时会得到一个
// “看起来成功但其实是别人会话”的结果，而不是一个明确的 403。
// 显式校验能把“客户端逻辑有 bug”当场暴露出来。
//
// actor/user 一致性和会话成员资格由 ConversationService 校验。

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

// 查询未读数（actor/user_id 双参数模式见 handleMarkRead 上方的说明）
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
    // 手拼 JSON：只有一个整数字段，没有转义风险。
    // 未读数回的是数字而不是布尔：前端需要展示具体条数角标。
    std::ostringstream data;
    data << "{\"unread\":" << unread << "}";
    WriteJson(resp, 0, "ok", data.str());
}

// 列出单聊会话列表（actor/user_id 双参数模式见 handleMarkRead 上方的说明）
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
    std::vector<SingleConversationSummary> sessions;
    const auto status =
        conversation_service_->ListSingleSummaries(authenticated_id, user_id, &sessions);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    nlohmann::json data={{"sessions",nlohmann::json::array()}};
    for (const auto& s : sessions) {
        data["sessions"].push_back({{"session_id",s.session.id},
            {"peer_user_id",s.peer_user_id},{"peer_name",s.peer_name},
            {"last_msg_seq",s.session.last_msg_seq},{"unread_count",s.unread_count}});
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 拉取弹幕列表
//
// 【注意】本 handler **不需要鉴权**：全函数没有 requireUser，也不读 token。
// 与 handleDanmakuSend（必须带 token）形成对比。这是有意的：弹幕是
// 视频页的公开时间轴，页面需要在登录前就能先把已有弹幕渲染出来。
// 代价要心里有数：video_id 是可枚举的（没有限流），因此本接口能被
// 当成一个免费的数据库读取放大器。若以后弹幕里出现个人信息，
// 必须补上鉴权。
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
    //
    // from_ms/to_ms 的负值被钳到 0（而不是报错）：timeline_ms 是视频内
    // 偏移，负值无意义，钳到 0 等价于“从头开始”，比拒请求友好。
    // DanmakuService::List 里还会再钳一次（std::max<int64_t>(0, ...)），
    // 两层重复是因为 service 也被 gRPC/内部路径调，不能假定调用方已钳。
    int64_t from_ms = 0;
    int64_t to_ms = 0;
    // 已知缺陷（与 handleHistory 同类）：2000 只是**默认值**，不是上限。
    // 客户端传 limit=100000000 会直接传到 DanmakuDao::ListDanmaku，
    // 而那里也只处理了 limit <= 0（归为 5000），同样没有上界。
    // 修法与 handleHistory 一样：在这里 clamp 到 [1, 2000] 即可。
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
        // content_json 存的是完整消息包络 {"content":{"text":...}, ...}，
        // 而弹幕时间轴只需要纯文本，因此这里把它投影成 {timeline_ms, text}。
        // 不回原始包络的理由与 Requests() 不回 payload 相同：里面带着
        // sender_id / session_id 等与渲染无关的字段，没必要暴露给
        // 一个未鉴权的接口。
        // 解析失败（或字段缺失）时 text 保持空串而**不报错**：
        // 弹幕是展示型数据，一条坏了不应该让整页拉不出来。
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
    //
    // 回显 from_ms/to_ms/limit 是为了让客户端确认服务端**实际用的**参数
    //（例如负值被钳到 0 之后），而不是只能靠自己算。
    // has_more 是个**启发式**：它没有像 SyncAfter 那样多拉一条来探测，
    // 而是看“本页是不是刚好填满”。因此恰好返回 limit 条时会误报
    // has_more=true，客户端再拉一次会得到空页。对弹幕这个场景可接受
    //（多一次空查询而已），但不要拿它当精确分页信号用。
    resp_j["from_ms"] = from_ms;
    resp_j["to_ms"] = to_ms;
    resp_j["limit"] = limit;
    resp_j["has_more"] = (static_cast<int>(list.size()) >= limit && limit > 0);

    WriteJson(resp, 0, "ok", resp_j.dump());
}

// 列出用户订阅的聊天室（actor/user_id 双参数模式见 handleMarkRead 上方）
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
        // session_id 与 room_id 都回：前者是消息层的会话标识（"r_<id>"），
        // 后者是房间业务 id。客户端拉历史/同步要用 session_id，
        // 而调房间类接口（join/agent）要用 room_id，两个都得给。
        // last_msg_seq 让客户端能算未读而不需要再拉一次全量历史。
        data["rooms"].push_back({{"session_id", room.session.id},
                                 {"room_id", room.session.group_id},
                                 {"name", room.name},
                                 {"last_msg_seq", room.session.last_msg_seq}});
    }
    WriteJson(resp, 0, "ok", data.dump());
}

// 房间共享 Agent 的三个端点（configure / task / checkpoint）共用本 handler。
//
// 合并的理由：三个端点的鉴权、可用性判定、解包与错误处理完全相同，
// 只有最后一步分派不同。分开写会把同一段鉴权代码复制三遍，
// 而鉴权代码一旦分叉就是安全漏洞。
//
// 本 handler 自己包了一层 try/catch 而不靠外层兼容网，有两个理由：
//   1. 文案更准："invalid room Agent fields" 比笼统的 "invalid json fields"
//      能直接告诉前端是 configure/checkpoint 的字段不对。
//   2. 它用 `throw std::invalid_argument("body")` 把“body 不是合法 JSON”
//      也收进了同一个 catch，避开了重复的错误分支。
// 权限判定全在 service 里：Configure 只让房间拥有者过，Checkpoint 让
// 拥有者或 developer_user_ids 里的人过，View 只要求成员。本层不重复。
void HttpApiServer::handleWorkspaceAuthorization(const HttpRequest& req,HttpResponse* resp) {
    if(req.method()!=HttpRequest::kPost){WriteJson(resp,405,"only POST allowed");return;}
    if(!room_agent_service_){WriteJson(resp,503,"room Agent unavailable");return;}
    const auto authorization=req.getHeader("Authorization");
    const auto key=authorization.rfind("Bearer ",0)==0?authorization.substr(7):std::string();
    nlohmann::json input,output;
    if(!ParseJsonBody(req.body(),&input)){WriteJson(resp,400,"invalid JSON");return;}
    const auto status=room_agent_service_->AuthorizeWorkspace(key,input,&output);
    if(!status.ok()){WriteServiceError(resp,status);return;}
    WriteJson(resp,0,"ok",output.dump());
}

void HttpApiServer::handleRoomAgent(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int64_t actor = 0;
    if (!requireUser(req, resp, &actor)) return;
    // 两个 service 指针任一为空就 503：房间 Agent 是可选特性，
    // 未启用时 main.cpp 不会把它们放进 LogicHttpServices。
    // 这里是本文件里**唯一**一处置真 HTTP 503 的业务错误
    //（/ready 不算，那是探针），因为“特性未开启”对前端而言是部署
    // 状态而不是请求结果，需要一个能区分于 200 的信号来隐藏入口。
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
        // 按 path 分派。注意 else 兜底到了 View：
        // 路由表里只有三个 agent 路径能进本 handler，因此走到 else 的
        // 只可能是 /api/chatroom/agent/task。写成 else 而不是显式比路径，
        // 意味着**新增路由表项时忘改这里会静默变成 View**（一个只读操作），
        // 方向上安全但难以发现；新增端点时记得同时改这里。
        if(req.path()=="/api/chatroom/tasks/list")status=room_agent_service_->Tasks(actor,room,&output);
        else if(req.path()=="/api/chatroom/tasks/create")status=room_agent_service_->CreateTask(actor,room,input,&output);
        else if(req.path()=="/api/chatroom/tasks/update")status=room_agent_service_->UpdateTask(actor,room,input,&output);
        else if(req.path()=="/api/chatroom/workspace/list")status=room_agent_service_->Workspace(actor,room,&output);
        else if(req.path()=="/api/chatroom/workspace/set")status=room_agent_service_->SetWorkspace(actor,room,input,&output);
        else if(req.path()=="/api/chatroom/agents/control")status=room_agent_service_->Control(actor,room,input,&output);
        else if(req.path()=="/api/chatroom/agents/list") status=room_agent_service_->Agents(actor,room,&output);
        else if(req.path()=="/api/chatroom/agents/install" || req.path()=="/api/chatroom/agents/uninstall") {
            status=room_agent_service_->Install(actor,room,input,req.path()=="/api/chatroom/agents/install");
            output=nlohmann::json::object();
        }
        else if (req.path() == "/api/chatroom/agent/configure")
            status = room_agent_service_->Configure(actor, room, input, &output);
        else if (req.path() == "/api/chatroom/agent/checkpoint")
            // revision 与 patch 都直接用 at()：Checkpoint 的乐观锁要求
            // 客户端必须带上它看到的 revision，缺字段就应该报 400 而不是
            // 默认成 0（那会被当成“创建”，与语义不符）。
            status = operator_task_service_->Checkpoint(actor, room, input.at("revision").get<int64_t>(),
                                                       input.at("patch"), &output);
        else status = room_agent_service_->View(actor, room, &output);
        if (!status.ok()) { WriteServiceError(resp, status); return; }
        WriteJson(resp, 0, "ok", output.dump());
    } catch (const std::exception&) {
        WriteJson(resp, 400, "invalid room Agent fields", "{}", HttpResponse::k400BadRequest);
    }
}

void HttpApiServer::handleAgentConnections(const HttpRequest& req, HttpResponse* resp) {
    if (req.method() != HttpRequest::kPost) {
        WriteJson(resp, 405, "only POST allowed", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int64_t actor = 0;
    if (!requireUser(req, resp, &actor)) return;
    if (!agent_connection_service_) {
        WriteJson(resp, 503, "用户 Agent 接入尚未启用", "{}");
        return;
    }
    if (req.body().size() > 16384) {
        WriteJson(resp, 413, "connection request too large", "{}");
        return;
    }
    nlohmann::json input, output;
    if (!ParseJsonBody(req.body(), &input)) {
        WriteJson(resp, 400, "invalid JSON", "{}", HttpResponse::k400BadRequest);
        return;
    }
    const auto status = agent_connection_service_->Execute(actor, input, &output);
    if (!status.ok()) { WriteServiceError(resp, status); return; }
    WriteJson(resp, 0, "ok", output.dump());
}

// 加入聊天室（订阅关系）
//
// 【join / leave / unsubscribe 三个接口的区别——本文件最容易看错的一组】
// 三个 handler 的代码几乎一模一样（鉴权 -> 解 room_id/user_id -> 调 service），
// 但**语义完全不同**，而且没有一个会动 Redis 在线计数：
//   Join         公开空间写正式成员；私有空间先接受邀请。重复加入保留角色。
//   Leave        **空操作**，只校参数与身份。关闭页面不等于退订，
//                也不改变 WS 在线状态。保留它只为了兼容前端已有调用。
//   Unsubscribe  真正的退订：删 group_member 行。幂等（行不在也返回成功）。
//
// 在线存在（presence）是另一条路径：由 Comet 在收到 WS 控制消息时调
// ReportRoomJoin/ReportRoomLeave（Join 校验成员，然后维护 Redis 在线状态）。
// 把这两层混为一谈是读本项目最常见的误解，详见 room_service.h。
//
// 三个 handler 都用 actor/user_id 双参数模式（见 handleMarkRead 上方），
// Service 统一校验正式成员和空间管理权限。
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
        //
        // 译：房间不存在历来是“HTTP 200 + 业务码 404”。
        //
        // 注意这一行是在**推翻** WriteServiceError 的默认行为：
        // WriteServiceError 本来会把 404 映射到真 HTTP 404，而这里特意
        // 用 WriteJson（默认 HTTP 200）把它拉回 200。
        // 三个房间类 handler 都重复了这四行，是因为旧前端把 HTTP 404
        // 当成“接口不存在”而不是“房间不存在”，会弹错误的提示。
        // 不要“统一成 WriteServiceError”，那会改前端行为。
        if (status.code == 404)
            WriteJson(resp, status.code, status.message);
        else
            WriteServiceError(resp, status);
        return;
    }
    WriteJson(resp, 0, "ok");
}

// 离开聊天室（在线层）
//
// 注意上面这行旧注释里的“在线层”其实**不准确**（诚实标注）：
// RoomService::Leave 是个空操作，它既不动在线层（Redis）也不动订阅层
//（MySQL），只校参数与身份。真正的在线层变更是 ReportRoomLeave，
// 走的是 Comet -> Logic 的 gRPC，不是本 HTTP 接口。
// 三个接口的完整对比见 handleChatroomJoin 上方。
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
//
// 这才是真正会改持久状态的“退出”：删 group_member 行。
// 删了之后 ListSubscriptions 就不再返回这个房间，但历史消息仍在
// message 表里（会话本身不会被删）；重新 Join 就能再看到。
// 404 映射的说明见 handleChatroomJoin。
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

// 普通用户鉴权门。返回 false 时错误已经写进 resp，调用方直接 return 即可。
bool HttpApiServer::requireUser(const HttpRequest& req, HttpResponse* resp,
                                int64_t* user_id) {
    if (!user_id) {
        WriteJson(resp, 500, "user output is null");
        return false;
    }
    // 先置 0：下面任何一条失败路径都不会让调用方读到未初始化值。
    // 虽然约定是“false 就不看 user_id”，但防一下总比不防好。
    *user_id = 0;
    // 依赖不可用回 503 而不是 401：两者对客户端意义完全不同。
    // 401 会让前端清 token 并弹登录框，而 Redis 暂时挂了就把所有人
    // 踢下线是灾难性的。因此这里把“服务没准备好”与“你未登录”分开。
    if (!user_service_ || !user_service_->AuthenticationAvailable()) {
        WriteJson(resp, 503, "user authentication is unavailable");
        return false;
    }
    const std::string authorization = AuthorizationHeader(req);
    constexpr const char* kBearer = "Bearer ";
    // rfind(kBearer, 0) == 0 是 C++17 的 starts_with 惯用写法。
    // 第二个条件是 size() <= 7，即“只有 'Bearer ' 后面什么都没有”也拒，
    // 避开把一个空 token 交给 Authenticate（那边也会拒，但错误文案会更模糊）。
    // 注意 'Bearer' 后面那个空格是必需的："Bearerxyz" 不会被当成合法头。
    if (authorization.rfind(kBearer, 0) != 0 ||
        authorization.size() <= std::char_traits<char>::length(kBearer)) {
        WriteJson(resp, 401, "user bearer token required", "{}",
                  HttpResponse::k401Unauthorized);
        return false;
    }
    const std::string token =
        authorization.substr(std::char_traits<char>::length(kBearer));
    // audience 固定为 kHttpUser。与 gRPC 侧 VerifyToken 用的
    // kRealtimeUser 不同：那边会把内置管理员账号与 bot 账号 403 掉，
    // 这边不会，因此管理员 token 可以调普通用户接口。
    const auto status = user_service_->Authenticate(
        token, UserService::TokenAudience::kHttpUser, user_id);
    if (!status.ok()) {
        // 直接用 service 给的码：401（token 无效）/ 403（账号被禁用）
        // 的区分由 UserService 负责，本层不重新判定。
        WriteServiceError(resp, status);
        return false;
    }
    return true;
}

// 管理员鉴权门。与 requireUser 的三个关键差异：
//   1. **不输出 user_id**：因为管理员账号就是一个固定常量 kAdminUserId。
//      各 handler 直接把这个常量当 actor 传给 service，而**绝不**从
//      请求 JSON 里读。若改成“从 body 里取 admin_id”，就等于把鉴权
//      交给了客户端，任何人都能当管理员。
//   2. 先过 AdminEnabled() 门禁：管理功能可以通过配置整体关掉，
//      关掉后即使拿到正确 token 也是 503。
//   3. 鉴权失败一律回 **HTTP 400**（而不是 401/403），与 requireUser 不一致。
//      这是管理后台前端的历史约定（它靠非 200 弹错误框，但不区分原因），
//      不要“顺手修正”，那会弄坏前端。body.code 里仍然是真实的 401/403/503。
bool HttpApiServer::requireAdmin(const HttpRequest& req, HttpResponse* resp) {
    if (!user_service_ || !user_service_->AdminEnabled()) {
        WriteJson(resp, 503, "admin login is disabled");
        return false;
    }
    std::string authorization = AuthorizationHeader(req);
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
//
// 【安全注意】本 handler 与 handleAdminListChatroom 是全文件里仅有的
// 两处**把用户可控字符串手拼进 JSON** 的地方（下面的 name）。
// name 来自请求体，既没过长度限制也没过字符白名单
//（RoomService::Create 只判了 name.empty()），因此一个带双引号或
// 反斜杠的房间名就会拼出**非法 JSON**，前端解析失败。
// 对比：handleChatroomList（用户面）用 nlohmann::json 构造响应，
// name 会被正确转义——也就是说用户面做对了，管理面没做。
//
// 同一个 name 往下还会遇到第二个问题：GroupDao::CreateChatroom 把它
// 直接拼进 SQL 单引号里而**没过 mysql_real_escape_string**（见那里的
// 安全缺陷注释）。两道问题目前都只靠 requireAdmin 这一道门禁兜住。
//
// 修法（不属于本次“只加注释”的范围，因此未改）：
//   这里改成 nlohmann::json{{"room_id", group_id}, {"name", name}}.dump()
//   DAO 那边改成预编译占位符绑定，并给 name 加长度上限（schema 是 VARCHAR(128)）
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
//
// 手拼 JSON 的风险与 handleAdminCreateChatroom 相同，而且这里更严重：
// name 是从**数据库**里读回来的，因此一个已经存进去的恶意房间名
// 会让本接口对**所有管理员**持续返回非法 JSON（而不只是创建那一次）。
//
// offset/limit 没有上限钳制：GroupDao::ListChatrooms 只处理了
// offset<0 与 limit<=0（归为 50），同样没有上界。管理面风险较低
//（有 requireAdmin 门禁且人手点击），但与 handleHistory 属同一类问题。
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
//
// scope 缺省 "all"：不传就是全体广播。这是个很危险的默认值（一个
// 漏传字段的请求就会发给所有人），但它是旧契约；service 层会把
// 非法 scope 拒成 400，因此只能“漏传”而不能“传错”。
//
// 【词汇表转换】HTTP 用 "all"/"chatroom"，而内部（Kafka 事件与 gRPC
// Broadcast）用 "all"/"group"。转换在 MessageService::BroadcastAdmin 里做，
// 不在本层。两套词汇共存是历史原因，读代码时很容易把它当成 bug。
//
// 【已知缺陷】text 没有长度上限，而广播是**一对 N** 的放大：
// 一条请求会变成给每个在线连接的一次下推 + 一条 Kafka 事件。
// 一个几 MB 的 text 就能把下行带宽打满。当前只靠 requireAdmin 拦。
// 对比：普通消息路径上有完整的长度与字节预算校验。
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
    // 空 body 是合法的（与其他 handler 不同）：先初始化成一个空对象，
    // 只在 body 非空且解析失败时才报 400。因此 GET 风格的“拉第一页默认
    // 20 条”可以用一个空请求体完成，前端不需要凑一个 {}。
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
            // status == 0 是一个**特例**，表示“不按状态过滤”（查全部）。
            // 它必须单独先判，因为 ParseUserStatus 的合法区间是 [1,3]，
            // 0 会被它拒成 400。也就是说：“不传 status”与“传 status=0”
            // 等价，但“传 status=5”是错误。三种情况必须能区分开，
            // 否则前端就没法表达“清除状态筛选”这个动作。
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
    // total 是一个独立的 COUNT(*)，与上面的数据查询**不在同一个快照**里，
    // 因此并发注册时总数可能与实际页数轻微不一致（可接受，详见
    // UserDao::ListUsers 的注释）。回显 offset/limit 让前端能确认
    // 服务端实际用的分页参数。
    data["total"] = total;
    data["offset"] = offset;
    data["limit"] = limit;
    data["users"] = nlohmann::json::array();
    for (const auto& user : users) {
        // 状态同时回**名字与数字**两种形式：前端展示用名字（避开硬编码
        // 枚举映射），脚本/测试用数字。两者来自同一个 user.status，
        // 因此不可能不一致。
        nlohmann::json item = {
            {"user_id", user.id},         {"account", user.account},
            {"name", user.name},          {"status", UserStatusName(user.status)},
            {"status_code", user.status}, {"created_at", user.created_at},
        };
        // deleted_at 用 JSON null 而不是空字符串表示“未删除”：
        // 两者对前端意义不同（null 可以直接判 falsy 并避开展示一个
        // 空的时间框）。DAO 层把 MySQL 的 NULL 读成了空串，
        // 因此这里要把空串翻译回 null。
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
        // 按 path 推导目标状态（四个 path 共用本 handler，见路由表注释）：
        //   /disable -> 2（禁用）  /restore -> 1（激活）  /delete -> 3（软删）
        //   /status  -> 必须显式传 status 字段
        // 这样设计的价值：前端“禁用”按钮不需要知道状态枚举的数值，
        // 也就不可能因为传错数字而误删用户；而 /status 保留了给脚本用的
        // 通用入口。path 分支在前、body.status 在后的顺序也不能颠倒：
        // 若先读 body.status，/disable 传了个 status=active 就会变成“解禁”。
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
        // 本 handler 自己接异常（而不靠外层兼容网）：封禁/解禁是审计敏感
        // 操作，需要回“invalid user status fields”这种能指出具体问题的文案，
        // 而不是外层的笼统 400。reason 会被写进审计日志，因此类型错了必须拒。
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
        // 两个字段都用 at()（而不是 value() + 默认值）：改名接口上
        // “没传 name”是一个应该被明确拒的请求，而不是静默把名字改成空串。
        // 抛出的异常由本 handler 的 catch 翻成 "user_id/name required"。
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
    // 吊销该用户的所有登录 token。
    //
    // 效果与边界要分清楚：token 的权威在 Redis，吊销后**新的** HTTP/WS
    // 握手会被立即拒（Authenticate 永远绕过 2s 缓存，详见 user_service.h）；
    // 但**已经建立的** WebSocket 连接不会被本接口踢下线，那条路径走的是
    // GetActiveUser（带 2s 缓存）。真要立即断开连接需要额外的下推控制消息。
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
    // 同 handleAdminListUsers：空 body 合法，意为“拉第一页全部审计”。
    if (!req.body().empty() && !ParseJsonBody(req.body(), &body)) {
        WriteJson(resp, 400, "invalid json body", "{}", HttpResponse::k400BadRequest);
        return;
    }
    int offset = 0;
    int limit = 20;
    // target_user_id == 0 表示“不按对象过滤，查全部”（与上面 status==0
    // 是同一类约定）。limit 的上限钳制在 AuditLogDao 里做（硬性钳到
    // [1,100]），不在本层：钳在 DAO 能保证无论谁调都跑不掉这个上限。
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
//
// 注意 `(void)authenticated_id;`：本接口**要求登录但不使用身份**。
// 也就是说任意已登录用户都能查任意房间的在线人数（不需要是成员）。
// 这是有意的：在线人数被当成公开展示信息（房间列表页要在用户
// 加入之前就显示热度）。若以后认为它敏感，必须在这里补上成员校验。
// 保留 requireUser 而不是完全开放，是为了把匿名流量挡在外面。
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
    const auto status = room_service_->MemberOnlineCount(authenticated_id, room_id, &count);
    if (!status.ok()) {
        WriteServiceError(resp, status);
        return;
    }
    WriteJson(resp, 0, "ok",
              nlohmann::json{{"room_id", room_id}, {"online_count", count}}.dump());
}


void HttpApiServer::handleContactLookup(const HttpRequest& req, HttpResponse* resp) {
    if(req.method()!=HttpRequest::kPost) { WriteServiceError(resp,{400,"only POST allowed"}); return; }
    int64_t actor=0;
    if(!requireUser(req,resp,&actor))return;
    nlohmann::json body;
    if(!ParseJsonBody(req.body(),&body) || !body.is_object() || body.size()!=1 ||
       !body.contains("account") || !body["account"].is_string()) {
        WriteServiceError(resp,{400,"complete account required"}); return;
    }
    ContactSummary contact;
    auto status=user_service_->LookupContact(actor,body["account"].get<std::string>(),&contact);
    if(!status.ok()) { WriteServiceError(resp,status); return; }
    WriteJson(resp,0,"ok",nlohmann::json{{"user_id",contact.user_id},
        {"account",contact.account},{"name",contact.name}}.dump());
}

void HttpApiServer::handleSpaces(const HttpRequest& req, HttpResponse* resp) {
    if (req.method()!=HttpRequest::kPost) { WriteServiceError(resp,{400,"only POST allowed"}); return; }
    int64_t actor=0;
    if(!requireUser(req,resp,&actor)) return;
    nlohmann::json body;
    if(!ParseJsonBody(req.body(),&body) || !body.is_object()) { WriteServiceError(resp,{400,"JSON object required"}); return; }
    using Json=nlohmann::json;
    auto roleName=[](int role) { return role==kRoomOwner ? "owner" : role==kRoomAdmin ? "admin" : "member"; };
    auto groupJson=[](const ImGroup& g) { return Json{{"room_id",g.id},{"session_id","r_"+std::to_string(g.id)},
        {"name",g.name},{"description",g.description},{"visibility",g.visibility},{"owner_id",g.owner_id}}; };
    Json data=Json::object(); ServiceStatus status;
    try {
        const auto& path=req.path();
        const auto room=body.value("room_id",int64_t{0});
        if(path=="/api/spaces/create") {
            int64_t id=0;
            status=room_service_->CreateSpace(actor,body.value("name",std::string{}),body.value("description",std::string{}),body.value("visibility",std::string{"private"}),&id);
            data={{"room_id",id},{"session_id","r_"+std::to_string(id)}};
        } else if(path=="/api/spaces/list") {
            std::vector<SpaceSummary> rooms;
            status=room_service_->ListSpaces(actor,body.value("offset",0),body.value("limit",100),&rooms);
            data["spaces"]=Json::array();
            for(const auto& item:rooms) { auto row=groupJson(item.group); row["role"]=roleName(item.role); data["spaces"].push_back(row); }
        } else if(path=="/api/spaces/discover") {
            std::vector<ImGroup> rooms;
            status=room_service_->DiscoverSpaces(actor,body.value("offset",0),body.value("limit",50),&rooms);
            data["spaces"]=Json::array();
            for(const auto& room:rooms)data["spaces"].push_back(groupJson(room));
        } else if(path=="/api/spaces/invitations") {
            std::vector<SpaceInvitation> invitations;
            status=room_service_->Invitations(actor,&invitations); data["invitations"]=Json::array();
            for(const auto& item:invitations) { auto row=groupJson(item.group); row["invited_by"]=item.invited_by; data["invitations"].push_back(row); }
        } else if(path=="/api/spaces/members") {
            std::vector<SpaceMember> members;
            status=room_service_->Members(actor,room,&members); data["members"]=Json::array();
            for(const auto& item:members) data["members"].push_back({{"user_id",item.user_id},{"role",roleName(item.role)},{"account",item.account},{"name",item.name}});
        } else if(path=="/api/spaces/update") {
            status=room_service_->UpdateSpace(actor,room,body.value("name",std::string{}),body.value("description",std::string{}),body.value("visibility",std::string{"private"}));
        } else if(path=="/api/spaces/invite") status=room_service_->Invite(actor,room,body.value("user_id",int64_t{0}));
        else if(path=="/api/spaces/accept") status=room_service_->Accept(actor,room);
        else if(path=="/api/spaces/role") {
            const auto role=body.value("role",std::string{});
            status=room_service_->ChangeRole(actor,room,body.value("user_id",int64_t{0}),role=="member"?kRoomMember:role=="admin"?kRoomAdmin:-1);
        } else if(path=="/api/spaces/remove") status=room_service_->Remove(actor,room,body.value("user_id",int64_t{0}));
        else if(path=="/api/spaces/leave") status=room_service_->Unsubscribe(actor,actor,room);
        else status={404,"unknown space operation"};
    } catch(const Json::exception&) { status={400,"invalid space request fields"}; }
    if(!status.ok()) WriteServiceError(resp,status); else WriteJson(resp,0,"ok",data.dump());
}

}  // namespace sparkpush
