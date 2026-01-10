// ============================================================================
// HTTP API 服务器
//
// 提供业务层的 HTTP RESTful API 接口，主要功能包括：
// 1. 用户认证：登录、注册
// 2. 消息发送：单聊消息发送
//
// 技术栈：
// - 使用 muduo 网络库的 HttpServer 处理 HTTP 请求
// - 支持 CORS 跨域访问，便于前端开发调试
// - 使用 JSON 格式进行数据交互
// - 基于 token 的身份认证机制
//
// API 路由：
// - POST /api/login: 用户登录
// - POST /api/register: 用户注册
// - POST /api/message/send: 发送单聊消息
// ============================================================================
#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "kafka_producer.h"
#include "redis_store.h"
#include "room_dao.h"
#include "spark_push.pb.h"
#include "user_dao.h"

namespace sparkpush {

// HTTP API 服务器类
//
// 职责说明：
// 1. 接收和处理 HTTP 请求
// 2. 路由分发到对应的处理函数
// 3. 调用业务层（UserDao、RedisStore）完成业务逻辑
// 4. 返回 JSON 格式的响应
//
// 依赖注入：
// - UserDao: 用户数据访问对象，负责数据库操作
// - RedisStore: Redis 存储对象，负责 token 和路由管理
// - KafkaProducer: Kafka 生产者，负责消息推送
//
// 线程模型：
// - 基于 muduo 的事件循环（EventLoop），单线程处理 HTTP 请求
// - 所有 I/O 操作（数据库、Redis、Kafka）都是同步调用
class HttpApiServer {
   public:
    // 构造函数：初始化 HTTP 服务器并注入依赖
    //
    // @param loop: muduo 事件循环对象，管理网络事件
    // @param listenAddr: 监听地址和端口，如 0.0.0.0:9000
    // @param user_dao: 用户数据访问对象，用于查询和创建用户
    // @param room_dao: 房间数据访问对象，用于房间操作
    // @param redis_store: Redis 存储对象，用于 token 和路由管理
    // @param push_producer: Kafka 生产者，用于发送推送消息
    HttpApiServer(muduo::net::EventLoop* loop, const muduo::net::InetAddress& listenAddr,
                  UserDao* user_dao, RoomDao* room_dao, RedisStore* redis_store,
                  KafkaProducer* push_producer, int io_threads = 1);

    // 启动 HTTP 服务器
    // 调用此方法后，服务器开始监听端口并处理请求
    void start();

   private:
    // HTTP 请求统一入口
    //
    // 功能：
    // 1. 处理 CORS 预检请求（OPTIONS）
    // 2. 根据路径分发到具体的处理函数
    // 3. 对未知路径返回 404 错误
    //
    // @param req: HTTP 请求对象
    // @param resp: HTTP 响应对象
    void onRequest(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 从 HTTP 请求中提取用户 ID
    //
    // 功能：
    // 1. 从请求头中提取 token（支持 Authorization 和 Token 两种方式）
    // 2. 通过 Redis 验证 token 并获取 user_id
    //
    // 使用场景：
    // 需要身份认证的接口（如发送消息）调用此方法验证用户身份
    //
    // @param req: HTTP 请求对象
    // @param uid: 输出参数，返回用户 ID
    // @return: 验证成功返回 true，失败返回 false
    bool GetUserIdFromRequest(const muduo::net::HttpRequest& req, int64_t* uid);

    // 处理用户登录请求
    //
    // API 规格：
    // - 路径：POST /api/login
    // - 请求体：{"account": "admin", "password":
    // "0192023a7bbd73250516f069df18b500"}
    // - 响应体：{"code": 0, "message": "ok", "data": {"user_id": 1001, "token":
    // "tk-1001-1234567890", "name": "张三"}}
    //
    // 功能：
    // 1. 验证账号和密码（密码为 MD5 hash）
    // 2. 生成 token 并存储到 Redis
    // 3. 返回 user_id、token 和用户昵称
    //
    // @param req: HTTP 请求对象
    // @param resp: HTTP 响应对象
    void handleLogin(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 处理用户注册请求
    //
    // API 规格：
    // - 路径：POST /api/register
    // - 请求体：{"account": "user1", "password":
    // "5f4dcc3b5aa765d61d8327deb882cf99", "name": "用户1"}
    // - 响应体：{"code": 0, "message": "ok", "data": {"user_id": 1001, "token":
    // "tk-1001-1234567890", "name": "用户1"}}
    //
    // 功能：
    // 1. 检查账号是否已存在
    // 2. 创建新用户记录
    // 3. 生成 token 并存储到 Redis
    // 4. 返回 user_id、token 和用户昵称（注册即登录）
    //
    // @param req: HTTP 请求对象
    // @param resp: HTTP 响应对象
    void handleRegister(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 处理发送单聊消息请求
    //
    // API 规格：
    // - 路径：POST /api/message/send
    // - 请求头：Authorization: Bearer <token> 或 Token: <token>
    // - 请求体：{"msg_type": "text", "target_type": "single_chat", "target_id":
    // 1002, "content": "你好", "client_msg_id": "..."}
    // - 响应体：{"code": 0, "message": "ok", "data": {"msg_id":
    // "msgid:1001:1002-1", "msg_seq": 1, "session_id": "s_1001:1002"}}
    //
    // 功能：
    // 1. 验证用户身份（从 token 获取发送方 user_id）
    // 2. 验证请求参数（目标用户、消息内容等）
    // 3. 生成消息 ID 和序号
    // 4. 构造 ChatMessage 对象
    // 5. 调用 PostProcessSingleMessage 推送消息
    // 6. 返回消息 ID、序号和会话 ID
    //
    // @param req: HTTP 请求对象
    // @param resp: HTTP 响应对象
    void handleSendMessage(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);
    void handleSendMessageFast(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);
    void handleTestSendMessage(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);
    // =========================================================================
    // 房间相关 HTTP 接口（新增）
    //
    // 说明：
    // - 以下接口均为业务层 RESTful API 的路由入口，负责：
    //   1) 解析/校验请求参数（JSON、路径参数、查询参数）
    //   2) 身份认证（从 token 提取 user_id）
    //   3) 调用 RoomDao / RedisStore 执行业务逻辑
    //   4) 返回统一 JSON 响应：{"code":0,"message":"ok","data":...}
    //
    // 鉴权约定：
    // - 需要登录的接口会从请求头读取 token：
    //   - Authorization: Bearer <token>
    //   - Token: <token>
    // - 通过 Redis 校验 token 并得到 user_id（见 GetUserIdFromRequest）。
    // =========================================================================

    // 创建房间
    //
    // API 规格（示例）：
    // - 路径：POST /api/room/create
    // - 请求头：Authorization/Token（必需）
    // - 请求体：{"name":"房间名","description":"...","extra":{...}}
    // - 响应体：{"code":0,"message":"ok","data":{"room_id":123,"name":"房间名"}}
    //
    // 功能：
    // 1. 验证用户身份
    // 2. 校验房间名等参数
    // 3. 创建房间记录并返回 room_id
    void handleCreateRoom(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 获取房间列表
    //
    // API 规格（示例）：
    // - 路径：GET /api/room/list?page=1&page_size=20
    // - 请求头：可选（根据业务需要，若要返回“是否已加入”等信息则需要鉴权）
    // - 响应体：{"code":0,"message":"ok","data":{"total":10,"rooms":[...]}}
    //
    // 功能：
    // 1. 读取分页参数并查询房间列表
    // 2. 返回房间基本信息集合
    void handleRoomList(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 加入房间
    //
    // API 规格（示例）：
    // - 路径：POST /api/room/join
    // - 请求头：Authorization/Token（必需）
    // - 请求体：{"room_id":123}
    // - 响应体：{"code":0,"message":"ok","data":{"room_id":123}}
    //
    // 功能：
    // 1. 验证用户身份
    // 2. 校验 room_id 是否存在
    // 3. 写入成员关系（user <-> room）
    void handleJoinRoom(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 离开房间
    //
    // API 规格（示例）：
    // - 路径：POST /api/room/leave
    // - 请求头：Authorization/Token（必需）
    // - 请求体：{"room_id":123}
    // - 响应体：{"code":0,"message":"ok","data":{"room_id":123}}
    //
    // 功能：
    // 1. 验证用户身份
    // 2. 删除成员关系（user <-> room）
    // 3. 可选：处理房间空成员后的清理逻辑（视业务实现）
    void handleLeaveRoom(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 获取房间成员列表
    //
    // API 规格（示例）：
    // - 路径：GET /api/room/users?room_id=123
    // - 请求头：Authorization/Token（通常需要，防止泄露成员信息）
    // - 响应体：{"code":0,"message":"ok","data":{"room_id":123,"users":[...]}}
    //
    // 功能：
    // 1. 校验 room_id
    // 2. 查询并返回房间内用户列表（user_id、name 等）
    void handleRoomUsers(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 发送房间消息（群聊/房间内广播）
    //
    // API 规格（示例）：
    // - 路径：POST /api/room/message/send
    // - 请求头：Authorization/Token（必需）
    // - 请求体：{"room_id":123,"msg_type":"text","content":"你好","client_msg_id":"..."}
    // - 响应体：{"code":0,"message":"ok","data":{"msg_id":"...","msg_seq":1}}
    //
    // 功能：
    // 1. 验证用户身份，并校验发送者是否为房间成员
    // 2. 生成消息 ID/序号，构造消息体
    // 3. 推送到房间内在线成员（通常通过 Kafka/Comet 分发）
    void handleSendRoomMessage(const muduo::net::HttpRequest& req, muduo::net::HttpResponse* resp);

    // 成员变量：业务依赖对象
    UserDao* user_dao_;              // 用户数据访问对象
    RoomDao* room_dao_;              // 房间数据访问对象
    RedisStore* redis_store_;        // Redis 存储对象
    KafkaProducer* push_producer_;   // Kafka 生产者对象
    muduo::net::HttpServer server_;  // muduo HTTP 服务器对象
};

}  // namespace sparkpush
