// ============================================================================
// logic 服务的 HTTP API 服务器
// 
// 提供面向客户端的 RESTful 接口，包括：
// - POST /api/register: 用户注册
// - POST /api/login: 用户登录
// 
// 技术要点：
// 1. 基于 muduo HTTP 服务器实现，支持高并发
// 2. 使用 JSON 格式进行请求和响应
// 3. 登录/注册成功后生成 token 存储到 Redis，返回给客户端
// 4. 支持 CORS 跨域请求，方便 Web 前端调用
// ============================================================================
#pragma once

#include <muduo/net/EventLoop.h>
#include <muduo/net/http/HttpRequest.h>
#include <muduo/net/http/HttpResponse.h>
#include <muduo/net/http/HttpServer.h>

#include "redis_store.h"
#include "user_dao.h"

namespace sparkpush {

// HTTP API 服务器类：处理用户认证相关的 HTTP 请求
class HttpApiServer {
   public:
    // 构造函数：注入事件循环、监听地址、用户 DAO 和 Redis 存储
    HttpApiServer(muduo::net::EventLoop* loop,
                  const muduo::net::InetAddress& listenAddr, UserDao* user_dao,
                  RedisStore* redis_store);

    // 启动 HTTP 服务器，开始监听端口
    void start();

   private:
    // HTTP 请求路由分发器：根据请求路径调用对应的处理函数
    void onRequest(const muduo::net::HttpRequest& req,
                   muduo::net::HttpResponse *resp);

    // 处理登录请求：验证账号密码，生成 token
    void handleLogin(const muduo::net::HttpRequest& req,
                     muduo::net::HttpResponse* resp);
    
    // 处理注册请求：创建用户，生成 token
    void handleRegister(const muduo::net::HttpRequest& req,
                        muduo::net::HttpResponse* resp);

    // 数据访问对象：操作用户表
    UserDao* user_dao_;
    
    // Redis 存储：管理 token 和路由信息
    RedisStore* redis_store_;
    
    // muduo HTTP 服务器实例
    muduo::net::HttpServer server_;
};

}  // namespace sparkpush
