// ============================================================================
// logic 服务的 gRPC 接口实现
//
// 提供给 comet 服务调用的 RPC 接口，包括：
// 1. VerifyToken: 验证客户端 token，返回用户 ID 并记录路由信息
// 2. SendUpstreamMessage: 处理客户端上行消息（通过 comet 转发）
// 3. UserOffline: 处理用户下线通知，清理路由信息
//
// 设计要点：
// 1. 采用 gRPC 同步调用模式，适合低延迟的内网服务通信
// 2. 依赖 RedisStore 查询 token 和维护用户路由表
// 3. 依赖 KafkaProducer 将消息推送到 comet 节点
// 4. 通过 UserDao 访问用户数据库（当前未使用，保留用于扩展）
//
// 使用场景：
// - comet 收到 WebSocket 连接请求时，调用 VerifyToken 验证用户身份
// - comet 收到客户端消息时，调用 SendUpstreamMessage 转发给 logic 处理
// - 用户断开连接时，调用 UserOffline 清理路由信息
// ============================================================================
#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "kafka_producer.h"
#include "redis_store.h"
#include "room_dao.h"
#include "spark_push.grpc.pb.h"
#include "user_dao.h"

namespace sparkpush {

// LogicService gRPC 服务实现类
//
// 职责说明：
// 1. 实现 proto 定义的 RPC 接口
// 2. 处理 comet 节点的请求
// 3. 协调各个组件（Redis、Kafka、数据库）完成业务逻辑
//
// 继承关系：
// - 继承自 sparkpush::LogicService::Service（由 protobuf 生成）
// - 使用 final 关键字防止进一步继承
//
// 线程安全：
// - 所有 RPC 方法可能被多个线程并发调用
// - 依赖对象（UserDao、RedisStore、KafkaProducer）需要保证线程安全
class LogicServiceImpl final : public sparkpush::LogicService::Service {
   public:
    // 构造函数：初始化 gRPC 服务并注入依赖
    //
    // @param user_dao: 用户数据访问对象（当前未使用，保留用于扩展）
    // @param room_dao: 房间数据访问对象，用于房间消息成员校验和回源
    // @param push_producer: Kafka 生产者，用于发送推送消息到 comet
    // @param redis_store: Redis 存储对象，用于 token 验证和路由管理
    // @param connection_ttl_ms: 连接 TTL（毫秒），用于设置 Redis
    // 中路由信息的过期时间
    LogicServiceImpl(UserDao* user_dao, RoomDao* room_dao,
                     KafkaProducer* push_producer, RedisStore* redis_store,
                     int connection_ttl_ms);

    // 验证 token RPC 接口
    //
    // 功能说明：
    // 1. 验证客户端提供的 token 是否有效
    // 2. 返回对应的 user_id
    // 3. 在 Redis 中记录用户连接路由（user_id -> comet_id:conn_id）
    //
    // 调用场景：
    // comet 收到 WebSocket 连接请求时，从 URL 参数提取 token，
    // 调用此接口验证 token 并获取 user_id
    //
    // 参数说明：
    // @param context: gRPC 上下文对象
    // @param request: 请求参数，包含 token、comet_id 和 conn_id
    // @param response: 响应参数，返回 user_id 和错误信息
    // @return: gRPC 状态，返回 OK 表示 RPC 调用成功（不代表业务成功）
    ::grpc::Status VerifyToken(
        ::grpc::ServerContext* context,
        const ::sparkpush::VerifyTokenRequest* request,
        ::sparkpush::VerifyTokenReply* response) override;

    // 上行消息处理 RPC 接口
    //
    // 功能说明：
    // 1. 接收 comet 转发的客户端上行消息
    // 2. 生成消息 ID 和序号
    // 3. 构造 ChatMessage 对象
    // 4. 将消息推送到目标用户的 comet 节点
    //
    // 调用场景：
    // 客户端通过 WebSocket 发送消息给 comet，
    // comet 将消息通过此接口转发给 logic 处理
    //
    // 参数说明：
    // @param context: gRPC 上下文对象
    // @param request: 请求参数，包含发送方、接收方和消息内容
    // @param response: 响应参数，返回 ChatMessage 和错误信息
    // @return: gRPC 状态，返回 OK 表示 RPC 调用成功
    ::grpc::Status SendUpstreamMessage(
        ::grpc::ServerContext* context,
        const ::sparkpush::UpstreamMessageRequest* request,
        ::sparkpush::UpstreamMessageReply* response) override;

    // 用户下线通知 RPC 接口
    //
    // 功能说明：
    // 1. 接收 comet 发送的用户下线通知
    // 2. 从 Redis 中删除用户连接路由信息
    //
    // 调用场景：
    // 用户断开 WebSocket 连接时，comet 调用此接口通知 logic 清理路由
    //
    // 参数说明：
    // @param context: gRPC 上下文对象
    // @param request: 请求参数，包含 user_id、comet_id 和 conn_id
    // @param response: 响应参数，返回错误信息
    // @return: gRPC 状态，返回 OK 表示 RPC 调用成功
    ::grpc::Status UserOffline(::grpc::ServerContext* context,
                               const ::sparkpush::UserOfflineRequest* request,
                               ::sparkpush::SimpleReply* response) override;

   private:
    // 设置错误信息的辅助函数
    //
    // 功能说明：
    // 统一设置 protobuf ErrorInfo 的 code 和 message 字段
    //
    // 参数说明：
    // @param e: ErrorInfo 指针
    // @param code: 错误码，0 表示成功，非 0 表示各种错误
    // @param msg: 错误描述信息
    void SetError(ErrorInfo* e, int code, const std::string& msg);

    // 成员变量：依赖对象
    UserDao* user_dao_;  // 用户数据访问对象（保留未使用）
    RoomDao* room_dao_;  // 房间数据访问对象（用于房间消息成员校验和回源）
    KafkaProducer* push_producer_;  // 推送消息生产者
    KafkaProducer* persist_producer_;  // 持久化消息生产者（保留未使用）
    RedisStore* redis_store_;          // Redis 存储对象
    int connection_ttl_ms_{60000};  // 连接 TTL（毫秒），默认 60 秒
};

}  // namespace sparkpush