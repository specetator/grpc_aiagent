#pragma once

#include "comet_server.h"

#include "spark_push.grpc.pb.h"

#include <grpcpp/grpcpp.h>

namespace sparkpush {

// CometService 的实现：被 job 通过 gRPC 调用，将下行消息推送到本机连接。
// 注意：不做跨 comet 节点转发，调用方需根据 comet_id 选择目标实例。
//
// 定位：这是 CometServer 的**薄适配层**，只做三件事：
//   1) 角色鉴权（AuthorizeInternalRpc，只接受 "job" 密钥）
//   2) 把 protobuf 请求转成 CometServer 的调用
//   3) 回填 reply 与指标
// 不持有任何业务状态，因此可以安全地放在栈上（见 app.cpp）。
//
// 鉴权为何必需：本端口（9105）能被访问就意味着能向任意在线用户
// 推送伪造消息。没有角色密钥校验，任何能连到这个端口的人都能
// 向全体客户端发送任意内容。
class CometServiceImpl final : public sparkpush::CometService::Service {
public:
    explicit CometServiceImpl(CometServer* server);

    // gRPC 方法：向本机 comet 管理的连接推送消息。
    // - 当 targets 非空时按 user_id 精确推送。
    // - 否则根据 message.session_id 约定执行房间 / 全服广播。
    //
    // 这是 Job 的 Unary 兜底路径（流不可用或流 reply 超时时使用）。
    ::grpc::Status PushToComet(
            ::grpc::ServerContext* context,
            const ::sparkpush::PushToCometRequest* request,
            ::sparkpush::PushToCometReply* response) override;

    // 向消息的发送者回 delivered_ack 帧。注意这不是已读回执。
    ::grpc::Status PushDeliveryAck(
            ::grpc::ServerContext* context,
            const ::sparkpush::PushDeliveryAckRequest* request,
            ::sparkpush::SimpleReply* response) override;

    // Job 为每个 Comet 复用一条双向客户端流；每个 request 都返回对应 reply。
    //
    // 这是下推的主路径。严格一请求一应答，因为 Job **只有收到 reply
    // 才提交 Kafka offset**；漏发一个 reply 会让 Job 阻塞到超时并重放。
    //
    // 进入本函数时会把 spark_push_comet_push_stream_ready 置 1，
    // 退出时置 0。启动脚本与健康检查都以它为门禁。
    ::grpc::Status PushStream(
            ::grpc::ServerContext* context,
            ::grpc::ServerReaderWriter<::sparkpush::PushToCometReply,
                                       ::sparkpush::PushToCometRequest>* stream)
            override;

private:
    // 幂等去重 + fanout 策略选择 + 投递 + 游标上报。
    // Unary 与 Stream 两条路径共用它，保证行为一致（尤其是幂等）。
    // 返回成功 send 的连接数。
    size_t ProcessPushRequest(const ::sparkpush::PushToCometRequest& request);
    // 外部注入的 comet 服务实例，生命周期由调用方管理。
    CometServer* server_;
};

}  // namespace sparkpush

