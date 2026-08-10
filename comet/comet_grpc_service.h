#pragma once

#include "comet_server.h"

#include "spark_push.grpc.pb.h"

#include <grpcpp/grpcpp.h>

namespace sparkpush {

// CometService 的实现：被 job 通过 gRPC 调用，将下行消息推送到本机连接。
// 注意：不做跨 comet 节点转发，调用方需根据 comet_id 选择目标实例。
class CometServiceImpl final : public sparkpush::CometService::Service {
public:
    explicit CometServiceImpl(CometServer* server);

    // gRPC 方法：向本机 comet 管理的连接推送消息。
    // - 当 targets 非空时按 user_id 精确推送。
    // - 否则根据 message.session_id 约定执行房间 / 全服广播。
    ::grpc::Status PushToComet(
            ::grpc::ServerContext* context,
            const ::sparkpush::PushToCometRequest* request,
            ::sparkpush::PushToCometReply* response) override;

    ::grpc::Status PushDeliveryAck(
            ::grpc::ServerContext* context,
            const ::sparkpush::PushDeliveryAckRequest* request,
            ::sparkpush::SimpleReply* response) override;

    // Job 为每个 Comet 复用一条双向客户端流；每个 request 都返回对应 reply。
    ::grpc::Status PushStream(
            ::grpc::ServerContext* context,
            ::grpc::ServerReaderWriter<::sparkpush::PushToCometReply,
                                       ::sparkpush::PushToCometRequest>* stream)
            override;

private:
    size_t ProcessPushRequest(const ::sparkpush::PushToCometRequest& request);
    // 外部注入的 comet 服务实例，生命周期由调用方管理。
    CometServer* server_;
};

}  // namespace sparkpush

