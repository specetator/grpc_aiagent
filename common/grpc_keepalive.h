#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>

namespace sparkpush {

// HTTP/2 ping：对端进程退出后 Read 不能一直挂死。服务端必须允许这个 ping
// 频率，否则会被 GOAWAY。
inline grpc::ChannelArguments MakeGrpcKeepaliveChannelArgs() {
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    return args;
}

inline std::shared_ptr<grpc::Channel> CreateKeepaliveChannel(
    const std::string& target) {
    return grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(),
                                     MakeGrpcKeepaliveChannelArgs());
}

inline void ApplyGrpcKeepaliveServerArgs(grpc::ServerBuilder* builder) {
    if (!builder) return;
    builder->AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    builder->AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
    builder->AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    builder->AddChannelArgument(GRPC_ARG_HTTP2_MAX_PING_STRIKES, 0);
    builder->AddChannelArgument(
        GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000);
}

}  // namespace sparkpush
