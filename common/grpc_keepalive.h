#pragma once

#include <grpcpp/grpcpp.h>

#include <memory>
#include <string>
#include "internal_rpc.h"

namespace sparkpush {

// HTTP/2 ping：对端进程退出后 Read 不能一直挂死。服务端必须允许这个 ping
// 频率，否则会被 GOAWAY。
//
// 为什么这个头文件对 Comet/Job 至关重要：两者的双向流都靠阻塞式
// Read 等 reply。TCP 默认 keepalive 是 2 小时，如果 Logic/Comet 进程被
// kill -9 或容器被停，对端的 Read 会挂死几小时——表现为客户端气泡
// 永远停在「发送中」。这里把探测周期压到 10s + 5s 超时，最多 15s
// 就能发现死连接并触发重连。
//
// 各参数的含义：
//   KEEPALIVE_TIME_MS 10000            空闲 10s 就发 ping
//   KEEPALIVE_TIMEOUT_MS 5000          ping 后 5s 无应答则判定断开
//   KEEPALIVE_PERMIT_WITHOUT_CALLS 1   没有在飞 RPC 时也发（流空闲期同样需要探活）
//   HTTP2_MAX_PINGS_WITHOUT_DATA 0     0 = 不限制；不设的话 gRPC 默认会在
//                                      收到过多「无数据 ping」后直接 GOAWAY 断连
inline grpc::ChannelArguments MakeGrpcKeepaliveChannelArgs() {
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    return args;
}

// 创建带 keepalive + 角色鉴权拦截器的客户端通道。
//
// 这是所有内部 gRPC 客户端的**唯一**入口（Comet->Logic、Job->Comet、
// hermes_bridge->inference 都走这里），因此安全约束只需在这一处保证。
//
// 参数 role 决定附上哪个密钥，必须是 comet/job/admin 之一；
// RpcAuthFactory 构造时会做 RequireRpcKey 校验，密钥没配就抛异常。
inline std::shared_ptr<grpc::Channel> CreateKeepaliveChannel(
    const std::string& target, const std::string& role) {
    std::shared_ptr<grpc::ChannelCredentials> credentials;
    const auto ca = RpcReadFile("SPARK_RPC_TLS_CA");
    if (!ca.empty()) {
        grpc::SslCredentialsOptions options;
        options.pem_root_certs = ca;
        credentials = grpc::SslCredentials(options);
    } else {
        // Never send workload or user credentials over a remote plaintext channel.
        // 没配 CA 时只允许连回环地址。与服务端 InternalServerCredentials
        // 的约束对称：两端任一侧把非回环明文通道都拦住，就不会出现
        // 「服务端开了 TLS 但客户端走明文」或反之的错配。
        //
        // 这里特别危险的原因：拦截器会把 RPC 密钥放进 metadata，
        // 明文链路下密钥直接可被嗅探，进而完全冒充该角色。
        if (target.rfind("127.0.0.1:", 0) != 0 && target.rfind("localhost:", 0) != 0 &&
            target.rfind("[::1]:", 0) != 0)
            throw std::runtime_error("remote internal RPC requires SPARK_RPC_TLS_CA");
        credentials = grpc::InsecureChannelCredentials();
    }
    std::vector<std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>> factories;
    factories.emplace_back(new RpcAuthFactory(role));
    return grpc::experimental::CreateCustomChannelWithInterceptors(
        target, credentials, MakeGrpcKeepaliveChannelArgs(), std::move(factories));
}

// 服务端侧的 keepalive 参数。必须与客户端配套，否则客户端发的
// 频繁 ping 会被服务端当成滥用而计入 strike，累计后发 GOAWAY 断连。
//
// 两个容易配错的项：
//   HTTP2_MAX_PING_STRIKES 0                     0 = 容忍任意频率的 ping
//   MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS 5000  允许最快 5s 一个空闲 ping，
//                                                必须 <= 客户端的 KEEPALIVE_TIME_MS(10s)
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
