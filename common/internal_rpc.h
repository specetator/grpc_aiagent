#pragma once
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include "security.h"

namespace sparkpush {

// =====================================================================
// 内部 RPC 的角色鉴权与传输安全。
//
// 要解决的问题：Logic 的 gRPC 端口（9100）一旦裸露，任何人都能调
// SendUpstreamMessage 冒充任意用户发消息。因此每个调用方必须持有
// 属于自己角色的预共享密钥。
//
// 三个角色各用**独立**密钥（而不是共享一个）：
//   comet / job / admin
// 这样某个角色密钥泄露时，可以单独轮换而不影响其他服务，
// 也能在日志里分辨出是谁在调。
//
// 密钥只从环境变量读（由 scripts/ 生成的 .env.rpc 注入，不进仓库）。

// 读环境变量，未设置时返回空串而不是 nullptr，方便后续直接做长度判定。
inline std::string RpcEnvironment(const char* name) {
    const char* value = std::getenv(name);
    return value ? value : "";
}

// 角色名 -> 对应环境变量里的密钥。未知角色返回空串，
// 后续的长度校验会把它拦下（fail-closed）。
inline std::string RpcKey(const std::string& role) {
    if (role == "comet") return RpcEnvironment("SPARK_RPC_COMET_KEY");
    if (role == "job") return RpcEnvironment("SPARK_RPC_JOB_KEY");
    if (role == "admin") return RpcEnvironment("SPARK_RPC_ADMIN_KEY");
    return {};
}

// 启动时的硬性门禁：密钥缺失或短于 32 字节直接抛异常，让进程起不来。
//
// 为什么是 32 字节：低于这个长度的共享密钥很容易被离线暴力破解，
// 而且弱密钥往往是「随手填个测试值」遗留下来的。宁可启动失败，
// 也不能带着弱凭证静默运行。
inline void RequireRpcKey(const std::string& role) {
    if (RpcKey(role).size() < 32)
        throw std::runtime_error("internal RPC credential missing or shorter than 32 bytes: " + role);
}

// 服务端校验入站请求的密钥。每个 RPC handler 开头都应该先调这个。
//
// 三个安全细节：
// 1) 服务端自己的密钥也先过长度校验。配置不全时宁可拒绝所有请求，
//    也不能退化成「不校验」。
// 2) equal_range 后要求恰好一个值（next(first) == second）：同时发
//    两个 x-spark-rpc-key 头是典型的请求走私/参数污染手法，直接拒。
// 3) 用 ConstantTimeEquals 而不是 ==：普通字符串比较会在首个不同
//    字节处提前返回，攻击者能通过测量响应时间逐字节猜出密钥。
inline grpc::Status AuthorizeInternalRpc(grpc::ServerContext* context,
                                         const std::string& role) {
    const auto key = RpcKey(role);
    if (!context || key.size() < 32)
        return {grpc::StatusCode::UNAUTHENTICATED, "internal RPC credential required"};
    const auto& metadata = context->client_metadata();
    const auto range = metadata.equal_range("x-spark-rpc-key");
    if (range.first == range.second || std::next(range.first) != range.second ||
        !ConstantTimeEquals(key, std::string(range.first->second.data(), range.first->second.size())))
        return {grpc::StatusCode::UNAUTHENTICATED, "invalid internal RPC credential"};
    return grpc::Status::OK;
}

// 读 TLS 材料文件。路径由环境变量给出（而不是写死），便于部署时挂载。
//
// 路径已配但文件读不了时选择**抛异常**而不是静默降级到明文：
// 运维本想用 TLS，因为路径写错而静默变成明文传输是更严重的事故。
inline std::string RpcReadFile(const char* variable) {
    const auto path = RpcEnvironment(variable);
    if (path.empty()) return {};
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error(std::string("cannot read TLS file configured by ") + variable);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

// 构造服务端凭证，并强制一条部署约束：非回环地址必须用 TLS。
//
// 判定逻辑的三层：
//   证书+私钥都有 -> 用 SSL
//   只有其中一个   -> 抛异常（半配置比不配置更危险）
//   都没有         -> 仅当 bind 是 127.0.0.1/localhost/::1 时才允许明文
//
// 最后一条是关键：单机教学环境下内部 RPC 走回环，明文可接受；
// 一旦绑到 0.0.0.0 或对外网卡，密钥与消息正文就会走在可被嗅探的
// 链路上，因此直接拒绝启动。
inline std::shared_ptr<grpc::ServerCredentials> InternalServerCredentials(const std::string& bind) {
    const auto certificate = RpcReadFile("SPARK_RPC_TLS_CERT");
    const auto key = RpcReadFile("SPARK_RPC_TLS_KEY");
    if (!certificate.empty() && !key.empty()) {
        grpc::SslServerCredentialsOptions options;
        options.pem_key_cert_pairs.push_back({key, certificate});
        return grpc::SslServerCredentials(options);
    }
    if (!certificate.empty() || !key.empty()) throw std::runtime_error("both RPC TLS certificate and key required");
    if (bind != "127.0.0.1" && bind != "localhost" && bind != "::1")
        throw std::runtime_error("non-loopback internal RPC requires TLS");
    return grpc::InsecureServerCredentials();
}

// 客户端拦截器：在每次 RPC 的初始 metadata 里自动附上角色密钥。
//
// 用拦截器而不是在每个调用点手动 AddMetadata：后者很容易漏，
// 而漏一处就是一个未鉴权的调用。拦截器把它变成通道级的不变量。
class RpcAuthInterceptor final : public grpc::experimental::Interceptor {
 public:
    explicit RpcAuthInterceptor(std::string key) : key_(std::move(key)) {}
    void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override {
        // 只在「即将发送初始 metadata」这个 hook 点注入一次。
        // 初始 metadata 每个 RPC 只发一次，因此不会重复追加。
        if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA))
            methods->GetSendInitialMetadata()->emplace("x-spark-rpc-key", key_);
        // 必须调 Proceed：拦截器链靠它推进，漏了会让 RPC 永远挂住。
        methods->Proceed();
    }
 private:
    std::string key_;
};

// 拦截器工厂。gRPC 对每个 RPC 都会调一次 CreateClientInterceptor，
// 因此拦截器对象是一次性的；密钥在工厂里存一份共享拷贝。
//
// 构造时就调 RequireRpcKey：把「密钥没配」的错误从运行时的每次 RPC
// 提前到进程启动，避免服务起来后所有调用都静默 401。
class RpcAuthFactory final : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
    explicit RpcAuthFactory(std::string role) : key_(RpcKey(role)) { RequireRpcKey(role); }
    grpc::experimental::Interceptor* CreateClientInterceptor(grpc::experimental::ClientRpcInfo*) override {
        return new RpcAuthInterceptor(key_);   // 所有权交给 gRPC，由其负责释放
    }
 private:
    std::string key_;
};
}  // namespace sparkpush
