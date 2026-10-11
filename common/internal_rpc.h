#pragma once
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include "security.h"

namespace sparkpush {
inline std::string RpcEnvironment(const char* name) {
    const char* value = std::getenv(name);
    return value ? value : "";
}
inline std::string RpcKey(const std::string& role) {
    if (role == "comet") return RpcEnvironment("SPARK_RPC_COMET_KEY");
    if (role == "job") return RpcEnvironment("SPARK_RPC_JOB_KEY");
    if (role == "admin") return RpcEnvironment("SPARK_RPC_ADMIN_KEY");
    return {};
}
inline void RequireRpcKey(const std::string& role) {
    if (RpcKey(role).size() < 32)
        throw std::runtime_error("internal RPC credential missing or shorter than 32 bytes: " + role);
}
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
inline std::string RpcReadFile(const char* variable) {
    const auto path = RpcEnvironment(variable);
    if (path.empty()) return {};
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error(std::string("cannot read TLS file configured by ") + variable);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}
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
class RpcAuthInterceptor final : public grpc::experimental::Interceptor {
 public:
    explicit RpcAuthInterceptor(std::string key) : key_(std::move(key)) {}
    void Intercept(grpc::experimental::InterceptorBatchMethods* methods) override {
        if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA))
            methods->GetSendInitialMetadata()->emplace("x-spark-rpc-key", key_);
        methods->Proceed();
    }
 private:
    std::string key_;
};
class RpcAuthFactory final : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
    explicit RpcAuthFactory(std::string role) : key_(RpcKey(role)) { RequireRpcKey(role); }
    grpc::experimental::Interceptor* CreateClientInterceptor(grpc::experimental::ClientRpcInfo*) override {
        return new RpcAuthInterceptor(key_);
    }
 private:
    std::string key_;
};
}  // namespace sparkpush
