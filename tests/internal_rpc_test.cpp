#include <cstdlib>
#include <stdexcept>
#include "grpc_keepalive.h"
#include "grpc_service.h"
using namespace sparkpush;
void Require(bool value) { if (!value) throw std::runtime_error("RPC authorization regression"); }
int main() {
    std::string comet_key, job_key;
    Require(GenerateSecureToken(&comet_key) && GenerateSecureToken(&job_key));
    setenv("SPARK_RPC_COMET_KEY", comet_key.c_str(), 1);
    setenv("SPARK_RPC_JOB_KEY", job_key.c_str(), 1);
    UserService users(nullptr, {});
    LogicServiceImpl service(&users, nullptr, nullptr, nullptr);
    grpc::ServerBuilder builder;
    int port = 0;
    builder.AddListeningPort("127.0.0.1:0", InternalServerCredentials("127.0.0.1"), &port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    Require(server && port > 0);
    const auto target = "127.0.0.1:" + std::to_string(port);
    VerifyTokenRequest request;
    VerifyTokenReply reply;
    auto invoke = [&](std::shared_ptr<grpc::Channel> channel) {
        auto stub = LogicService::NewStub(channel);
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
        return stub->VerifyToken(&context, request, &reply);
    };
    Require(invoke(grpc::CreateChannel(target, grpc::InsecureChannelCredentials())).error_code()
            == grpc::StatusCode::UNAUTHENTICATED);
    Require(invoke(CreateKeepaliveChannel(target, "job")).error_code()
            == grpc::StatusCode::UNAUTHENTICATED);
    Require(invoke(CreateKeepaliveChannel(target, "comet")).ok());
    // Workload authorization precedes user authentication, even for a streaming RPC.
    auto untrusted = LogicService::NewStub(grpc::CreateChannel(target, grpc::InsecureChannelCredentials()));
    grpc::ClientContext stream_context;
    stream_context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
    auto stream = untrusted->MessageStream(&stream_context);
    stream->WritesDone();
    Require(stream->Finish().error_code() == grpc::StatusCode::UNAUTHENTICATED);
    auto trusted = LogicService::NewStub(CreateKeepaliveChannel(target, "comet"));
    grpc::ClientContext admin_context;
    BroadcastRequest broadcast;
    BroadcastReply broadcast_reply;
    Require(trusted->Broadcast(&admin_context, broadcast, &broadcast_reply).error_code()
            == grpc::StatusCode::UNAUTHENTICATED);
    bool rejected = false;
    try { InternalServerCredentials("0.0.0.0"); } catch (const std::runtime_error&) { rejected = true; }
    Require(rejected);
    server->Shutdown();
    server->Wait();
}
