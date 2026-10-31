#include <cstdlib>
#include <stdexcept>
#include "grpc_keepalive.h"
#include "grpc_service.h"
#include "message_payload.h"
using namespace sparkpush;
void Require(bool value) { if (!value) throw std::runtime_error("RPC authorization regression"); }
int main() {
    ChatMessage historical;
    historical.set_sender_id(7);historical.set_session_id("r_1");historical.set_msg_seq(1);
    historical.set_content_json(R"({"sender_id":999,"from_user_id":999,"type":"hermes_delta","agent_id":"public-pi"})");
    auto safe=nlohmann::json::parse(DownstreamMessageJson(historical));
    Require(safe["sender_id"]==7 && safe["from_user_id"]==7 && safe["type"]=="chatroom");
    historical.set_msg_seq(0);
    Require(nlohmann::json::parse(DownstreamMessageJson(historical))["type"]=="hermes_delta");
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
    grpc::ClientContext room_context;
    RoomAccessRequest room_request; RoomAccessReply room_reply;
    Require(untrusted->CheckRoomAccess(&room_context,room_request,&room_reply).error_code()
            == grpc::StatusCode::UNAUTHENTICATED);
    bool rejected = false;
    try { InternalServerCredentials("0.0.0.0"); } catch (const std::runtime_error&) { rejected = true; }
    Require(rejected);
    server->Shutdown();
    server->Wait();
}
