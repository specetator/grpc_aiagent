
#include "comet_server.h"
#include "app.h"
#include "security.h"
#include <grpcpp/grpcpp.h>
#include <muduo/base/Logging.h>
#include <chrono>
#include <thread>
#include <iostream>
#include <atomic>
class FakeLogic final : public sparkpush::LogicService::Service {
 public:
 std::atomic<bool> delivered{false};
 grpc::Status MessageStream(grpc::ServerContext* context,
   grpc::ServerReaderWriter<sparkpush::StreamResponse,sparkpush::StreamMessage>*) override {
   while (!context->IsCancelled()) std::this_thread::sleep_for(std::chrono::milliseconds(10));
   return {grpc::StatusCode::CANCELLED, "stalled stream cancelled"};
 }
 grpc::Status VerifyToken(grpc::ServerContext*, const sparkpush::VerifyTokenRequest* q, sparkpush::VerifyTokenReply* r) override {
  if(q->token()=="slow") std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  r->set_route_generation(1); r->set_route_token("fixture-lease"); r->set_user_id(q->token()=="slow"?8:7); return grpc::Status::OK;
 }
 grpc::Status SyncOffline(grpc::ServerContext*,const sparkpush::SyncOfflineRequest*,sparkpush::SyncOfflineReply*) override {return grpc::Status::OK;}
 grpc::Status SyncMessages(grpc::ServerContext*,const sparkpush::SyncMessagesRequest*,sparkpush::SyncMessagesReply* reply) override {
  auto* message = reply->add_messages();
  message->set_msg_id("s_7_8-1"); message->set_session_id("s_7_8"); message->set_msg_seq(1);
  message->set_sender_id(8); message->set_timestamp_ms(123); message->set_msg_type("text");
  message->set_content_json("legacy text"); reply->set_safe_seq(1);
  return grpc::Status::OK;
 }
 grpc::Status MarkDelivered(grpc::ServerContext*,const sparkpush::MarkDeliveredRequest* request,sparkpush::SimpleReply*) override {
  if (request->user_id()==7 && request->cursors_size()==1 && request->cursors(0).msg_seq()==1) delivered=true;
  return grpc::Status::OK;
 }
 grpc::Status UserOffline(grpc::ServerContext*,const sparkpush::UserOfflineRequest*,sparkpush::SimpleReply*) override {return grpc::Status::OK;}
};
int main(int argc,char** argv){
 muduo::Logger::setLogLevel(muduo::Logger::WARN);
 std::string key; if(!sparkpush::GenerateSecureToken(&key)) return 1;
 setenv("SPARK_RPC_COMET_KEY",key.c_str(),1);setenv("SPARK_RPC_JOB_KEY",key.c_str(),1);
 FakeLogic fake; grpc::ServerBuilder b; int logic_port=0;
 b.AddListeningPort("127.0.0.1:0",grpc::InsecureServerCredentials(),&logic_port); b.RegisterService(&fake); auto gs=b.BuildAndStart();
 sparkpush::Config c; c.listen_port=std::stoi(argv[1]); c.logic_grpc_target="127.0.0.1:"+std::to_string(logic_port); c.use_grpc_stream=false; c.comet_id="review-probe"; c.metrics_port=0; c.comet_grpc_pool_size=2;
 c.comet_io_threads=1; c.comet_grpc_port=0;
 if(argc>2) { c.use_grpc_stream=true; c.grpc_stream_count=1; c.grpc_stream_ack_timeout_ms=250; }
 sparkpush::RunComet(c); gs->Shutdown(); gs->Wait();
 return fake.delivered ? 0 : 1;
}
