#include "comet_grpc_service.h"

#include "logging.h"

namespace sparkpush {

CometServiceImpl::CometServiceImpl(CometServer* server)
    : server_(server) {}

void CometServiceImpl::ProcessPushRequest(
    const ::sparkpush::PushToCometRequest& request) {
  if (request.targets_size() > 0) {
    // 有明确的目标用户列表：推送给指定用户（单聊）
    std::vector<int64_t> users;
    users.reserve(request.targets_size());
    for (const auto& t : request.targets()) {
      users.push_back(t.user_id());
    }
    server_->PushToUsers(request.message(), users);
  } else if (request.room_id() > 0) {
    // 有 room_id：按房间维度广播（聊天室/弹幕）
    server_->PushToRoom(request.message(), request.room_id());
  } else {
    // 兜底：检查 session_id 前缀（向后兼容）
    const std::string& sid = request.message().session_id();
    if (sid.substr(0, 5) == "room:" || sid.substr(0, 8) == "danmaku:") {
      // 从 session_id 解析 room_id
      auto colon = sid.find(':');
      if (colon != std::string::npos) {
        try {
          int64_t room_id = std::stoll(sid.substr(colon + 1));
          if (room_id > 0) {
            server_->PushToRoom(request.message(), room_id);
          }
        } catch (...) {}
      }
    } else if (sid == "broadcast") {
      server_->PushToAll(request.message());
    }
  }
}

::grpc::Status CometServiceImpl::PushToComet(
    ::grpc::ServerContext*,
    const ::sparkpush::PushToCometRequest* request,
    ::sparkpush::PushToCometReply* response) {
  LogInfo("[CometGrpc] PushToComet msg_id=" + request->message().msg_id() +
          " targets=" + std::to_string(request->targets_size()));
  ProcessPushRequest(*request);
  response->mutable_error()->set_code(0);
  response->mutable_error()->set_message("ok");
  return ::grpc::Status::OK;
}

::grpc::Status CometServiceImpl::PushStream(
    ::grpc::ServerContext* context,
    ::grpc::ServerReader<::sparkpush::PushToCometRequest>* reader,
    ::sparkpush::PushStreamReply* response) {
  (void)context;
  
  PushToCometRequest request;
  int64_t count = 0;
  
  LogInfo("[CometGrpc] PushStream started");
  // 持续读取客户端发送的消息流
  while (reader->Read(&request)) {
    LogInfo("[CometGrpc] PushStream read msg_id=" + request.message().msg_id() +
            " targets=" + std::to_string(request.targets_size()));
    // persist_only 消息跳过推送
    if (request.comet_id() != "persist_only") {
      ProcessPushRequest(request);
    }
    ++count;
  }
  LogInfo("[CometGrpc] PushStream ended, count=" + std::to_string(count));
  
  response->set_received_count(count);
  return ::grpc::Status::OK;
}

}  // namespace sparkpush



