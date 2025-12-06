#include "comet_grpc_service.h"

#include "logging.h"

namespace sparkpush {

// 构造 CometServiceImpl，持有 CometServer 指针以便转发推送。
CometServiceImpl::CometServiceImpl(CometServer* server)
        : server_(server) {}

// gRPC 下行入口：根据 targets / session_id 将消息分发到本机连接集合。
::grpc::Status CometServiceImpl::PushToComet(
        ::grpc::ServerContext*,
        const ::sparkpush::PushToCometRequest* request,
        ::sparkpush::PushToCometReply* response) {
    // Comet 侧收到 job/logic 的下行推送请求：
    // 1. 如果 targets 列表非空，按 user_id 精确推送。
    // 2. 否则根据 message.session_id 约定进行房间/广播推送：
    //    - "r_<room_id>" 表示房间广播
    //    - "broadcast" 表示全服广播（仅限本 comet 内）
    // 3. PushToUsers/PushToRoom/PushToAll 均只在本机连接集合内分发，
    //    不跨节点路由，调用方需自行按 comet_id 分片。
    LOG_INFO << "PushToComet received for comet_id=" << request->comet_id()
             << " msg_id=" << request->message().msg_id();
    if (request->targets_size() > 0) {
        std::vector<int64_t> users;
        users.reserve(request->targets_size());
        for (const auto& t : request->targets()) {
            users.push_back(t.user_id());
        }
        server_->PushToUsers(request->message(), users);
    } else {
        // targets 为空：按聊天室房间维度投递（依赖 session_id = "r_<room_id>" 约定）
        const std::string& sid = request->message().session_id();
        if (sid.size() > 2 && sid[0] == 'r' && sid[1] == '_') {
            int64_t room_id = 0;
            try {
                room_id = std::stoll(sid.substr(2));
            } catch (...) {
                room_id = 0;
            }
            if (room_id > 0) {
                server_->PushToRoom(request->message(), room_id);
            }
        } else if (sid == "broadcast") {
            server_->PushToAll(request->message());
        }
    }
    response->mutable_error()->set_code(0);
    response->mutable_error()->set_message("ok");
    return ::grpc::Status::OK;
}

}  // namespace sparkpush



