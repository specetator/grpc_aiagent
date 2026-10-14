#include "comet_grpc_service.h"

#include "logging.h"
#include "internal_rpc.h"
#include "metrics.h"

namespace sparkpush {

CometServiceImpl::CometServiceImpl(CometServer* server) : server_(server) {}

// 下推请求的实际处理：幂等去重 -> 选 fanout 策略 -> 投递 -> 上报游标。
//
// 返回值是「成功 send 的连接数」，会回填到 reply.delivered_count。
// Job 侧只在 delivered_count > 0 时才发 delivered_ack，因此这个值
// 直接决定了客户端能不能把气泡从「发送中」改成「已送达」。
//
// 三种 fanout 策略（优先级从上到下）：
//   1) targets 非空   按 user_id 精确投递（单聊、定向广播）
//   2) session_id 以 "r_" 开头   本机房间成员广播（聊天室）
//   3) session_id == "broadcast"  本机全量广播
//
// 为何分支 2 不靠 targets：聊天室的接收者可能很多，Logic 侧已经改成
// 按 comet 级 fanout（只告知「哪些节点有人」），具体成员由本机
// room_users_ 快照决定，避开在 Kafka 事件里携带巨大的用户列表。
size_t CometServiceImpl::ProcessPushRequest(
    const ::sparkpush::PushToCometRequest& request) {
    // AcceptPushRequest 先做幂等：Job 重连重放、Unary 兜底与流路径重叠时
    // 同一消息可能多次到达。返回 0 而不是错误：对 Job 而言重复投递
    // 应当被当成「已处理成功」，否则它会一直重试。
    if (!server_ || !server_->AcceptPushRequest(request.request_id())) {
        return 0;
    }
    if (request.targets_size() > 0) {
        std::vector<int64_t> users;
        users.reserve(request.targets_size());
        for (const auto& target : request.targets()) {
            if (target.user_id() > 0) users.push_back(target.user_id());
        }
        std::vector<int64_t> delivered_users;
        const size_t delivered = server_->PushToUsers(request.message(), users, &delivered_users);
        // 只有真正 send 成功才推进 delivered 游标。游标是 SyncOffline
        // 的唯一依据，提前推进会造成消息永久丢失。
        if (delivered > 0) {
            server_->ReportDeliveredToUsers(request.message(), delivered_users);
        }
        return delivered;
    }

    const std::string& session_id = request.message().session_id();
    // 房间广播：从 session_id 里解出 room_id（命名约定 r_{room_id}）。
    // 手写前缀判断而不用正则：这在每条群消息上都会跑，热路径不值得。
    if (session_id.size() > 2 && session_id[0] == 'r' && session_id[1] == '_') {
        try {
            const int64_t room_id = std::stoll(session_id.substr(2));
            if (room_id <= 0) return 0;
            // GetRoomUserIds 返回的是**本机内存快照**，不是权威成员表。
            // 权威校验已在 Logic 侧完成（成员资格 + bearer），
            // 这里只负责「把消息交给本机的在线连接」。
            const auto users = server_->GetRoomUserIds(room_id);
            std::vector<int64_t> delivered_users;
            const size_t delivered = server_->PushToUsers(request.message(), users, &delivered_users);
            if (delivered > 0) {
                server_->ReportDeliveredToUsers(request.message(), delivered_users);
            }
            return delivered;
        } catch (...) {
            return 0;   // stoll 对脏 session_id 会抛，吃掉并计为未投递
        }
    }
    // 全量广播。注意不调 ReportDeliveredToUsers：广播不属于任何会话，
    // 没有 msg_seq，因此不存在需要推进的 delivered 游标。
    if (session_id == "broadcast") {
        return server_->PushToAll(request.message());
    }
    return 0;
}

// Unary 兜底路径。Job 在流不可用或流 reply 超时时会回退到这里。
//
// 与 PushStream 共用 ProcessPushRequest，因此两条路径的幂等与
// fanout 行为完全一致——这是「流失败后退化到 Unary 不会重复投递」
// 的前提（靠同一个 request_id 去重）。
//
// 即使 delivered == 0 也返回 Status::OK：「目标全离线」是正常业务
// 结果，不是 RPC 错误。返回错误会让 Job 重试并最终计入 delivery_lost。
::grpc::Status CometServiceImpl::PushToComet(
    ::grpc::ServerContext* context, const ::sparkpush::PushToCometRequest* request,
    ::sparkpush::PushToCometReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "job");
    if (!authorized.ok()) return authorized;
    const size_t delivered = ProcessPushRequest(*request);
    response->set_request_id(request->request_id());
    response->set_delivered_count(static_cast<int32_t>(delivered));
    response->mutable_error()->set_code(0);
    response->mutable_error()->set_message("ok");
    return ::grpc::Status::OK;
}

::grpc::Status CometServiceImpl::PushDeliveryAck(
    ::grpc::ServerContext* context, const ::sparkpush::PushDeliveryAckRequest* request,
    ::sparkpush::SimpleReply* response) {
    const auto authorized = AuthorizeInternalRpc(context, "job");
    if (!authorized.ok()) return authorized;
    if (server_ && request->user_id() > 0) {
        server_->PushDeliveryAck(request->user_id(), request->message());
    }
    response->mutable_error()->set_code(0);
    response->mutable_error()->set_message("ok");
    return ::grpc::Status::OK;
}

// Job 为每个 Comet 复用一条双向客户端流；每个 request 都返回对应 reply。
//
// 严格的一请求一应答：Job 侧靠 request_id 把 reply 与阻塞等待的
// 消费线程对上，并且**只有收到 reply 才提交 Kafka offset**。因此
// 这里必须保证每个 Read 到的 request 都对应一次 Write，否则 Job
// 会一直阻塞到超时。
//
// 串行处理（而不是并发）：同一连接上的消息顺序就是 Kafka 分区内
// 的顺序，并发处理会打乱它，客户端会看到乱序消息。
::grpc::Status CometServiceImpl::PushStream(
    ::grpc::ServerContext* context,
    ::grpc::ServerReaderWriter<::sparkpush::PushToCometReply,
                               ::sparkpush::PushToCometRequest>* stream) {
    const auto authorized = AuthorizeInternalRpc(context, "job");
    if (!authorized.ok()) return authorized;
    // 该 gauge 只在 Job->Comet 长连接真正进入服务端处理函数后置 1。
    // 启动脚本据此判断“可以收发消息”，避免仅凭 9105 端口监听就提前返回。
    MetricsRegistry::Instance().Set("spark_push_comet_push_stream_ready", 1);
    PushToCometRequest request;
    while (stream->Read(&request)) {
        const size_t delivered = ProcessPushRequest(request);
        PushToCometReply reply;
        // 回填 request_id 是幂等匹配的关键：Job 靠它找到对应的
        // PromiseState 并唤醒阻塞的消费线程。漏了会让 Job 永远等不到。
        reply.set_request_id(request.request_id());
        reply.set_delivered_count(static_cast<int32_t>(delivered));
        reply.mutable_error()->set_code(0);
        reply.mutable_error()->set_message("ok");
        // Write 失败说明流已断，直接 break。未发出 reply 的那些请求
        // 会由 Job 侧 ReconnectStream 全部标为失败，进而重放。
        if (!stream->Write(reply)) {
            break;
        }
    }
    // 退出时把就绪标志置 0：健康检查会立即发现下推通道已断，
    // 而不是因为「端口还在监听」而误判为健康。
    MetricsRegistry::Instance().Set("spark_push_comet_push_stream_ready", 0);
    MetricsRegistry::Instance().Increment("spark_push_comet_push_stream_closed_total");
    return ::grpc::Status::OK;
}

}  // namespace sparkpush
