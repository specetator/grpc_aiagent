#include "grpc_service.h"

#include "logging.h"

#include <chrono>
#include <random>
#include <nlohmann/json.hpp>

namespace sparkpush {

namespace {
// 线程安全的随机数生成器
// 使用 thread_local 保证每个线程独立实例，避免加锁开销
// std::random_device{}() 提供真随机种子
thread_local std::mt19937 tl_rng(std::random_device{}());

std::string GenerateRandomSuffix(int range = 100000) {
  std::uniform_int_distribution<int> dist(0, range - 1);
  return std::to_string(dist(tl_rng));
}
}  // namespace

LogicServiceImpl::LogicServiceImpl(ConversationStore* store,
                                   GroupMemberDao* group_member_dao,
                                   UserDao* user_dao,
                                   KafkaProducer* producer,
                                   KafkaProducer* broadcast_producer,
                                   RedisStore* redis_store)
    : store_(store),
      group_member_dao_(group_member_dao),
      user_dao_(user_dao),
      producer_(producer),
      broadcast_producer_(broadcast_producer),
      redis_store_(redis_store) {}

void LogicServiceImpl::SetError(ErrorInfo* e, int code, const std::string& msg) {
  e->set_code(code);
  e->set_message(msg);
  // 热路径：不打日志
}

::grpc::Status LogicServiceImpl::VerifyToken(
    ::grpc::ServerContext*,
    const ::sparkpush::VerifyTokenRequest* request,
    ::sparkpush::VerifyTokenReply* response) {
  const std::string& token = request->token();
  if (!redis_store_) {
    SetError(response->mutable_error(), 500, "redis store not initialized");
    return ::grpc::Status::OK;
  }
  int64_t uid = 0;
  if (!redis_store_->GetUserIdByToken(token, &uid)) {
    SetError(response->mutable_error(), 401, "invalid or expired token");
    return ::grpc::Status::OK;
  }
  response->set_user_id(uid);
  SetError(response->mutable_error(), 0, "ok");
  // 路由注册：以 conn_id 为粒度写入 Redis HASH（route:user:{uid}）
  // 同一用户可能在多个设备/浏览器标签页上同时在线，每条连接有独立路由记录
  redis_store_->AddRoute(uid, request->comet_id(), request->conn_id());
  return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::SendUpstreamMessage(
    ::grpc::ServerContext*,
    const ::sparkpush::UpstreamMessageRequest* request,
    ::sparkpush::UpstreamMessageReply* response) {
  // 委托给 HandleUpstreamMessage，消除代码重复
  HandleUpstreamMessage(*request, response);
  return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::UserOffline(
    ::grpc::ServerContext*,
    const ::sparkpush::UserOfflineRequest* request,
    ::sparkpush::SimpleReply* response) {
  HandleUserOffline(*request, response);
  return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::ReportRoomJoin(
    ::grpc::ServerContext*,
    const ::sparkpush::RoomReportRequest* request,
    ::sparkpush::SimpleReply* response) {
  HandleRoomJoin(*request, response);
  return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::ReportRoomLeave(
    ::grpc::ServerContext*,
    const ::sparkpush::RoomReportRequest* request,
    ::sparkpush::SimpleReply* response) {
  HandleRoomLeave(*request, response);
  return ::grpc::Status::OK;
}

::grpc::Status LogicServiceImpl::Broadcast(
    ::grpc::ServerContext*,
    const ::sparkpush::BroadcastRequest* request,
    ::sparkpush::BroadcastReply* response) {
  if (!broadcast_producer_) {
    SetError(response->mutable_error(), 500, "broadcast producer not initialized");
    response->set_task_id("");
    return ::grpc::Status::OK;
  }

  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  std::string task_id = "bcast-" + std::to_string(now_ms) + "-" + GenerateRandomSuffix();

  BroadcastTaskRequest task;
  task.set_task_id(task_id);
  task.set_scope(request->scope());
  task.set_group_id(request->group_id());
  task.set_content_json(request->content_json());

  std::string payload;
  if (!task.SerializeToString(&payload)) {
    SetError(response->mutable_error(), 500, "serialize BroadcastTaskRequest failed");
    response->set_task_id("");
    return ::grpc::Status::OK;
  }

  if (!broadcast_producer_->Send(task_id, payload)) {
    SetError(response->mutable_error(), 500, "send broadcast task to kafka failed");
    response->set_task_id("");
    return ::grpc::Status::OK;
  }

  SetError(response->mutable_error(), 0, "ok");
  response->set_task_id(task_id);
  return ::grpc::Status::OK;
}

// ============================================================================
// 消息处理核心逻辑
// 处理流程：
//   1. 根据 scene（single/chatroom/danmaku）确定路由目标
//   2. 生成 session_id 和 msg_seq（Redis INCR，保证严格递增）
//   3. 构建 Kafka 消息，按 comet_id 分发到不同分区
//   4. Job 消费 Kafka 后推送到对应 Comet，Comet 再推送到客户端
// 该方法同时被 SendUpstreamMessage（unary）和 MessageStream（双向流）调用
// ============================================================================

void LogicServiceImpl::HandleUpstreamMessage(
    const ::sparkpush::UpstreamMessageRequest& request,
    ::sparkpush::UpstreamMessageReply* response) {
  const std::string& scene = request.scene();
  int64_t from_user = request.from_user_id();
  if (from_user <= 0) {
    SetError(response->mutable_error(), 400, "from_user_id must be positive");
    return;
  }

  // 发送限流（已注释：保留代码供面试讲解，实际压测时关闭以减少 Redis 开销）
  // if (redis_store_) {
  //   std::string rate_key = "rate:msg:" + std::to_string(from_user);
  //   if (!redis_store_->CheckRateLimit(rate_key, 1000, 20)) {
  //     SetError(response->mutable_error(), 429, "rate limit exceeded");
  //     return;
  //   }
  // }

  std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
  int64_t to_user = 0;
  int64_t room_id = 0;
  std::string session_id;

  if (scene == "single") {
    to_user = request.to_user_id();
    if (to_user <= 0) {
      SetError(response->mutable_error(), 400, "to_user_id must be positive");
      return;
    }
    // 单聊 session_id 规则：取两个 user_id 的较小值在前，保证 A→B 和 B→A 是同一会话
    int64_t min_id = std::min(from_user, to_user);
    int64_t max_id = std::max(from_user, to_user);
    session_id = "single:" + std::to_string(min_id) + ":" + std::to_string(max_id);
    
    // 查询目标用户的路由：先查本地缓存（shared_mutex 读锁），miss 则查 Redis
    // 返回去重后的 comet_id 列表（用户可能有多条连接在同一 Comet 上）
    std::vector<std::string> comets;
    if (redis_store_ && redis_store_->GetUserRoutes(to_user, &comets)) {
      for (const auto& cid : comets) {
        comet_to_users[cid].push_back(to_user);
      }
    }
  } else if (scene == "chatroom") {
    room_id = request.group_id();
    if (room_id <= 0) {
      SetError(response->mutable_error(), 400, "group_id(room_id) must be positive");
      return;
    }
    session_id = "room:" + std::to_string(room_id);
    
    std::vector<std::string> room_comets;
    if (redis_store_ && redis_store_->GetRoomComets(room_id, &room_comets)) {
      for (const auto& cid : room_comets) {
        comet_to_users[cid];
      }
    }
  } else if (scene == "danmaku") {
    room_id = request.group_id();
    if (room_id <= 0) {
      SetError(response->mutable_error(), 400, "group_id(room_id) must be positive for danmaku");
      return;
    }
    if (request.video_id().empty()) {
      SetError(response->mutable_error(), 400, "video_id required for danmaku");
      return;
    }
    session_id = "danmaku:" + request.video_id();
    
    std::vector<std::string> room_comets;
    if (redis_store_ && redis_store_->GetRoomComets(room_id, &room_comets)) {
      for (const auto& cid : room_comets) {
        comet_to_users[cid];
      }
    }
  } else {
    SetError(response->mutable_error(), 400, "unsupported scene");
    return;
  }

  int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();
  std::string msg_id = std::to_string(now_ms) + "-" + GenerateRandomSuffix();
  
  std::string msg_type = (scene == "danmaku") ? "danmaku" : "text";

  std::string push_json = request.content_json();
  try {
    auto j = nlohmann::json::parse(push_json);
    j["msg_id"] = msg_id;
    j["create_time"] = now_ms;
    j["from_user_id"] = from_user;
    if (!j.contains("client_msg_id")) {
       j["client_msg_id"] = request.client_msg_id();
    }
    push_json = j.dump();
  } catch (...) {}

  // 消息序号：Redis INCR 原子自增，保证同一会话内严格递增
  // 这是当前系统的性能瓶颈（~50K QPS 单机上限），可通过 Redis 分片或 Snowflake ID 优化
  int64_t msg_seq = 0;
  if (redis_store_) {
    redis_store_->IncrSessionMsgSeq(session_id, &msg_seq);
  }

  ChatMessage* cm = response->mutable_message();
  cm->set_msg_id(msg_id);
  cm->set_session_id(session_id);
  cm->set_msg_seq(msg_seq);
  cm->set_sender_id(from_user);
  cm->set_timestamp_ms(now_ms);
  cm->set_msg_type(msg_type);
  cm->set_content_json(push_json);
  cm->set_client_msg_id(request.client_msg_id());

  SetError(response->mutable_error(), 0, "ok");

  // 按 comet_id 分发到 Kafka：同一 comet 的消息用 comet_id 作为 Kafka key
  // 这样同一 Comet 的消息会落在同一分区，Job 消费后可批量推送
  for (const auto& [comet_id, users] : comet_to_users) {
    PushToCometRequest req;
    req.set_comet_id(comet_id);
    *req.mutable_message() = *cm;
    for (int64_t uid : users) {
      auto* t = req.add_targets();
      t->set_user_id(uid);
    }
    req.set_scene(scene);
    req.set_to_user_id(to_user);
    req.set_room_id(room_id);
    req.set_need_persist(true);

    std::string payload;
    if (req.SerializeToString(&payload)) {
      producer_->Send(comet_id, payload);
    }
  }

  // 离线消息处理：目标用户全部不在线时，仍需发送到 Kafka 落盘
  // comet_id="persist_only" 是特殊标记，Job 消费后仅执行 MySQL 持久化，不做推送
  if (comet_to_users.empty()) {
    PushToCometRequest req;
    req.set_comet_id("persist_only");
    *req.mutable_message() = *cm;
    req.set_scene(scene);
    req.set_to_user_id(to_user);
    req.set_room_id(room_id);
    req.set_need_persist(true);

    std::string payload;
    if (req.SerializeToString(&payload)) {
      producer_->Send("persist_only", payload);
    }
  }
}

void LogicServiceImpl::HandleUserOffline(
    const ::sparkpush::UserOfflineRequest& request,
    ::sparkpush::SimpleReply* response) {
  if (redis_store_) {
    // 按 conn_id 精确删除该连接的路由
    redis_store_->RemoveRoute(request.user_id(), request.conn_id());
  }
  SetError(response->mutable_error(), 0, "ok");
}

// 房间路由管理（精确路由）：
//   Redis 数据结构：
//     room:comets:{room_id}             → SET，记录哪些 Comet 上有该房间的用户
//     room:comet_count:{room_id}:{cid}  → 计数器，该 Comet 上该房间的用户数
//   当某 Comet 上首个用户加入房间时 SADD，最后一个用户离开时 SREM
//   消息推送时只发给有用户的 Comet，避免全量广播
void LogicServiceImpl::HandleRoomJoin(
    const ::sparkpush::RoomReportRequest& request,
    ::sparkpush::SimpleReply* response) {
  if (!redis_store_) {
    SetError(response->mutable_error(), 500, "redis store not initialized");
    return;
  }
  int64_t room_id = request.room_id();
  const std::string& comet_id = request.comet_id();
  int64_t user_id = request.user_id();
  if (room_id <= 0 || comet_id.empty()) {
    SetError(response->mutable_error(), 400, "room_id/comet_id required");
    return;
  }

  if (user_id > 0) {
    redis_store_->IncrRoomOnlineCount(room_id, 1);
  }

  int64_t new_count = 0;
  if (redis_store_->IncrRoomCometCount(room_id, comet_id, 1, &new_count)) {
    if (new_count == 1) {
      redis_store_->AddRoomComet(room_id, comet_id);
    }
  }
  SetError(response->mutable_error(), 0, "ok");
}

void LogicServiceImpl::HandleRoomLeave(
    const ::sparkpush::RoomReportRequest& request,
    ::sparkpush::SimpleReply* response) {
  if (!redis_store_) {
    SetError(response->mutable_error(), 500, "redis store not initialized");
    return;
  }
  int64_t room_id = request.room_id();
  const std::string& comet_id = request.comet_id();
  int64_t user_id = request.user_id();
  if (room_id <= 0 || comet_id.empty()) {
    SetError(response->mutable_error(), 400, "room_id/comet_id required");
    return;
  }

  if (user_id > 0) {
    redis_store_->IncrRoomOnlineCount(room_id, -1);
  }

  int64_t new_count = 0;
  if (redis_store_->IncrRoomCometCount(room_id, comet_id, -1, &new_count)) {
    if (new_count <= 0) {
      redis_store_->RemoveRoomComet(room_id, comet_id);
    }
  }
  SetError(response->mutable_error(), 0, "ok");
}

// gRPC 双向流实现：单条持久连接复用多种消息类型
// 优势：消除每次 RPC 的连接建立/销毁开销，吞吐量从 ~16K 提升到 ~33K QPS
// 通过 oneof payload 区分不同消息类型（上行消息/下线通知/房间加入离开），
// 通过 request_id 匹配请求和响应
::grpc::Status LogicServiceImpl::MessageStream(
    ::grpc::ServerContext* context,
    ::grpc::ServerReaderWriter<::sparkpush::StreamResponse,
                               ::sparkpush::StreamMessage>* stream) {
  StreamMessage msg;
  while (stream->Read(&msg)) {
    StreamResponse resp;
    resp.set_request_id(msg.request_id());

    switch (msg.payload_case()) {
      case StreamMessage::kUpstream: {
        UpstreamMessageReply reply;
        HandleUpstreamMessage(msg.upstream(), &reply);
        *resp.mutable_error() = reply.error();
        *resp.mutable_upstream_reply() = reply;
        break;
      }
      case StreamMessage::kOffline: {
        SimpleReply reply;
        HandleUserOffline(msg.offline(), &reply);
        *resp.mutable_error() = reply.error();
        *resp.mutable_simple_reply() = reply;
        break;
      }
      case StreamMessage::kRoomJoin: {
        SimpleReply reply;
        HandleRoomJoin(msg.room_join(), &reply);
        *resp.mutable_error() = reply.error();
        *resp.mutable_simple_reply() = reply;
        break;
      }
      case StreamMessage::kRoomLeave: {
        SimpleReply reply;
        HandleRoomLeave(msg.room_leave(), &reply);
        *resp.mutable_error() = reply.error();
        *resp.mutable_simple_reply() = reply;
        break;
      }
      default:
        SetError(resp.mutable_error(), 400, "unknown message type");
        break;
    }

    if (!stream->Write(resp)) {
      break;  // 客户端断开
    }
  }
  return ::grpc::Status::OK;
}

}  // namespace sparkpush
