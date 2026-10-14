#pragma once
#include "model.h"
#include "spark_push.pb.h"

namespace sparkpush {
// Wire conversion belongs to adapters/publishers, not business DTOs.
//
// 译：线上格式转换属于适配器/发布层，不属于业务 DTO。
//
// 也就是说：纯业务类型 Message 不依赖 protobuf，而需要跨进程传递时
// 统一走本函数转成 ChatMessage。当前有两类调用方：
//   grpc_service.cpp       填 gRPC 响应（Unary 与同步页）
//   message_service.cpp    构造 Kafka 载荷与 Agent 请求里的消息体
// 把转换集中在一个 inline 函数里，是为了避开多处手写 set_xxx
// 在新增字段时漏改其中一处（那会造成某个字段默默不传）。
//
// 新增 Message 字段时，请同时改这里与 proto/spark_push.proto 里的 ChatMessage。
inline void FillChatMessage(const Message& message, ChatMessage* output) {
    output->set_msg_id(message.msg_id);
    output->set_session_id(message.session_id);
    output->set_msg_seq(message.msg_seq);
    output->set_sender_id(message.sender_id);
    output->set_timestamp_ms(message.timestamp_ms);
    output->set_msg_type(message.msg_type);
    output->set_content_json(message.content_json);
    output->set_client_msg_id(message.client_msg_id);
}
}  // namespace sparkpush
