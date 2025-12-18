// ============================================================================
// 消息处理辅助函数实现
//
// 实现消息的后置处理逻辑，包括：
// 1. 查询用户在线状态和连接信息
// 2. 构造推送请求
// 3. 通过 Kafka 发送给 comet 节点
// ============================================================================
#include "message_helper.h"

#include <unordered_map>
#include <unordered_set>

#include "logging.h"

namespace sparkpush {

// 单聊消息后置处理实现
//
// 处理流程：
// 1. 验证 Redis 和 Kafka 依赖是否可用
// 2. 从 Redis 查询目标用户的在线连接信息（comet 节点列表）
// 3. 构造 PushToCometRequest 推送请求，填充消息和目标信息
// 4. 将推送请求序列化为 protobuf 字节流
// 5. 通过 Kafka 发送到消息队列，由 comet 节点消费
//
// @param target_user: 目标用户 ID（消息接收方）
// @param session_id: 会话 ID，格式为 "s_<小uid>:<大uid>"
// @param cm: 聊天消息对象，包含完整的消息信息
// @param redis_store: Redis 存储对象，用于查询用户连接
// @param push_producer: Kafka 生产者，用于发送推送请求
// @return: 成功返回 true，失败返回 false
bool PostProcessSingleMessage(int64_t target_user,
                              const std::string& session_id,
                              const ChatMessage& cm, RedisStore* redis_store,
                              KafkaProducer* push_producer) {
    // 步骤1：验证 Redis 存储对象是否可用
    // 如果为空，无法查询用户连接信息，直接返回失败
    if (!redis_store) {
        LOG_ERROR << "redis_store is null in PostProcessSingleMessage";
        return false;
    }

    // 步骤2：验证 Kafka 生产者是否可用
    // 如果为空，无法发送推送消息到 comet，直接返回失败
    if (!push_producer) {
        LOG_ERROR << "Kafka producer not ready for single message session="
                  << session_id;
        return false;
    }

    // 步骤3：构造推送请求对象
    // PushToCometRequest 是 protobuf 定义的消息体，包含消息内容和目标信息
    PushToCometRequest req;
    // 将聊天消息复制到推送请求中
    *req.mutable_message() = cm;

    // 步骤4：从 Redis 查询目标用户当前连接的所有 comet 节点
    // 一个用户可能在多个设备/浏览器上登录，因此可能有多个 comet 连接
    std::vector<std::string> comets;
    if (redis_store->GetUserConnectionComets(target_user, &comets)) {
        // 遍历所有 comet 节点，为每个节点添加推送目标
        for (const auto& cid : comets) {
            // 添加 comet_id 到推送请求的 comet_ids 列表
            req.add_comet_ids(cid);
            // 添加推送目标，包含用户 ID 和对应的 comet_id
            auto* t = req.add_targets();
            t->set_user_id(target_user);
            t->set_comet_id(cid);
        }
    }

    // 步骤5：处理用户离线的情况
    // 如果用户没有在线连接（comets 为空），仍然构造一个推送目标
    // 这样可以让 comet 节点进行离线推送处理（如离线消息存储、推送通知等）
    if (comets.empty()) {
        auto* t = req.add_targets();
        t->set_user_id(target_user);
        LOG_WARN << "No online target for message " << session_id
                 << ", comet_ids empty";
    }

    // 步骤6：将推送请求序列化为字节流
    // protobuf 的 SerializeToString 将消息对象转换为二进制格式
    std::string payload;
    if (!req.SerializeToString(&payload)) {
        LOG_ERROR << "Serialize PushToCometRequest failed for session "
                  << session_id;
        return false;
    }

    // 步骤7：通过 Kafka 发送推送请求
    // 使用 session_id 作为 Kafka 的 key，便于追踪和调试
    // Send 方法是异步的，返回 true 表示提交成功，不代表一定发送成功
    LOG_INFO << "Send payload for session " << session_id;
    if (!push_producer->Send(session_id, payload)) {
        LOG_ERROR << "Kafka send failed for session " << session_id;
        return false;
    }

    // 返回成功，表示消息已提交到 Kafka
    return true;
}

}  // namespace sparkpush
