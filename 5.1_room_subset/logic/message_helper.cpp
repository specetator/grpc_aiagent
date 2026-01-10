// ============================================================================
// 消息处理辅助函数实现
//
// 实现消息的后置处理逻辑，包括：
// 1. 查询用户在线状态和连接信息
// 2. 构造推送请求
// 3. 通过 Kafka 发送给 comet 节点
// ============================================================================
#include "message_helper.h"

#include <chrono>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "function_timer.h"
#include "logging.h"

namespace sparkpush {

bool PostProcessSingleMessageWithComets(int64_t target_user, const std::string& session_id,
                                        const ChatMessage& cm,
                                        const std::vector<std::string>& comets,
                                        KafkaProducer* push_producer) {
    FUNCTION_TIMER();
    if (!push_producer) {
        LOG_ERROR << "Kafka producer not ready for single message session=" << session_id;
        return false;
    }

    PushToCometRequest req;
    *req.mutable_message() = cm;

    for (const auto& cid : comets) {
        req.add_comet_ids(cid);
    }

    // 离线：comets 为空时仍保留 target 供下游处理（与原逻辑一致）
    auto* t = req.add_targets();
    t->set_user_id(target_user);

    std::string payload;
    if (!req.SerializeToString(&payload)) {
        LOG_ERROR << "Serialize PushToCometRequest failed for session " << session_id;
        return false;
    }

    if (!push_producer->Send(session_id, payload)) {
        LOG_ERROR << "Kafka send failed for session " << session_id;
        return false;
    }
    return true;
}

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
bool PostProcessSingleMessage(int64_t target_user, const std::string& session_id,
                              const ChatMessage& cm, RedisStore* redis_store,
                              KafkaProducer* push_producer) {
    FUNCTION_TIMER();  //打印函数耗时
    // 步骤1：验证 Redis 存储对象是否可用
    // 如果为空，无法查询用户连接信息，直接返回失败
    if (!redis_store) {
        LOG_ERROR << "redis_store is null in PostProcessSingleMessage";
        return false;
    }

    // 步骤2：验证 Kafka 生产者是否可用
    // 如果为空，无法发送推送消息到 comet，直接返回失败
    if (!push_producer) {
        LOG_ERROR << "Kafka producer not ready for single message session=" << session_id;
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
        LOG_WARN << "No online target for message " << session_id << ", comet_ids empty";
    }

    // 步骤6：将推送请求序列化为字节流
    // protobuf 的 SerializeToString 将消息对象转换为二进制格式
    std::string payload;
    if (!req.SerializeToString(&payload)) {
        LOG_ERROR << "Serialize PushToCometRequest failed for session " << session_id;
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

// 房间消息后置处理实现
//
// 处理流程（精准投递 pipeline 版本）：
// 1. 从 Redis 缓存获取房间成员列表（miss 时回源 MySQL 并回填）
// 2. 使用 pipeline 批量查询成员的在线路由（user_id -> comet_id）
// 3. 按 comet_id 聚合成员
// 4. 为每个 comet 构造 PushToCometRequest（只包含该 comet 上的在线用户）
// 5. 将推送请求序列化后通过 Kafka 发送
//
// @param room_id: 房间 ID
// @param cm: 聊天消息对象
// @param redis_store: Redis 存储对象
// @param push_producer: Kafka 生产者
// @param fallback_func: 回源函数（Redis miss 时查询 MySQL）
// @return: 成功返回 true，失败返回 false
bool PostProcessRoomMessage(int64_t room_id, const ChatMessage& cm, RedisStore* redis_store,
                            KafkaProducer* push_producer,
                            std::function<bool(int64_t, std::vector<int64_t>*)> fallback_func) {
    FUNCTION_TIMER();
    // 步骤1：验证 Redis 存储对象是否可用
    if (!redis_store) {
        LOG_ERROR << "redis_store is null in PostProcessRoomMessage";
        return false;
    }

    // 步骤2：验证 Kafka 生产者是否可用
    if (!push_producer) {
        LOG_ERROR << "Kafka producer not ready for room message room_id=" << room_id;
        return false;
    }

    // 步骤3：从 Redis
    // 缓存获取房间成员列表（读穿）,如果从redis读取不到会从MySQL读取并回填redis
    std::vector<int64_t> member_uids;
    if (!redis_store->GetRoomMembersFromCache(room_id, &member_uids, fallback_func)) {
        LOG_ERROR << "GetRoomMembersFromCache failed for room_id="
                  << room_id;  //这里不应该返回false，应该从MySQL读取?
        //但先不搞那么复杂，因为线上如果用了redis,我们就不应允许他全崩溃的
        return false;
    }

    // 步骤4：如果房间没有成员，直接返回成功（不投递）
    if (member_uids.empty()) {
        LOG_INFO << "Room has no members, skip push. room_id=" << room_id;
        return true;
    }

    // 步骤5：使用 pipeline 批量查询成员的在线路由
    // 返回 comet_id -> [user_id...] 的聚合映射
    std::unordered_map<std::string, std::vector<int64_t>> comet_users;
    if (!redis_store->BatchGetUserConnectionsComets(member_uids, &comet_users)) {
        LOG_ERROR << "BatchGetUserConnectionsComets failed for room_id=" << room_id;
        return false;
    }

    // 步骤6：如果所有成员都离线，不投递推送任务
    if (comet_users.empty()) {
        LOG_INFO << "All room members offline, skip push. room_id=" << room_id
                 << ", member_count=" << member_uids.size();
        return true;
    }

    // 步骤7：为每个 comet 构造推送请求并发送到 Kafka
    for (const auto& kv : comet_users) {
        const std::string& comet_id = kv.first;            // 获取comet_id
        const std::vector<int64_t>& user_ids = kv.second;  // 获取该comet_id上的在线用户列表

        // 构造推送请求
        PushToCometRequest req;
        *req.mutable_message() = cm;
        req.set_room_id(room_id);     // 设置房间 ID
        req.add_comet_ids(comet_id);  // 指定目标 comet

        // 添加该 comet 上的在线用户
        for (auto uid : user_ids) {
            auto* t = req.add_targets();
            t->set_user_id(uid);
            t->set_comet_id(comet_id);
        }

        // 序列化推送请求
        std::string payload;
        if (!req.SerializeToString(&payload)) {
            LOG_ERROR << "Serialize PushToCometRequest failed for room_id=" << room_id
                      << ", comet_id=" << comet_id;
            continue;  // 跳过该 comet，继续处理下一个
        }

        // 通过 Kafka 发送推送请求
        // 使用 room_id 作为 key，便于追踪和调试
        std::string kafka_key = "room:" + std::to_string(room_id);
        LOG_INFO << "Send room message to comet_id=" << comet_id << ", room_id=" << room_id
                 << ", user_count=" << user_ids.size() << ", msg_id=" << cm.msg_id();

        if (!push_producer->Send(kafka_key, payload)) {
            LOG_ERROR << "Kafka send failed for room_id=" << room_id << ", comet_id=" << comet_id;
            continue;  // 跳过该 comet，继续处理下一个
        }
    }

    // 返回成功，表示消息已提交到 Kafka
    LOG_INFO << "PostProcessRoomMessage completed, room_id=" << room_id
             << ", comet_count=" << comet_users.size() << ", member_count=" << member_uids.size();
    return true;
}

}  // namespace sparkpush
