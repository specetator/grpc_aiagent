// ============================================================================
// 消息处理辅助函数
//
// 提供消息处理相关的后置处理功能，主要包括：
// 1. 单聊消息的推送处理
// 2. 构造下行推送请求并发送到 Kafka
//
// 设计要点：
// - 从 Redis 查询用户的在线 comet 节点信息
// - 构造 PushToCometRequest 消息体
// - 通过 Kafka 将消息发送给 comet 节点进行实时推送
// ============================================================================
#pragma once

#include <string>
#include <vector>

#include "kafka_producer.h"
#include "redis_store.h"
#include "spark_push.pb.h"

namespace sparkpush {

// 单聊消息后置处理：负责将消息推送到目标用户
//
// 功能说明：
// 1. 从 Redis 查询目标用户当前连接的 comet 节点列表
// 2. 构造 PushToCometRequest 消息，包含消息内容和目标 comet 信息
// 3. 将推送请求序列化后通过 Kafka 发送给对应的 comet 节点
// 4. 如果用户不在线（没有 comet
// 连接），也会构造推送请求，用于离线推送或后续处理
//
// 参数说明：
// @param target_user: 目标用户 ID，消息接收方
// @param session_id: 会话 ID，用于标识是哪个会话的消息
// @param cm: 聊天消息对象，包含消息的完整信息（msg_id、发送者、内容等）
// @param redis_store: Redis 存储对象指针，用于查询用户连接信息
// @param push_producer: Kafka 生产者指针，用于发送推送消息到 comet 节点
//
// 返回值：
// @return true: Kafka 发送操作尝试成功（不代表用户一定在线或收到）
// @return false: 关键依赖缺失（redis_store 或 push_producer 为空）或序列化失败
//
// 注意事项：
// - 会话/未读/快照等功能在当前章节未实现，仅实现消息推送
// - 返回 true 只表示消息已发送到 Kafka，不保证用户在线或已收到
// - 如果用户离线，推送请求仍会发送，由 comet 节点处理离线逻辑
bool PostProcessSingleMessage(int64_t target_user,
                              const std::string& session_id,
                              const ChatMessage& cm, RedisStore* redis_store,
                              KafkaProducer* push_producer);

// 房间消息后置处理：负责将消息精准推送到房间在线成员所在的 comet
//
// 功能说明：
// 1. 从 Redis 缓存获取房间成员列表（miss 时回源 MySQL）
// 2. 使用 pipeline 批量查询成员的在线路由
// 3. 按 comet_id 聚合成员
// 4. 为每个 comet 构造 PushToCometRequest 并发送到 Kafka
// 5. 如果所有成员都离线，不投递推送任务
//
// 参数说明：
// @param room_id: 房间 ID
// @param cm: 聊天消息对象，包含消息的完整信息
// @param redis_store: Redis 存储对象，用于查询缓存和在线路由
// @param push_producer: Kafka 生产者，用于发送推送消息到 comet 节点
// @param fallback_func: 回源函数（Redis miss 时查询 MySQL 成员列表）
//
// 返回值：
// @return true: Kafka 发送操作尝试成功
// @return false: 关键依赖缺失或序列化失败
//
// 注意事项：
// - 使用 pipeline 批量查询在线路由，降低延迟
// - 按 comet 聚合后精准投递，避免全局广播
// - 不允许 comet_id 为空（避免 job 广播）
bool PostProcessRoomMessage(
    int64_t room_id, const ChatMessage& cm, RedisStore* redis_store,
    KafkaProducer* push_producer,
    std::function<bool(int64_t, std::vector<int64_t>*)> fallback_func);

}  // namespace sparkpush
