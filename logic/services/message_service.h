#pragma once

#include <chrono>
#include <deque>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "hermes_command.h"
#include "model.h"
#include "rate_limiter.h"
#include "keyed_mutex.h"
#include "services/service_status.h"

namespace sparkpush {
class ConversationStore;
class GroupMemberDao;
class UserService;
class KafkaProducer;
class RedisStore;
class AttachmentService;
class RoomAgentService;

// 持久化发布的抽象。生产实现是 KafkaProducer 的 SendAndWait 包装，
// 测试可以注入一个直接返回 true 的假实现，不启 Kafka 就能跑 Submit 全流程。
// 抽成接口的唯一目的就是这个：Submit 里的 outbox 三步顺序是最关键的不变量，
// 必须能在单测里被驱动到。
class MessageEventPublisher {
 public:
    virtual ~MessageEventPublisher() = default;
    // 必须等 broker 确认（delivery report）才返回 true。
    // 这是 accepted_ack 语义的唯一依据：不等确认就返回，
    // 客户端会拿到一个「服务端已接受」的承诺，而消息可能根本没进 Kafka。
    virtual bool Confirm(const std::string& key, const std::string& payload, int timeout_ms) = 0;
};

// 上行消息命令。由适配层（gRPC 双向流 / Unary / HTTP）从请求里解出后填入。
//
// auth_token 必须原样透传：Submit 会用它重新解出 actor 并校验与
// from_user_id 一致。这是防止 Comet 被攻破后伪造发送者的关键：
// 即使 from_user_id 被改写，没有对应的 token 也过不了鉴权。
struct UpstreamCommand {
    std::string scene;
    int64_t from_user_id{0};
    int64_t to_user_id{0};
    int64_t group_id{0};
    std::string content_json;
    std::string client_msg_id;
    std::string source_comet_id;
    std::string auth_token;
};

// Submit 的结果。三个标志必须一起看，它们对应三个不同的语义边界：
//   has_message  消息已分配序号并构造完成
//   accepted     persist 事件已获 Kafka delivery report
//   message      已分配 msg_seq 的权威消息体
//
// **accepted 不等于已落库，也不等于对方已收到**。它只意味着
// 「消息已交给持久化管道且 broker 已确认」。对方收到是 delivered_ack，
// 那是另一条路径。把这三个混为一谈是读本项目最容易犯的错。
struct UpstreamResult {
    ServiceStatus status;
    Message message;
    bool has_message{false};
    bool accepted{false};
    int64_t accepted_at_ms{0};
};

// 广播结果。task_id 用于后续追查，scope 区分全局/房间广播。
struct BroadcastResult {
    ServiceStatus status;
    std::string task_id;
    std::string scope;
    int64_t group_id{0};
};

// Business orchestration shared by unary/streaming adapters and Kafka consumers.
// Owns rate limits, idempotent acceptance and Agent lifecycle caches, NOT RPCs.
//
// 译：由 Unary/流式适配器与 Kafka 消费者共用的业务编排层。
// 它拥有限流、幂等接受与 Agent 生命周期缓存，**但不拥有 RPC**。
//
// 这是全项目最核心的一个类。「不拥有 RPC」意味着：
// 本类只写 Kafka 与读写存储，从不直接调 Comet。下行推送由 Job 消费
// push topic 后通过 PushStream 完成。这个分离让 Logic 可以无状态水平扩展：
// 任一 Logic 实例都不需要知道用户连在哪个 Comet 上就能发消息。
//
// Submit 的八个阶段与不变量见 message_service.cpp 里的详细注释。
class MessageService {
   public:
    // 依赖很多，但没有一个是有默认值的（除了最后两个）：
    // 四个 producer 分别对应四个独立 topic，混用会造成消费者互相干扰。
    //   single_producer    push_single（单聊实时）
    //   group_producer     push_group（群聊/房间实时）
    //   broadcast_producer push_broadcast（全局广播）
    //   persist_producer   persist_message（落库，**accepted 的唯一依据**）
    //   agent_requests     Agent 请求队（可选）
    MessageService(ConversationStore* store, GroupMemberDao* members,
                   UserService* users, KafkaProducer* single_producer,
                   KafkaProducer* group_producer, KafkaProducer* broadcast_producer,
                   KafkaProducer* persist_producer, RedisStore* redis,
                   const RateLimitConfig& rate_limit, int persist_timeout_ms,
                   KafkaProducer* agent_requests, bool agent_enabled,
                   int64_t agent_user_id, std::map<int64_t, std::string> agent_users,
                   AttachmentService* attachments, RoomAgentService* room_agents = nullptr,
                   MessageEventPublisher* persistence_publisher = nullptr);

    // 全系统最核心的函数：上行消息的完整编排。
    // 八个阶段（格式门禁 -> 会话级串行 -> 鉴权 -> 限流 -> 路由解析
    // -> 序号分配 -> 持久化 -> 实时投递）详见 .cpp 里的路线图注释。
    UpstreamResult Submit(const UpstreamCommand& command);

    // Compatibility path: old HTTP API synchronously persists a text message,
    // with no accepted/delivered ACK or realtime fanout. Do not route it through
    // Submit implicitly or claim it has the WS reliability contract.
    //
    // 译：兼容路径。旧 HTTP API 同步持久化一条文本消息，
    // **没有 accepted/delivered ACK，也没有实时扇出**。
    // 不要把它隐式接到 Submit 上，也不要声称它具备 WS 的可靠性契约。
    //
    // 保留它只是为了不弄坏旧客户端。新集成一律走 Submit：
    // 本方法发出去的消息没人会实时收到，对方只能靠下次主动同步才看到。
    UpstreamResult SendLegacyText(int64_t sender, int64_t recipient,
                                  const std::string& content);

    // 登记一条已鉴权的路由（user -> comet），返回路由代数（generation）。
    // generation 与 route_token 共同解决「同一用户多设备/快速重连」时的
    // 路由竞争：旧连接的下线上报如果代数不对，就不能把新路由摘掉。
    int64_t AddAuthenticatedRoute(int64_t user, const std::string& comet, std::string* route_token);
    // 下线时摘路由。必须带上 generation 与 route_token 做比对，
    // 否则一次迟到的下线上报会把用户刚刚重建的新连接路由清掉，
    // 造成「明明在线却收不到消息」——这是这类系统最难查的 bug 之一。
    void UserOffline(int64_t user, const std::string& comet, int64_t generation, const std::string& route_token);

    // 广播。写 broadcast topic 后立即返回 task_id，**不等扇出完成**。
    // 实际扇出由 Job 的 broadcast consumer 消费后展开（见 common/broadcast_fanout.h）。
    BroadcastResult Broadcast(const std::string& scope, int64_t group_id,
                              const std::string& content_json);
    // 管理端广播：先过 RequireAdministrator，再走同一个 PublishBroadcast，
    // 但**不限流**（rate_limit=false）。管理通知不应该被普通消息的限流拖住。
    BroadcastResult BroadcastAdmin(int64_t actor, const std::string& scope,
                                   int64_t room_id, const std::string& text);

    // Agent 回复消费入口（hermes_reply topic）。返回 false 会让消息进 DLQ。
    bool HandleHermesReply(const std::string& payload, const std::string& kafka_key);
    // Agent 流式增量消费入口（hermes_delta topic）。
    // delta **不落库也不占 msg_seq**，只实时推给客户端；
    // 只有最终的 ai_reply 才走正常持久化。这是流式输出与可靠性契约的
    // 关键妥协：否则每个 token 都要一次 Kafka 往返 + 一次落库。
    bool HandleHermesDelta(const std::string& payload, const std::string& kafka_key);

   private:
    // Broadcast 与 BroadcastAdmin 的共同实现，rate_limit 区分两者。
    BroadcastResult PublishBroadcast(const std::string& scope, int64_t group_id,
                                     const std::string& content_json, bool rate_limit);
    // 事务性 outbox 的三步：PrepareOutbox -> Confirm/SendAndWait -> Published。
    // **顺序不可颠倒**：先写 outbox 再发 Kafka，才能在进程崩溃后靠
    // 定时扫描重发；先发的话，崩溃就会留下一条 Kafka 里有但 outbox 里没的消息。
    bool PersistToTopic(const Message&, const std::string& scene, int64_t user1,
                        int64_t user2, int64_t room_id, std::string* error);
    bool BuildHermesRequest(const Message&, nlohmann::json*, std::string*);
    bool EnqueueHermesRequest(const Message&, std::string*);
    // 下面四个是 Agent 生命周期缓存的读写，均带容量封顶（见对应成员注释）。
    void RememberHermesMessage(const Message&);
    bool IsHermesStreamCompleted(const std::string&);
    void MarkHermesStreamCompleted(const std::string&);
    // 返回 true 表示「以前见过」，即本次是重复 delta，应该丢弃。
    bool MarkHermesDeltaSeen(const std::string&);
    bool GetActiveUser(int64_t user, User* result, std::string* error);
    bool AuthorizeMessageImages(int64_t user, const std::string& session,
                                const std::string& content, std::string* error);
    bool AgentRoutes(int64_t room, int64_t user,
                     std::map<std::string, std::vector<int64_t>>* routes);

    ConversationStore* store_;
    // 为空时直接用 persist_producer_。可注入只是为了单测。
    MessageEventPublisher* persistence_publisher_{nullptr};
    // 按 session_id 分段的锁：同会话串行、跨会话并行。
    // 这是 Submit 阶段 2 的实现，weak_ptr 自动回收避开无限增长。
    KeyedMutex submission_mutexes_;
    GroupMemberDao* group_member_dao_;
    UserService* user_service_;
    // 四个 producer 对应四个 topic，不要互相代用。
    KafkaProducer* producer_;
    KafkaProducer* group_producer_;
    KafkaProducer* broadcast_producer_;
    KafkaProducer* persist_producer_;
    KafkaProducer* hermes_request_producer_;
    RedisStore* redis_store_;
    AttachmentService* attachment_service_;
    RoomAgentService* room_agent_service_;
    SceneRateLimiter rate_limiter_;
    int persist_kafka_timeout_ms_;
    bool hermes_enabled_;
    int64_t hermes_bot_user_id_;
    // bot_user_id -> bot 名称。允许多个 Agent bot 共存，
    // 每个会话可以绑到不同的 bot。
    std::map<int64_t, std::string> agent_bot_users_;

    // 下面三组缓存都是进程内的，**重启就丢**。它们只用于去重与上下文拼接，
    // 丢了不会造成消息丢失，只会多一次数据库读取或一次重复 delta。
    // 这也是它们能安全地放在内存里的前提：不要往里面放权威状态。

    // 每会话的近期消息，用于拼 Agent 上下文（避开每次查库）。
    // 封顶 1024 个会话。注意淘汰用的是 unordered_map::begin()，
    // 那**不是 LRU**而是任意一个桶：淘汰对象不确定。
    // 对当前用途（只是少一次查库）无害，但不要把它当 LRU 缓存依赖。
    std::mutex hermes_cache_mu_;
    std::unordered_map<std::string, std::deque<Message>> hermes_history_cache_;

    // 已完成的流式 request_id。set + order 双结构实现 FIFO 淘汰，
    // 上限 100000。set 用来 O(1) 查重，deque 用来知道该淘汰谁。
    std::mutex hermes_stream_mu_;
    std::unordered_set<std::string> hermes_stream_completed_;
    std::deque<std::string> hermes_stream_completed_order_;

    // 已见过的 delta_id，上限 200000。delta 比完整回复多得多，
    // 因此上限比上面高一个量级。
    // 同一个 hermes_stream_mu_ 保护：两组缓存的读写总是成对发生，
    // 共用一把锁避开两把锁的加锁顺序问题。
    std::unordered_set<std::string> hermes_delta_seen_;
    std::deque<std::string> hermes_delta_seen_order_;
};
}  // namespace sparkpush
