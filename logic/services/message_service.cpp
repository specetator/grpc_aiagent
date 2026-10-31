#include "services/message_service.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <thread>

#include "agent_event.h"
#include "citation_validation.h"
#include "conversation_store.h"
#include "group_dao.h"
#include "hermes_reply.h"
#include "image_attachment.h"
#include "kafka_producer.h"
#include "logging.h"
#include "message_codec.h"
#include "metrics.h"
#include "redis_store.h"
#include "services/attachment_service.h"
#include "services/business_rules.h"
#include "services/user_service.h"
#include "services/room_agent_service.h"
#include "text_metrics.h"

namespace sparkpush {

namespace {

// 从 content_json 里取出纯文本。容忍**三种**历史形式：
//   "hello"                                  content_json 直接就是字符串
//   {"content": "hello"}                     content 是字符串
//   {"content": {"text": "hello"}}           content 是对象（当前形式）
//
// 为什么要容忍三种：本函数读的是**已落库的历史**，而那些行是在不同
// 时期写入的。只支持最新形式会让拼 prompt 时静默丢掉旧消息，
// 用户看到的就是“Agent 忘了我们之前说什么”。
//
// 取不到时返回空串而不是报错：调用方（BuildHermesRequest）会跳过
// 空文本的行，因此一条格式奇怪的历史不会弄坏整个 prompt。
std::string ExtractTextFromContentJson(const std::string& content_json) {
    if (content_json.empty()) return {};
    try {
        const auto value = nlohmann::json::parse(content_json);
        if (value.is_string()) return value.get<std::string>();
        if (!value.is_object() || !value.contains("content")) return {};
        const auto& content = value.at("content");
        if (content.is_string()) return content.get<std::string>();
        if (content.is_object() && content.contains("text") &&
            content.at("text").is_string()) {
            return content.at("text").get<std::string>();
        }
    } catch (const nlohmann::json::exception&) {
        // 静默吞掉解析失败：历史数据不可信，而本函数的失败
        // 语义就是“这条没文本”。外层兼容网也接不到这里。
    }
    return {};
}

// 从一条 **bot 回复**里提取会话级元数据（上下文起点、模型覆盖、
// 是否命令回复）。与 hermes_command.h 里 HermesConversationEntry 的
// 那组 has_* / model / provider 字段一一对应。
//
// 开头三个短路条件定义了这个函数的适用范围：
//   !entry->assistant    只看 bot 回复。用户消息里即使写了
//                        hermes_model_override 也会被忽略——这正是
//                        “用户命令只是意图，不是确认”的具体落地。
//   content_json.empty() 空行没东西可提。
//
// 这些字段之所以存在消息正文里而不是单独一张表，理由见
// hermes_command.h 里对 HermesConversationEntry 的说明。
void ExtractHermesEntryMetadata(const std::string& content_json,
                                HermesConversationEntry* entry) {
    if (!entry || !entry->assistant || content_json.empty()) return;
    try {
        const auto value = nlohmann::json::parse(content_json);
        if (!value.is_object() || !value.contains("content") ||
            !value.at("content").is_object()) {
            return;
        }
        const auto& content = value.at("content");
        if (content.contains("hermes_context_start_seq") &&
            content["hermes_context_start_seq"].is_number_integer()) {
            entry->context_start_seq =
                content["hermes_context_start_seq"].get<int64_t>();
            // >= 0 才算“有效状态”：负值被当成哨兵（意为“清除边界”），
            // 因此 has_context_state 保持 false，不会把一个负 seq 当起点用。
            entry->has_context_state = entry->context_start_seq >= 0;
        }
        entry->command_reply = content.value("source", "") == "hermes_command";
        // Compatibility only for the exact successful legacy /new reply.
        // New cards and failed/preview commands never infer a boundary from text.
        //
        // 译：仅为了兼容那种**完全匹配**的旧版 /new 成功回复。
        // 新卡片、以及失败/预览类命令，绝不从正文文本去推断上下文边界。
        //
        // 为何需要它：旧版 /new 的回复没有写 hermes_context_start_seq 字段，
        // 只有一句固定文案。不兼容就会让老会话的上下文边界丢失，
        // 用户明明发过 /new，Agent 却仍然带着之前的历史。
        //
        // 五个条件必须**全部**成立，少一个就不算：
        //   command_reply            得是命令回复
        //   !has_context_state       新版字段不在（在的话用新版，不猜）
        //   !contains("agent_event") 不是卡片类回复
        //   hermes_command == "new"   得是 /new（/reset /clear 已归一到 new）
        //   text 以固定文案开头    rfind(prefix, 0) == 0 就是 starts_with
        // 最后一条是“从文本推断”的，因此把前面四条当安全网：
        // 只有四个结构化条件都排除了其他可能，才允许去看文本。
        entry->legacy_new_confirmation =
            entry->command_reply && !entry->has_context_state &&
            !content.contains("agent_event") &&
            content.value("hermes_command", "") == "new" &&
            entry->text.rfind("已开启新的 Pi Agent 上下文。", 0) == 0;
        // 模型状态快照。默认值给 true 而不是 false 很关键：
        // value("hermes_model_state_confirmed", true) 意味着“字段不在时
        // 当成已确认”。旧版回复没有这个字段，但它们确实是已确认的状态；
        // 若默认给 false，所有旧回复里的模型快照都会被忽略。
        // 只有显式写 confirmed=false 的（预览/失败卡片）才被排除。
        if (content.value("hermes_model_state_confirmed", true) &&
            content.contains("hermes_model_override") &&
            content.at("hermes_model_override").is_boolean()) {
            entry->has_model_state = true;
            entry->model_override = content.at("hermes_model_override").get<bool>();
            entry->model = content.value("hermes_model", "");
            entry->provider = content.value("hermes_provider", "");
        }
    } catch (const nlohmann::json::exception&) {
    }
}

}  // namespace

// 构造：初始化逻辑服务依赖；持久化由独立 Kafka topic 承担。
MessageService::MessageService(
    ConversationStore* store, GroupMemberDao* group_member_dao,
    UserService* user_service, KafkaProducer* producer, KafkaProducer* group_producer,
    KafkaProducer* broadcast_producer, KafkaProducer* persist_producer,
    RedisStore* redis_store, const RateLimitConfig& rate_limit,
    int persist_kafka_timeout_ms, KafkaProducer* hermes_request_producer,
    bool hermes_enabled, int64_t hermes_bot_user_id,
    std::map<int64_t, std::string> agent_bot_users,
    AttachmentService* attachment_service, RoomAgentService* room_agents,
    MessageEventPublisher* persistence_publisher)
    : store_(store),
      persistence_publisher_(persistence_publisher),
      group_member_dao_(group_member_dao),
      user_service_(user_service),
      producer_(producer),
      group_producer_(group_producer),
      broadcast_producer_(broadcast_producer),
      persist_producer_(persist_producer),
      hermes_request_producer_(hermes_request_producer),
      redis_store_(redis_store),
      attachment_service_(attachment_service),
      room_agent_service_(room_agents),
      rate_limiter_(rate_limit),
      persist_kafka_timeout_ms_(persist_kafka_timeout_ms),
      hermes_enabled_(hermes_enabled),
      hermes_bot_user_id_(hermes_bot_user_id),
      agent_bot_users_(std::move(agent_bot_users)) {
    // 把 Hermes bot 自己也注册进 agent_bot_users_，而且**无条件覆盖**：
    // 配置里若给同一个 id 配了别的名字，以代码里的 kHermesBotName 为准。
    // 这个 map 的用途是“识别哪些 user_id 是 Agent 而不是人”：
    // BuildHermesRequest 靠它判断一条消息该发给哪个 bot，
    // 历史拼接靠它决定 assistant 标志（进而决定要不要提元数据）。
    // 漏了 Hermes 自己就会把它当成普通用户，自己的回复会被当成
    // 用户提问拼进 prompt，形成自我强化的胡说八道。
    agent_bot_users_[hermes_bot_user_id] = kHermesBotName;
}

// 下面三个是转发给 UserService / RedisStore 的小包装。
// 包一层的理由：让 Comet 侧的 gRPC handler 只依赖 MessageService，
// 不需要同时拿到 UserService 与 RedisStore 两个指针（见 grpc_service.cpp）。
//
// GetActiveUser 走的是 UserService 里那个带 2s 缓存的热路径（而
// Authenticate 永远绕过缓存），因此封禁一个用户后，已建立的 WS 连接
// 最多还会活 2 秒。这是有意的性能取舍，详见 user_service.h。
bool MessageService::GetActiveUser(int64_t user, User* result, std::string* error) {
    return user_service_ && user_service_->GetActiveUser(user, result, error);
}
// 返回路由代数（generation）；<= 0 表示注册失败（Redis 不可用）。
// generation 与 token 一起构成“只有当前这条连接能撤销自己的路由”的凭证：
// RemoveRouteGeneration 只在两者都对得上时才真摧，避开“迟到的下线上报
// 误删新连接的路由”（详见 redis_store.h 里的 AddRouteGeneration 说明）。
int64_t MessageService::AddAuthenticatedRoute(int64_t user, const std::string& comet, std::string* token) {
    int64_t generation = 0;
    if (redis_store_) redis_store_->AddRouteGeneration(user, comet, &generation, token);
    return generation;
}
// 返回 void：下线清理是尽力而为的。Redis 挂了也得让 Comet 把本地连接清掉，
// 否则 Comet 的 user_conns_ 会泄漏。
//
// 注意（诚实标注）：route:* 系列键**没有 TTL**（见 redis_store.cpp 的
// AddRouteGeneration，那个 Lua 脚本里只有 INCR/SET/SADD，没有 EXPIRE）。
// 它们只能被两条路径清掉：本函数（带 generation+token 的显式下线上报），
// 或管理员吊销 token 时的 DEL route:user:<uid>。
// 因此一个 Comet 进程被 kill -9 时会留下陈旧路由。影响是有界的：
// 往一个已死的 comet 下推只会失败（记指标），而客户端重连后会拿到
// 新的 generation，缺失的消息由 SyncOffline 按 delivered_seq 水位补齐。
// 也就是说真正的恢复机制是“重连 + 补推”，而不是路由表的自洁。
void MessageService::UserOffline(int64_t user, const std::string& comet, int64_t generation, const std::string& token) {
    if (redis_store_) redis_store_->RemoveRouteGeneration(user, comet, generation, token);
}

// 事务性 outbox 模式：保证「消息已 accepted」与「事件已进 Kafka」等价。
//
// 三步必须严格按这个顺序：
//   1) PrepareOutbox  先把 payload 写入 MySQL message_reservation（状态=prepared）
//   2) SendAndWait    同步等 Kafka delivery report
//   3) Published      把 reservation 标为已发布
//
// 为什么不能先 Send 再写库：那样会在「Kafka 已收到、MySQL 未记录」
// 的窗口里崩溃，事件无人知晓，无法重发。
// 为什么不能先写库再异步 Send：那样 accepted_ack 就失去了意义。
//
// 崩溃恢复：停在 prepared 状态的记录由 main.cpp 的 outbox_recovery
// 定时器（1s）扫出来重发。重发用同一个 msg_id，Job 侧幂等落库，
// 因此最终语义是「至少一次事件 + 幂等消费」，不是 exactly-once。
//
// persistence_publisher_ 是为了单测可注入（mock 掉真实 Kafka）；
// 生产路径走 persist_producer_->SendAndWait。
bool MessageService::PersistToTopic(const Message& message, const std::string& scene,
                                    int64_t user1, int64_t user2, int64_t room_id,
                                    std::string* err) {
    if (!persist_producer_ && !persistence_publisher_) {
        if (err) *err = "persist Kafka producer not initialized";
        return false;
    }
    PersistMessageRequest request;
    // 注意这里发的是 **protobuf 二进制**而不是 JSON：persist topic 是
    // 纯内部信道（Logic -> Job），没有人类可读需求，二进制更小也更
    // 不容易出现“字段名写错”这类静默故障。对比：push_single/push_group
    // 走的也是 protobuf，而 hermes 的 request/reply topic 走 JSON
    //（因为 hermes_bridge 是 Python 端，不想引入 protobuf 依赖）。
    //
    // 除了消息本体，还额外带了 scene / user1_id / user2_id / room_id：
    // Job 落库时需要它们建会话行（单聊要两个 uid，房间要 room_id），
    // 而仅靠 session_id 字符串反推是脆弱的。
    FillChatMessage(message, request.mutable_message());
    request.set_scene(scene);
    request.set_user1_id(user1);
    request.set_user2_id(user2);
    request.set_room_id(room_id);
    std::string payload;
    if (!request.SerializeToString(&payload)) {
        if (err) *err = "serialize PersistMessageRequest failed";
        return false;
    }
    // 先写 outbox 再发 Kafka（三步里的第一步）。PrepareOutbox 是一个 CAS：
    // 已经用**不同** payload 预备过的记录不会被覆盖，随后的一致性回读
    // 会发现不一致并报冲突（详见 message_reservation.cpp）。
    if (!store_ || !store_->PrepareOutbox(message, payload, err)) return false;
    // Kafka 的 **消息键 = session_id**，这是全系统消息有序的关键：
    // Kafka 只保证单分区内有序，而按 session_id 做键能把同一个会话的
    // 所有消息映射到同一个分区。换成用 msg_id 或空键就会让同会话消息
    // 散到不同分区，Job 并发消费时落库顺序就会乱。
    const bool confirmed = persistence_publisher_
        ? persistence_publisher_->Confirm(message.session_id, payload, persist_kafka_timeout_ms_)
        : persist_producer_->SendAndWait(message.session_id, payload, persist_kafka_timeout_ms_);
    if (!confirmed) {
        // 注意此时 outbox 已经写了（stage=1）但**不回滚**：这正是设计意图。
        // 回滚反而会造成消息丢失（序号已分配、预约已作废，而客户端
        // 可能已经拿到了序号）。留在 stage=1 让 outbox_recovery 重发。
        // 返回 false 会让 Submit 不回 accepted_ack，客户端会重试，
        // 而重试带着同一个 client_msg_id，因此序号分配那一步会幂等命中。
        if (err) *err = "persist topic delivery failed";
        return false;
    }
    // 第三步：标为已发布。失败时消息其实已经安全进了 Kafka，
    // 因此这里返回 false 是“保守方向”：宁可让 outbox_recovery 重发一次
    //（Job 侧幂等），也不能让一条已发布的消息被当成未发布而永远卡在 stage=1。
    if (!store_->Published(message, err)) return false;
    MetricsRegistry::Instance().Increment("spark_push_persist_events_accepted_total");
    return true;
}

// Hermes 上下文缓存：记住本进程最近经手的消息（每会话最多 100 条，
// 最多 1024 个会话）。
//
// 存在的理由：Job 的 persist consumer 可能还没把上一轮消息写入 MySQL，
// 如果只查 MySQL 拼 Prompt，会漏掉刚刚发生的对话。这里用进程内缓存
// 补上这个时间窗口。
//
// 同 msg_id 直接覆盖而不是追加：同一条消息可能被 Job 竞争性地写入
// MySQL 后又从缓存路径进来，覆盖保证不出现重复轮次。
void MessageService::RememberHermesMessage(const Message& message) {
    if (message.session_id.empty() || message.msg_id.empty()) return;
    std::lock_guard<std::mutex> lock(hermes_cache_mu_);
    auto it = hermes_history_cache_.find(message.session_id);
    if (it == hermes_history_cache_.end()) {
        if (hermes_history_cache_.size() >= 1024) {
            hermes_history_cache_.erase(hermes_history_cache_.begin());
        }
        it = hermes_history_cache_.emplace(message.session_id, std::deque<Message>{})
                 .first;
    }
    for (auto& cached : it->second) {
        if (cached.msg_id == message.msg_id) {
            cached = message;
            return;
        }
    }
    it->second.push_back(message);
    while (it->second.size() > 100) it->second.pop_front();
}

bool MessageService::IsHermesStreamCompleted(const std::string& request_id) {
    if (request_id.empty()) return false;
    std::lock_guard<std::mutex> lock(hermes_stream_mu_);
    return hermes_stream_completed_.find(request_id) != hermes_stream_completed_.end();
}

void MessageService::MarkHermesStreamCompleted(const std::string& request_id) {
    if (request_id.empty()) return;
    std::lock_guard<std::mutex> lock(hermes_stream_mu_);
    if (!hermes_stream_completed_.insert(request_id).second) return;
    hermes_stream_completed_order_.push_back(request_id);
    constexpr size_t kCompletedLimit = 100000;
    while (hermes_stream_completed_order_.size() > kCompletedLimit) {
        hermes_stream_completed_.erase(hermes_stream_completed_order_.front());
        hermes_stream_completed_order_.pop_front();
    }
}

// 返回 true 表示该增量已经见过或其最终消息已经处理；返回 false 表示本次
// 首次登记。增量是体验数据，允许在进程重启后丢失，最终 ai_reply 仍可补齐。
bool MessageService::MarkHermesDeltaSeen(const std::string& delta_id) {
    if (delta_id.empty()) return true;
    std::lock_guard<std::mutex> lock(hermes_stream_mu_);
    if (hermes_stream_completed_.find(delta_id) != hermes_stream_completed_.end()) {
        return true;
    }
    if (!hermes_delta_seen_.insert(delta_id).second) return true;
    hermes_delta_seen_order_.push_back(delta_id);
    constexpr size_t kDeltaLimit = 200000;
    while (hermes_delta_seen_order_.size() > kDeltaLimit) {
        hermes_delta_seen_.erase(hermes_delta_seen_order_.front());
        hermes_delta_seen_order_.pop_front();
    }
    return false;
}

// 把一条已分配序号的用户消息，组装成一份可以直接发给 Agent 的 JSON 请求。
//
// 两条完全不同的分支（靠 session_id 前缀区分）：
//   "r_" 开头  房间共享 Agent。历史取 20 条，然后**整个委派给**
//                RoomAgentService::BuildRequest（它要处理任务 scope、
//                can_develop、[user_id=N] 前缀、提示注入防护等一堆
//                房间特有的东西，不应该在这里重复一遍）。
//   "s_" 开头  单聊 Agent。下面那一大段就是这条路径。
//
// 单聊路径的五步：
//   1. 找出对端是哪个 bot（靠重建期望的 session_id 来匹配，不是靠解析）
//   2. 拉 MySQL 历史（最新 50 条）
//   3. 与进程内缓存合并去重（补 Job 还没落库的那个窗口）
//   4. 排序 + 裁到最新 50 条 + 投影成 HermesConversationEntry
//   5. 交给 BuildHermesPromptPlan 做命令识别与预算裁剪，再拼成 JSON
//
// 输出的 request_id = current.msg_id（确定性拼接），这是下游幂等的前提：
// 同一条消息重试时 request_id 相同，hermes_bridge 与 PutRequest 都靠它去重。
bool MessageService::BuildHermesRequest(const Message& current, nlohmann::json* request,
                                        std::string* err) {
    if (current.session_id.rfind("r_", 0) == 0) {
        std::vector<Message> history;
        // 房间分支只取 20 条（单聊是 50）：房间是多人共享的，
        // 历史里大部分是与当前提问者无关的闲聊，而且 room_agent 那边
        // 还会再截一次（每行 1000 字节、整体 24000 字节）。
        if (!room_agent_service_ || !store_->GetHistory(current.session_id, 0, 20, &history, err))
            return false;
        {
            // 同样要把进程内缓存合并进去，理由与下面单聊路径完全相同。
            // 去重靠 msg_id：MySQL 里已经有了的就不重复加。
            std::lock_guard<std::mutex> lock(hermes_cache_mu_);
            const auto cached = hermes_history_cache_.find(current.session_id);
            if (cached != hermes_history_cache_.end())
                for (const auto& item : cached->second) {
                    if (std::none_of(history.begin(), history.end(), [&](const Message& h) { return h.msg_id == item.msg_id; }))
                        history.push_back(item);
                }
        }
        // First input can precede creation of the room session by Job.
        //
        // 译：第一条输入可能早于 Job 创建房间会话行。
        //
        // 也就是说：用户在房间里发的第一条消息到达这里时，
        // session 行可能还不存在、历史也可能是空的。这是**正常情况**
        // 而不是错误，因此 RoomAgentService::BuildRequest 必须能在
        // 空历史上工作（只拿当前这条提问拼 prompt）。
        return room_agent_service_->BuildRequests(current, history, request, err);
    }
    if (!request) {
        if (err) *err = "Hermes request output is null";
        return false;
    }
    // 找出当前会话对应的是哪个 bot。
    //
    // 注意做法是**正向重建期望值再比对**，而不是反向解析 session_id
    // 取出两个 uid 再看哪个是 bot。两者结果看似相同，但正向重建额外
    // 强制了 session_id 必须是**规范形式**（min 在前、max 在后）：
    // 一个手写的 "s_<bot>_<user>"（顺序反了）在这里不会匹配，
    // 而反向解析就会把它当成合法会话。
    // 这也是为何单聊会话 id 在全系统里都由同一套规则生成。
    int64_t bot_user_id = 0;
    for (const auto& bot : agent_bot_users_) {
        const auto expected =
            "s_" + std::to_string(std::min(current.sender_id, bot.first)) + "_" +
            std::to_string(std::max(current.sender_id, bot.first));
        if (current.session_id == expected) {
            bot_user_id = bot.first;
            break;
        }
    }
    // 靠 0 当哨兵：前提是没有任何 bot 的 user_id 是 0。
    // 配置里的 agent_bot_users 条目确实经过校验（common/config.cpp 里
    // `if (id > 0 && !name.empty() && ...)`），因此那部分前提成立。
    // 但有一个缝隙（诚实标注）：hermes_bot_user_id 是 `std::stoll(value)`
    // 直接赋的，**没有做 >0 校验**，而构造函数里又无条件把它写进
    // agent_bot_users_。若运维把它配成 0，上面循环就会匹配到 id=0，
    // 而下面的 `!bot_user_id` 又会把它当成“没找到 bot”而报
    // "invalid Agent conversation"，现象是 Hermes 单聊整体失效。
    // 修法是在 config.cpp 里给 hermes_bot_user_id 加一个 >0 校验。
    // 不匹配意味着“这不是一个与 Agent 的单聊”，不应该走 AI 路径。
    if (!bot_user_id) {
        if (err) *err = "invalid Agent conversation";
        return false;
    }

    // ConversationStore::GetHistory 已将 MessageDao 的倒序查询翻转为
    // msg_seq 升序。按这个顺序交给 Hermes，保证多轮上下文与会话顺序一致。
    // 当前消息可能尚未被 Job 落盘，因此最后显式补入 current。
    std::vector<Message> history;
    std::string history_error;
    if (store_->GetHistory(current.session_id, 0, 50, &history, &history_error)) {
    } else {
        // 历史查询失败不应悄悄伪装成“无历史”。当前请求仍可发送，
        // 但把原因记录到日志，方便区分首次对话与数据库异常。
        LOG_WARN << "Build Hermes history failed for session " << current.session_id
                 << ": " << history_error;
    }

    // 当前消息可能已经被 Job 竞争性地写入 MySQL；它会在函数末尾显式追加，
    // 所以先按 msg_id/client_msg_id 排除，避免同一轮用户输入被放进 Prompt 两次。
    const auto same_message = [](const Message& lhs, const Message& rhs) {
        return (!lhs.msg_id.empty() && lhs.msg_id == rhs.msg_id) ||
               (!lhs.client_msg_id.empty() && lhs.client_msg_id == rhs.client_msg_id);
    };
    history.erase(std::remove_if(
                      history.begin(), history.end(),
                      [&](const Message& item) { return same_message(item, current); }),
                  history.end());

    // Job 的 persist consumer 可能还没把上一轮消息写入 MySQL。将本进程
    // 最近的 Hermes 消息与 MySQL 历史按 msg_id/client_msg_id 合并，覆盖这个
    // 短暂窗口；进程重启后仍以 MySQL 为事实来源。
    {
        std::lock_guard<std::mutex> lock(hermes_cache_mu_);
        const auto cache_it = hermes_history_cache_.find(current.session_id);
        if (cache_it != hermes_history_cache_.end()) {
            for (const auto& cached : cache_it->second) {
                if (same_message(cached, current)) continue;
                const bool already_present = std::any_of(
                    history.begin(), history.end(),
                    [&](const Message& item) { return same_message(item, cached); });
                if (!already_present) history.push_back(cached);
            }
        }
    }
    // 排序的 tiebreaker 用 msg_id 而不只用 msg_seq：同一会话内 msg_seq
    // 理论上唯一，但缓存条目与 MySQL 条目合并后，若出现了
    // 序号相同而 msg_id 不同的脏数据，没有 tiebreaker 就会让排序结果
    // 不确定（std::sort 不是稳定排序），同一输入两次拼出不同的 prompt。
    std::sort(history.begin(), history.end(),
              [](const Message& lhs, const Message& rhs) {
                  if (lhs.msg_seq != rhs.msg_seq) {
                      return lhs.msg_seq < rhs.msg_seq;
                  }
                  return lhs.msg_id < rhs.msg_id;
              });
    // 裁到**最新的** 50 条（从头部删），与 LimitPrompt 的裁剪方向一致：
    // 丢最旧的、保留最新的。反过来（保留最旧）会让 Agent 永远看不到
    // 用户刚说的话，而旧上下文的价值远低于新上下文。
    if (history.size() > 50) {
        history.erase(history.begin(), history.end() - 50);
    }

    std::string current_text = ExtractTextFromContentJson(current.content_json);
    const auto current_images = ExtractImageRefsFromContentJson(current.content_json);
    if (current_text.empty() && current_images.empty()) {
        if (err) *err = "Hermes only supports text or image messages";
        return false;
    }
    // 纯图片消息也要给一句文本：prompt 的 messages[] 里 content 不能为空，
    // 否则模型只看到一堆附件引用而不知道用户想干什么。
    // 固定文案而不是报错：发图不说话是一个完全正常的用户行为。
    if (current_text.empty()) current_text = "请查看这张图片。";

    // 投影成 HermesConversationEntry：把 Message 压成拼 prompt 所需的最小
    // 字段集，让 BuildHermesPromptPlan 保持为纯函数（不依赖 DAO/JSON）。
    std::vector<HermesConversationEntry> entries;
    entries.reserve(history.size());
    for (const auto& item : history) {
        HermesConversationEntry entry;
        entry.msg_seq = item.msg_seq;
        // assistant 标志靠 **sender_id 是不是 bot** 判定，而不是靠 msg_type。
        // 理由：历史数据里的 msg_type 并不统一（旧行可能是 "text"），
        // 而 sender_id 是权威的。这个标志直接决定了下游要不要提元数据、
        // 以及拼成 "assistant" 还是 "user" 角色，弄反就会让模型把自己的
        // 回复当成用户输入。
        entry.assistant = item.sender_id == bot_user_id;
        entry.text = ExtractTextFromContentJson(item.content_json);
        ExtractHermesEntryMetadata(item.content_json, &entry);
        entries.push_back(std::move(entry));
    }

    // 命令规划器负责过滤本地命令、建立 /new 边界、重写 /retry 上下文，
    // 并在约 12 KiB 的预算内保留最近对话。
    const HermesPromptPlan plan =
        BuildHermesPromptPlan(entries, current_text, current.msg_seq);
    MetricsRegistry::Instance().Observe("spark_push_hermes_prompt_chars",
                                        static_cast<int64_t>(plan.prompt_chars));
    MetricsRegistry::Instance().Observe("spark_push_hermes_prompt_messages",
                                        static_cast<int64_t>(plan.messages.size()));

    *request = {
        {"request_id", current.msg_id},
        {"message_seq", current.msg_seq},
        {"session_id", current.session_id},
        {"user_id", current.sender_id},
        {"bot_user_id", bot_user_id},
        {"client_msg_id", current.client_msg_id},
        {"messages", nlohmann::json::array()},
        {"call_model", plan.call_model},
        {"force_knowledge_retrieval", plan.force_knowledge_retrieval},
        {"model_override", plan.model_override},
        {"context_start_seq", plan.context_start_seq},
        {"context_message_count", plan.context_message_count},
        {"prompt_chars", plan.prompt_chars},
    };
    for (const auto& item : plan.messages) {
        (*request)["messages"].push_back(
            {{"role", item.first}, {"content", item.second}});
    }
    if (plan.model_override) {
        (*request)["model"] = plan.model;
        if (!plan.provider.empty()) (*request)["provider"] = plan.provider;
    }
    if (plan.command.kind != HermesCommandKind::kNone) {
        (*request)["command"] = {
            {"name", HermesCommandName(plan.command.kind)},
            {"input_name", plan.command.name},
            {"arguments", plan.command.arguments},
            {"local_error", plan.local_error},
        };
    }
    if (!current_images.empty()) {
        nlohmann::json images = nlohmann::json::array();
        for (const auto& image : current_images) {
            images.push_back({{"attachment_id", image.id},
                              {"mime", image.mime},
                              {"name", image.name}});
        }
        (*request)["images"] = std::move(images);
    }
    const auto current_metrics = MeasureText(current_text);
    if (LengthAuditEnabled()) {
        LOG_INFO << "length_audit stage=logic_request request_id=" << current.msg_id
                 << " provider="
                 << (plan.provider.empty() ? "adapter-state" : plan.provider)
                 << " model=" << (plan.model.empty() ? "adapter-state" : plan.model)
                 << " stream=profile-default max_tokens=unset max_output_tokens=unset"
                 << " prompt_bytes=" << plan.prompt_chars
                 << " prompt_chars=" << current_metrics.unicode_chars
                 << " prompt_message_count=" << plan.messages.size()
                 << " context_start_seq=" << plan.context_start_seq
                 << " context_message_count=" << plan.context_message_count;
    }
    return true;
}

// 图片引用的权限校验。这里处理的是一个很具体的提权风险：
// 消息正文里的 attachment_id 是客户端可控的，若不校，一个用户就能
// 在自己发的消息里引用**别人会话里的图**，然后靠消息广播让
// 整个会话的人都看到它（或通过 Agent 把图读出来）。
//
// attachment_service_ 为空时的行为值得注意：只有当正文里**确实带了**
// 图片引用才报错。没带图的消息应该照常通过——附件功能没开启
// 不应该把普通文本聊天也弄坏（fail-open 于无关路径，fail-closed 于相关路径）。
bool MessageService::AuthorizeMessageImages(int64_t sender, const std::string& session,
                                            const std::string& content,
                                            std::string* error) {
    if (!attachment_service_) {
        if (ExtractImageRefsFromContentJson(content).empty()) return true;
        if (error) *error = "image attachments are not enabled";
        return false;
    }
    const auto status =
        attachment_service_->AuthorizeMessageImages(sender, session, content);
    if (error) *error = status.ok() ? "" : status.message;
    return status.ok();
}

// 把请求发到 ai_request topic，由 hermes_bridge 消费。
//
// 注意本函数用的是 **SendAndWait（同步等 delivery report）**而不是异步 Send：
// 调用方需要知道“请求到底交没交给 Agent”。若异步发，用户发完提问
// 就会永久等一个不会来的回复（而服务端以为发出去了）。
// 代价是本函数会阻塞到 persist_kafka_timeout_ms，而它在 Submit 的
// 会话级串行锁里跑——因此 Kafka 卡顶时会拖慢同会话的后续消息。
//
// 消息键同样用 session_id：保证同一会话的多轮提问按序到达 Agent。
// 这一点比持久化路径上更关键：Agent 是有状态的，乱序到达会让
// 上下文拼接出错。
bool MessageService::EnqueueHermesRequest(const Message& current, std::string* err) {
    if (!hermes_request_producer_) {
        if (err) *err = "Hermes request Kafka producer not initialized";
        return false;
    }
    nlohmann::json request;
    if (!BuildHermesRequest(current, &request, err)) return false;
    const auto requests=request.is_array()?request:nlohmann::json::array({request});
    for(const auto& item:requests) {
        // Generic room requests are dispatched by the durable source-gated outbox.
        if(item.value("collaboration",nlohmann::json::object()).value("kind","")=="room_agent_v1") continue;
        if(!hermes_request_producer_->SendAndWait(current.session_id,item.dump(),persist_kafka_timeout_ms_)) {
            if(err)*err="Hermes request topic delivery failed";return false;
        }
        MetricsRegistry::Instance().Increment("spark_push_hermes_requests_total");
    }
    return true;
}

bool MessageService::RecoverRoomAgentRequests(std::string* error) {
    if(!room_agent_service_ || !hermes_request_producer_)return true;
    nlohmann::json pending;
    if(!room_agent_service_->Pending(&pending,error))return false;
    for(const auto& request:pending) {
        const auto access=room_agent_service_->CheckDispatch(request);
        if(!access.ok()) {
            if(access.code!=403)return false; // Transient auth/storage failure: retry, never deny permanently.
            if(!room_agent_service_->RejectPending(request,access.message,error))return false;
            continue;
        }
        const auto key=request.at("session_id").get<std::string>()+":"+request.at("agent_id").get<std::string>();
        if(!room_agent_service_->MarkAttempt(request.at("request_id"),error))return false;
        if(!hermes_request_producer_->SendAndWait(key,request.dump(),persist_kafka_timeout_ms_))return false;
        if(!room_agent_service_->Dispatched(request.at("request_id"),error))return false;
    }
    return true;
}

// 处理上行消息：认证、会话顺序、持久化原文/序号预留、Kafka 确认、异步历史落盘。
// =====================================================================
// Submit：整个系统最核心的函数，一条上行消息从接收到 accepted 的全路径。
//
// 调用方：LogicServiceImpl 的 Unary(SendUpstreamMessage) 与
//         Streaming(MessageStream) 两个适配器，以及 HTTP 接口。
//         三者共享本函数，保证业务语义一致。
//
// 八个阶段（任一步失败都直接 return，已完成的副作用不回滚）：
//   1. 格式门禁    64 KiB 上限 + 必须是 JSON object
//   2. 会话级串行  KeyedMutex 按 session 加锁，8s 拿不到锁回 503
//   3. 鉴权        bearer 解出 actor，必须与 from_user_id 一致
//   4. 限流        按 scene 分档的令牌桶，超限 429
//   5. 路由解析    算出 session_id，并收集 comet_id -> [user_id] 映射
//   6. 序号分配    Redis Lua 原子完成去重 + INCR（热路径不写 MySQL）
//   7. 持久化      persist_message 必须 SendAndWait 成功才回 accepted_ack
//   8. 实时投递    异步写 push_single/push_group，失败不影响 accepted
//
// 最重要的不变量：阶段 7 是 accepted 的唯一边界。一旦它成功，
// 即使 Logic 立即崩溃、阶段 8 完全没做，消息也不会丢——Job 会从
// persist topic 重放落库，客户端重连后由 SyncOffline 补齐。
// 反过来，阶段 8 失败只记指标 + 返回「待投递」，不给假 ACK。
UpstreamResult MessageService::Submit(const UpstreamCommand& request) {
    UpstreamResult result;
    if (!store_) {
        result.status = ServiceStatus{500, "store not initialized"};
        return result;
    }
    const std::string& scene = request.scene;
    // 阶段 1：格式门禁。先卡字节数再解析，避免对巨型负载做 JSON 解析。
    // parse 的第三参 false = 不抛异常，失败时返回 discarded 值。
    if (request.content_json.size() > 64 * 1024) {
        result.status = {413, "message exceeds 64 KiB"};
        return result;
    }
    const auto input = nlohmann::json::parse(request.content_json, nullptr, false);
    if (input.is_discarded() || !input.is_object()) {
        result.status = {400, "message content must be a JSON object"};
        return result;
    }
    // Agent identity and execution receipts are server-owned, including nested
    // content consumed by history clients. Reject before reserving a sequence.
    auto nested_content = input.value("content", nlohmann::json());
    if (nested_content.is_string()) nested_content = nlohmann::json::parse(nested_content.get<std::string>(), nullptr, false);
    for (const auto* key : {"agent_id", "agent_name", "actor_user_id", "request_id",
                           "source_msg_id", "agent_status", "agent_event", "collaboration",
                           "bot_user_id", "task_id", "agent_command_result"}) {
        if (input.contains(key) || (nested_content.is_object() && nested_content.contains(key))) {
            result.status = {400, "server-owned Agent metadata is not accepted"};
            return result;
        }
    }
    // 阶段 2：会话级串行锁。
    //
    // 为什么需要：Redis Lua 已经保证了序号分配的原子性，但一个完整
    // Submit 还包含路由查询、outbox 写入、Kafka 发送。同一会话的并发
    // 提交如果不串行，Kafka 分区内的消息顺序会与 msg_seq 顺序不一致，
    // 下游消费时会看到乱序。
    //
    // 锁粒度是会话而不是全局：单聊键用 (min,max) 归一化，保证 A->B 和
    // B->A 竞争同一把锁（它们本来就是同一个 session）。
    // KeyedMutex 内部用 weak_ptr，无人持有时自动回收，不会无限增长。
    //
    // 用 try_lock + 5ms 自旋而不是阻塞 lock：必须在 8s 内给客户端
    // 一个明确的 503，不能让它无限期挂在「发送中」。
    auto submission_mutex = submission_mutexes_.Get(scene == "chatroom"
        ? "room:" + std::to_string(request.group_id)
        : "single:" + std::to_string(std::min(request.from_user_id, request.to_user_id)) + ":" +
          std::to_string(std::max(request.from_user_id, request.to_user_id)));
    std::unique_lock<std::mutex> submission_order(*submission_mutex, std::defer_lock);
    const auto admission_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
    while (!submission_order.try_lock()) {
        if (std::chrono::steady_clock::now() >= admission_deadline) {
            result.status = {503, "session submission queue timed out"};
            return result;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    // 阶段 3：鉴权。
    //
    // 安全关键点：actor 必须从可信凭证（auth_token）解出，而不是相信
    // 请求体里的 from_user_id。两者不一致直接 403，否则任何人都能
    // 冒充别人发消息。校验后把 from_user 覆盖为 actor，后续一律用 actor。
    //
    // 紧接的 GetActiveUser 校验账号未被封禁/删除（带 2s 短缓存，
    // 避免每条消息都打 MySQL）。
    int64_t from_user = request.from_user_id;
    if (from_user <= 0) {
        result.status = ServiceStatus{400, "from_user_id must be positive"};
        return result;
    }
    if (!user_service_ || !user_service_->UsersAvailable()) {
        result.status = ServiceStatus{500, "user dao not initialized"};
        return result;
    }
    {
        int64_t actor = 0;
        const auto auth = user_service_->Authenticate(request.auth_token,
            UserService::TokenAudience::kRealtimeUser, &actor);
        if (!auth.ok()) { result.status = auth; return result; }
        if (actor != from_user) {
            result.status = {403, "sender does not match authenticated actor"};
            return result;
        }
        from_user = actor;
    }
    User sender;
    std::string sender_error;
    if (!GetActiveUser(from_user, &sender, &sender_error)) {
        result.status = ServiceStatus{403, "sender account is disabled or deleted"};
        return result;
    }
    // client_msg_id 是幂等键，缺失就无法去重，因此强制要求。
    // 上限 128 字节防止被当成存储滥用。
    if (request.client_msg_id.empty() || request.client_msg_id.size() > 128) {
        result.status = ServiceStatus{400, "client_msg_id must contain 1..128 bytes"};
        return result;
    }

    // 阶段 4：按 scene 分档限流。
    //
    // 限流键的选择有讲究：单聊按发送者限（防单人刷），
    // 聊天室按房间限（防单房炸，否则一个大房间会拖垮全局）。
    // 三种 scene 各用独立令牌桶，广播风暴不会拖死单聊。
    const std::string rate_key = scene == "chatroom" ? std::to_string(request.group_id)
                                                     : std::to_string(from_user);
    if (!rate_limiter_.Allow(scene, rate_key)) {
        MetricsRegistry::Instance().Increment("spark_push_rate_limited_total_" + scene);
        result.status = ServiceStatus{429, "scene rate limit exceeded; retry later"};
        return result;
    }

    // 阶段 5：路由解析。输出两个东西：
    //   session_id       本地构造，不查库（单聊 s_{min}_{max}，房间 r_{room_id}）
    //   comet_to_users   comet_id -> 该节点上需要接收的 user_id 列表
    //
    // 注意 comet_to_users 为空是**正常情况**（对方全离线），
    // 不是错误；消息仍会落库，对方上线后由 SyncOffline 补推。
    std::unordered_map<std::string, std::vector<int64_t>> comet_to_users;
    std::string err;
    std::string session_id;
    int64_t single_u1 = 0, single_u2 = 0, room_id = 0;
    bool hermes_target = false;

    if (scene == "single") {
        int64_t to_user = request.to_user_id;
        if (to_user <= 0) {
            result.status = ServiceStatus{400, "to_user_id must be positive"};
            return result;
        }
        single_u1 = from_user;
        single_u2 = to_user;
        if (single_u1 > single_u2) std::swap(single_u1, single_u2);
        session_id = "s_" + std::to_string(single_u1) + "_" + std::to_string(single_u2);
        hermes_target = agent_bot_users_.count(to_user) != 0;
        if (hermes_target) {
            if (!hermes_enabled_ || !hermes_request_producer_) {
                result.status = ServiceStatus{503, "Hermes Bot is disabled"};
                return result;
            }
            User bot;
            std::string bot_error;
            if (!GetActiveUser(to_user, &bot, &bot_error)) {
                result.status =
                    ServiceStatus{503, "Hermes Bot is disabled or unavailable"};
                return result;
            }
        } else {
            std::vector<std::string> comets;
            if (redis_store_ && !redis_store_->GetUserRoutes(to_user, &comets)) {
                LOG_ERROR << "GetUserRoutes failed for user "
                          << std::to_string(to_user);
            } else {
                for (const auto& cid : comets) {
                    comet_to_users[cid].push_back(to_user);
                }
            }
        }
    } else if (scene == "chatroom") {
        if (!redis_store_) {
            result.status = {503, "room routing unavailable"};
            return result;
        }
        room_id = request.group_id;
        if (room_id <= 0) {
            result.status = ServiceStatus{400, "group_id(room_id) must be positive"};
            return result;
        }
        session_id = "r_" + std::to_string(room_id);
        bool is_member=false;
        if (!group_member_dao_ || !group_member_dao_->IsMember(room_id,from_user,&is_member,&err)) {
            result.status = {500,"room membership lookup failed"}; return result;
        }
        if (!is_member) { result.status={403,"room membership required"}; return result; }
        if (room_agent_service_) {
            const auto access = room_agent_service_->Trigger(from_user, room_id, request.content_json, &hermes_target);
            if (!access.ok()) { result.status = access; return result; }
        }
        std::vector<std::string> room_comets;
        bool use_room_comets = false;
        if (redis_store_->GetRoomComets(room_id, &room_comets) &&
            !room_comets.empty()) {
            use_room_comets = true;
        }
        // 房间广播的两种扇出策略，二选一：
        //   策略 A（快）用 room:comets:{room_id} 路由集。里面存的是
        //     “这个房间当前有人在连的 comet 节点”，直接给每个节点发一份
        //     （注意上面 `comet_to_users[cid];` 只建空 vector：不指定具体
        //     用户，由 Comet 自己按本地 room_users_ 快照扇出）。
        //     一次 Kafka 发送服务一整个节点，成本与房间人数无关。
        //   策略 B（慢）用持久成员表精确扇出。先 ListRoomMembers 拿到
        //     全部成员，再逐个 GetUserRoutes，拼出 comet -> [uid] 映射。
        //     成本是 O(成员数) 次 Redis 查询。
        //
        // Explicit durable members are authoritative when Agent work is enabled;
        // WS presence alone is not authorization to receive workspace messages.
        //
        // 译：开启了 Agent 协作时，**显式的持久成员表**才是权威；
        // 仅有 WS 在线并不构成接收工作区消息的授权。
        //
        // 这句话背后的安全考量：房间共享 Agent 的对话里包含任务规格、
        // 交接摘要与 can_develop 标志，这些都是“工作区内容”。
        // 而 room:comets 只是“谁在线”的快照，一个非成员只要能连上
        // 那个房间所在的 comet（例如发一条 room_join 控制消息）就可能
        // 被算进去。因此一旦 room_agent_service_ 存在，就宁可走慢路径
        // 也要用 group_member 表做权威判定。
        // 代价：大房间的广播会从 O(comet 数) 退化成 O(成员数)。
        if (use_room_comets && !room_agent_service_) {
            for (const auto& cid : room_comets) {
                comet_to_users[cid];
            }
        } else {
            std::vector<int64_t> members;
            if (group_member_dao_) {
                if (!group_member_dao_->ListRoomMembers(room_id, &members, &err)) {
                    LOG_ERROR << "ListRoomMembers failed: " << err;
                }
            }
            for (int64_t uid : members) {
                std::vector<std::string> comets;
                if (!redis_store_->GetUserRoutes(uid, &comets)) {
                    continue;
                }
                for (const auto& cid : comets) {
                    comet_to_users[cid].push_back(uid);
                }
            }
        }
    } else {
        result.status = ServiceStatus{400, "unsupported scene"};
        return result;
    }

    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();

    // 阶段 6：附件授权 + 序号分配。
    //
    // AuthorizeMessageImages 校验 content.attachments 里的每个 att_* id：
    // 必须属于发送者、必须绑定到当前会话。不校验就能引用别人上传的
    // 图片（水平越权）。图片二进制从始至终不进 Kafka，只传 JSON 引用。
    // 有附件时 msg_type 标为 image，供客户端选渲染分支。
    if (!AuthorizeMessageImages(from_user, session_id, request.content_json, &err)) {
        result.status = ServiceStatus{400, err};
        return result;
    }
    const auto image_refs = ExtractImageRefsFromContentJson(request.content_json);
    const char* msg_type = image_refs.empty() ? "text" : "image";

    // AppendMessageHotPath 是热路径的核心，内部做三件事：
    //   a) 写 message_reservation（durable outbox）：先把 canonical 原文和
    //      client_msg_id 落到 MySQL，保证 Redis 丢缓存也能恢复；
    //   b) 调 Redis Lua 原子完成「校准 floor + 去重 + INCR msg_seq」；
    //   c) is_new=false 表示命中幂等键，返回已分配的旧 seq 和旧原文。
    //
    // is_new 的用途很关键：重复提交时仍要给客户端回 accepted_ack（否则
    // 客户端会一直重试），但**不能**重复触发 Agent、也不计 accepted 指标。
    Message msg;
    bool is_new = false;
    if (!store_->AppendMessageHotPath(session_id, from_user, msg_type,
                                      request.content_json, now_ms,
                                      request.client_msg_id, &msg, &is_new, &err)) {
        result.status = ServiceStatus{500, "hot path append failed: " + err};
        return result;
    }

    // 回填服务端权威字段到 content_json。
    //
    // 客户端发来的只有内容，msg_id/session_id/msg_seq/create_time/from_user_id
    // 全部由服务端覆盖。这是防篡改的最后一道：即使客户端在 JSON 里
    // 伪造了 from_user_id，这里也会被真实值覆盖。
    // catch(...) 吃掉解析异常：原文已经落库，不能因为回显字段拼接失败
    // 就把一条已 accepted 的消息报错。
    std::string push_json = msg.content_json;
    try {
        auto j = nlohmann::json::parse(push_json);
        j["type"] = scene == "chatroom" ? "chatroom" : "single_chat";
        if (scene == "chatroom") j["group_id"] = room_id;
        j["msg_id"] = msg.msg_id;
        j["session_id"] = msg.session_id;
        j["msg_seq"] = msg.msg_seq;
        j["create_time"] = msg.timestamp_ms;
        j["from_user_id"] = from_user;
        j["sender_id"] = from_user;
        if (!j.contains("client_msg_id")) {
            j["client_msg_id"] = request.client_msg_id;
        }
        j["sender"] = {{"uid", from_user}};
        push_json = j.dump();
        msg.content_json = push_json;
    } catch (...) {
    }
    if (hermes_target || scene == "chatroom") RememberHermesMessage(msg);

    result.message = msg;
    result.has_message = true;
    ChatMessage chat_message;
    ChatMessage* cm = &chat_message;
    FillChatMessage(msg, cm);
    cm->set_content_json(push_json);

    // 阶段 7：持久化——accepted_ack 的唯一边界。
    //
    // accepted_ack 的边界是持久化事件已经收到 Kafka delivery report；
    // 这一步替代 Logic 本地线程池，Logic 崩溃后仍可由 Job 从 topic 重放。
    //
    // PersistToTopic 内部是 outbox 模式：
    //   PrepareOutbox -> SendAndWait -> Published
    // 三步任一失败都返回 false，这里就回 503，**绝不**给假 ACK。
    // 残留的 prepared 记录由 main.cpp 里的 outbox_recovery 定时器重发。
    //
    // 注意这里是同步等待（SendAndWait），因此整个 Submit 的延迟下限
    // 就是一次 Kafka round-trip。这是用延迟换可靠性的明确取舍。
    // Materialize ALL generic intents before preparing the source message outbox.
    // A crash after source confirmation can then be recovered without client retry.
    if(scene=="chatroom" && hermes_target && input.contains("agent_mentions") && !input["agent_mentions"].empty()) {
        nlohmann::json intents;
        if(!BuildHermesRequest(msg,&intents,&err)) { result.status={503,err};return result; }
    }
    if (!PersistToTopic(msg, scene, single_u1, single_u2, room_id, &err)) {
        MetricsRegistry::Instance().Increment("spark_push_persist_events_failed_total");
        result.status = ServiceStatus{503, err};
        return result;
    }
    result.accepted = true;
    result.accepted_at_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count();
    MetricsRegistry::Instance().Increment("spark_push_messages_accepted_total_" +
                                          scene);

    // 相同 sender/session/client_msg_id 复用原文和序号；投递允许同 ID 重发，由下游去重。
    //
    // 注意这里对 is_new=false（幂等命中）**仍然继续往下走投递**，
    // 而不是提前 return。理由见下面那句英文注释。
    if (!is_new) MetricsRegistry::Instance().Increment("spark_push_duplicate_total_" + scene);
    // Retry the same immutable fanout ID, including after a prior publish failure.
    //
    // 译：重试时使用同一个不可变的扇出 ID，即使上一次发布已经失败过。
    //
    // 为何幂等命中还要再投一次：上一次可能在阶段 7 成功了但阶段 8
    // 失败了（Kafka push topic 瞬时不可用），此时消息已落库但对方没收到。
    // 客户端重试时如果因为“已去重”就短路返回，那条消息就永远不会被投递。
    // 重投是安全的：request_id = msg_id + "@" + comet_id 不变，
    // Comet 侧的 recent_push_ids_ 会把真正的重复丢掉。

    // 阶段 8：实时投递（best-effort）。
    //
    // 从这里开始消息已经 accepted，后续任何失败都不会撤销 ACK，只记指标。
    //
    // Agent 分支：目标联系人是 Bot 时，用户输入另走 ai_request topic。
    // 单聊场景下直接 return——Bot 会话不需要向「对方」投递，
    // 回答会由 HandleHermesReply 当成一条普通单聊消息重新走完整链路。
    // 房间场景不 return，因为还要投递给房间里的其他人类成员。
    if (hermes_target) {
        // 输入消息已经越过 accepted_ack 的持久化边界；AI 请求单独进入
        // ai_request，Bridge 崩溃或 Hermes 暂时不可用时由 Kafka 重试。
        if (!EnqueueHermesRequest(msg, &err)) {
            MetricsRegistry::Instance().Increment("spark_push_hermes_errors_total");
            result.status = ServiceStatus{503, err};
            return result;
        }
        result.status = ServiceStatus{0, "Hermes request accepted"};
        if (scene == "single") return result;
    }

    // 对方全离线：不算错误。消息已 durable，上线后走 SyncOffline。
    if (comet_to_users.empty()) {
        result.status = ServiceStatus{0, "accepted for offline storage"};
        return result;
    }
    // 单聊与群聊走不同 topic / 不同 producer，与 Job 侧的独立 consumer group
    // 一一对应，做到三套令牌桶互相隔离。
    KafkaProducer* realtime_producer = scene == "single" ? producer_ : group_producer_;
    if (!realtime_producer) {
        result.status = ServiceStatus{503, "scene Kafka producer not ready"};
        return result;
    }

    // request_id = msg_id + "@" + comet_id，是一个**不变**的幂等键。
    //
    // 为什么不用随机数：同一条消息向同一个 comet 重发时必须用相同的
    // request_id，Comet 侧的 AcceptPushRequest 才能识别出重复并丢弃。
    // 用随机数会让每次重试都被当成新消息，客户端看到重复气泡。
    //
    // Kafka key 用 comet_id：保证发往同一节点的消息进同一分区，
    // 从而在分区内保持 msg_seq 顺序。
    size_t accepted_routes = 0;
    for (const auto& kv : comet_to_users) {
        const std::string& comet_id = kv.first;
        const auto& users = kv.second;
        PushToCometRequest req;
        req.set_comet_id(comet_id);
        req.set_request_id(msg.msg_id + "@" + comet_id);
        req.set_ack_comet_id(request.source_comet_id);
        req.set_ack_user_id(from_user);
        req.set_scene(scene == "single" ? "single" : "group");
        *req.mutable_message() = *cm;
        for (int64_t uid : users) {
            auto* target = req.add_targets();
            target->set_user_id(uid);
        }
        std::string payload;
        if (!req.SerializeToString(&payload)) {
            LOG_ERROR << "Serialize PushToCometRequest failed";
            continue;
        }
        if (!realtime_producer->Send(comet_id, payload)) {
            LOG_ERROR << "Kafka send failed for comet " << comet_id;
        } else {
            ++accepted_routes;
        }
    }
    // 实时 topic 用的是异步 Send（不是 SendAndWait）：投递失败不能阻塞
    // 已经成立的 accepted。
    //
    // accepted_routes == 0 时仍返回 code=0（成功），但文案改成
    // 「accepted; realtime delivery pending」——这是个很关键的语义区分：
    // 消息安全（已 durable），只是实时通道暂时不可用。如果这里返回
    // 错误，客户端会重试，反而可能产生用户可见的重复发送体验。
    if (accepted_routes == 0) {
        // 消息已经在 persist topic 中 durable；实时 topic 暂时不可用时，
        // 客户端仍拿到 accepted_ack，重连后由 cursor/offline sync 补齐。
        MetricsRegistry::Instance().Increment(
            "spark_push_realtime_enqueue_failed_total_" + scene);
        result.status = ServiceStatus{0, "accepted; realtime delivery pending"};
        return result;
    }
    if (accepted_routes != comet_to_users.size()) {
        LOG_ERROR << "Kafka accepted only " << accepted_routes << "/"
                  << comet_to_users.size() << " routes for msg_id=" << msg.msg_id;
    }
    MetricsRegistry::Instance().Increment("spark_push_realtime_enqueued_total_" + scene,
                                          static_cast<int64_t>(accepted_routes));
    result.status = ServiceStatus{0, "ok"};
    return result;
}

// 消费 ai_reply topic：把一条 Agent 回复变成一条普通的单聊/房间消息。
//
// 【返回值就是 Kafka 的提交契约】（与 main.cpp 里的 consumer Options 配合）：
//   return true   处理成功 -> 提交偏移量，这条记录不会再被看到
//   return false  处理失败 -> 不提交，重试；超过 max_processing_attempts(=3)
//                 后进 ai_reply.dlq（这个 topic 配了 DLQ，而 ai_delta 没配）
// 因此本函数里每一个 `return false` 都意味着“稍后会重来一次”，
// 必须确保重来是安全的。
//
// 【重来为何安全——本函数最关键的一个不变量】
// 注意后半段有几处 `return false` 发生在**消息已经持久化之后**
//（例如 AgentRoutes 失败、room Complete 失败）。重来时会重新跑一遍
// PersistHermesReply，而它能幂等命中的前提是：两次生成的 Message
// 字节**完全相同**。这就是 hermes_reply.h 里那条“不读墙上时钟”
// 约束存在的全部理由：timestamp_ms 取自载荷里的 completed_at_ms、
// client_msg_id = "hermes:" + request_id。若其中任何一个改成
// time(nullptr) 或随机值，重来时 InsertMessage 的内容比对就会判定
// “序号冲突”而把一条完全正常的回复丢进 DLQ。
//
// 【校验闸门顺序】（全部在持久化之前，任一不过就 return false）：
//   1. 载荷必须是 JSON object 且带整型 bot_user_id
//   2. bot_user_id 必须在 agent_bot_users_ 里（不认识的 bot 直接丢）
//   3. 房间回复额外过 RoomAgentService::ValidateReply（白名单式归属比对）
//   4. PrepareHermesReply（字段校验 + 引用校验）
// 第 2 步尤其重要：hermes_bridge 是一个外部进程，它出错或被攻破时
// 可能把一个回复归到任意 bot_user_id 上；而 sender_id 直接决定了
// 客户端把它渲染成“谁说的话”。
bool MessageService::HandleHermesReply(const std::string& payload,
                                       const std::string& kafka_key) {
    // kafka_key 故意不用：回复的归属信息全在载荷里（session_id /
    // user_id / request_id），而 key 只用于分区选择。
    // 若改成靠 key 判定归属，就等于信任了一个没被 ValidateReply 覆盖的字段。
    (void)kafka_key;
    PreparedHermesReply reply;
    std::string err;
    const auto envelope = nlohmann::json::parse(payload, nullptr, false);
    if (!envelope.is_object() || !envelope.contains("bot_user_id") ||
        !envelope["bot_user_id"].is_number_integer())
        return false;
    const auto bot_user_id = envelope["bot_user_id"].get<int64_t>();
    const auto bot = agent_bot_users_.find(bot_user_id);
    if (bot == agent_bot_users_.end()) return false;
    const bool room_reply = envelope.value("session_id", "").rfind("r_", 0) == 0;
    nlohmann::json room_request;
    // 房间回复必须先过 ValidateReply：它会从库里取出当初存的请求，
    // 逐字段比对 request_id / session_id / user_id / bot_user_id / collaboration，
    // 任一不匹配就整个拒收（白名单式比对，不是过滤）。
    // 输出的 room_request 随后会被传给 PrepareHermesReply 做第二层校验，
    // 并在下面提供权威的 room_id（**不从载荷里取**）。
    if (room_reply && (!room_agent_service_ ||
        !room_agent_service_->ValidateReply(envelope, &room_request, &err))) return false;
    if (!PrepareHermesReply(payload, bot_user_id, bot->second, &reply, &err,
                           room_reply ? &room_request : nullptr)) {
        LOG_ERROR << "Reject Hermes reply: " << err;
        MetricsRegistry::Instance().Increment("spark_push_hermes_errors_total");
        // durable ai_reply DLQ after bounded retries
        // 译：持久化的 ai_reply 在有限次重试后进 DLQ。
        // 与 ai_delta（直接丢）形成对比：一条最终回复丢了，用户就会
        // 永久等一个不会来的答案，因此必须留一份可人工回放的记录。
        return false;
    }
    const auto& request_id = reply.request_id;
    const auto user_id = reply.user_id;
    const bool ok = reply.ok;
    if (!reply.citation_warning.empty()) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_citations_rejected_total");
    }
    Message message = reply.message;
    const auto stored_content_metrics = MeasureText(message.content_json);
    const auto stored_text_metrics =
        MeasureText(ExtractTextFromContentJson(message.content_json));
    if (LengthAuditEnabled()) {
        LOG_INFO
            << "length_audit stage=storage_before_enqueue request_id=" << request_id
            << " provider=" << envelope.value("effective_provider", "adapter-state")
            << " model=" << envelope.value("effective_model", "unknown")
            << " stream=" << (envelope.value("streamed", false) ? "true" : "false")
            << " max_tokens=unset max_output_tokens=unset"
            << " provider_bytes=" << envelope.value("audit_provider_output_bytes", 0)
            << " provider_chars=" << envelope.value("audit_provider_output_chars", 0)
            << " content_json_bytes=" << stored_content_metrics.utf8_bytes
            << " content_json_chars=" << stored_content_metrics.unicode_chars
            << " response_bytes="
            << envelope.value("response_bytes", stored_text_metrics.utf8_bytes)
            << " response_chars="
            << envelope.value("response_unicode_chars",
                              stored_text_metrics.unicode_chars)
            << " finish_reason=" << envelope.value("finish_reason", "");
    }
    bool is_new = false;
    // user1/user2 按 (min,max) 归一，与 Submit 里构造单聊 session_id 的
    // 规则完全一致。Job 落库时靠它们建会话行，弄反了就会建出两个
    // 方向相反的重号会话。
    const int64_t user1 = std::min(user_id, bot_user_id);
    const int64_t user2 = std::max(user_id, bot_user_id);
    // room_id 取自 **ValidateReply 返回的库里请求**，而不是载荷。
    // 载荷里的 session_id 只被用来判定“是不是房间回复”这一个布尔，
    // 真正的 room_id 走已鉴权的存储值，避开被伪造的载荷把回复
    // 归到另一个房间。
    const int64_t reply_room = room_reply ? room_request.at("collaboration").at("room_id").get<int64_t>() : 0;
    // PersistHermesReply 把“分配序号”与“写 outbox + Kafka”两步封成一对
    // lambda（见 hermes_reply.h），顺序契约写在签名里：必须先 allocate 再 persist。
    // 两个 lambda 都捕获了 &err，因此失败原因能一路传到下面的日志。
    if (!PersistHermesReply(
            &message, &is_new,
            [this, &err](Message* msg, bool* fresh) {
                return store_ && store_->AppendMessageHotPath(
                                     msg->session_id, msg->sender_id, msg->msg_type,
                                     msg->content_json, msg->timestamp_ms,
                                     msg->client_msg_id, msg, fresh, &err);
            },
            [this, user1, user2, reply_room, &err](const Message& msg) {
                return PersistToTopic(msg, reply_room ? "chatroom" : "single",
                                      reply_room ? 0 : user1, reply_room ? 0 : user2, reply_room, &err);
            })) {
        LOG_ERROR << "Persist Hermes reply failed request_id=" << request_id << ": "
                  << err;
        MetricsRegistry::Instance().Increment("spark_push_hermes_errors_total");
        return false;
    }
    const auto persisted_content_metrics = MeasureText(message.content_json);
    if (LengthAuditEnabled()) {
        LOG_INFO << "length_audit stage=storage_after_enqueue request_id=" << request_id
                 << " content_json_bytes=" << persisted_content_metrics.utf8_bytes
                 << " content_json_chars=" << persisted_content_metrics.unicode_chars
                 << " is_new=" << (is_new ? "true" : "false");
    }
    if (!is_new) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_duplicate_replies_total");
    }
    // Only Kafka persistence confirmation closes the ephemeral stream and
    // exposes this answer to future Agent context. Job persists asynchronously.
    //
    // 译：只有 Kafka 持久化得到确认，才能关闭那个临时流，并把这条回答
    // 暴露给以后的 Agent 上下文。Job 的落库是异步的。
    //
    // 两句各对应一个顺序约束：
    //   Complete 必须在 PersistHermesReply 之后：它把 room_agent_request 的
    //     result 列写上，而那是“这次调用已完成”的权威标记。先写它就等于
    //     在消息还没安全落地时宣布成功，一旦后面失败，重试会因为
    //     Finish 的 `result IS NULL` 条件而被当成重复。
    //   RememberHermesMessage 也必须在之后：它把这条回复放进进程内
    //     历史缓存，下一轮拼 prompt 时会读到。若持久化失败就提前放进缓存，
    //     用户会看到一条“Agent 说过了但库里没有”的幽灵上下文。
    //
    // 注意这两处的 return false 都发生在持久化**之后**，因此重试完全
    // 依赖上面说的那个字节级幂等不变量。
    if (room_reply && !room_agent_service_->Complete(envelope, &err)) return false;
    MarkHermesStreamCompleted(request_id);
    RememberHermesMessage(message);

    std::map<std::string, std::vector<int64_t>> comets;
    // AgentRoutes 失败时 return false（而不是当成“对方离线”忽略）：
    // 这与 Submit 里“comet_to_users 为空是正常情况”的处理不同。
    // 理由是两者的语义不一样：Submit 里空集合意味着“查到了，但没人在线”，
    // 而这里失败意味着“查不出来”（Redis 故障）。后者应该重试，
    // 因为用户很可能就在线上等着；当成离线忽略就会让回复静静消失。
    if (!AgentRoutes(reply_room, user_id, &comets)) {
        LOG_ERROR << "Get Hermes recipient routes failed user_id="
                  << std::to_string(user_id);
        return false;
    }
    ChatMessage chat_message;
    FillChatMessage(message, &chat_message);
    // 下面整个循环是 best-effort：单个 comet 发失败只 continue 并记日志，
    // 不影响其他节点，也不影响最终的 return true。
    // 理由：消息已经 durable，没推到的那个节点上的用户重连后会由
    // SyncOffline 按 delivered_seq 水位补齐。为了一个推送失败而重试
    // 整个 handler，反而会把已经完成的 Complete/Remember 再跑一遍。
    for (const auto& route : comets) {
        const auto& comet_id = route.first;
        PushToCometRequest request;
        request.set_comet_id(comet_id);
        // request_id = msg_id + "@" + comet_id，与 Submit 里的构造规则完全一致：
        // Comet 侧靠它在 recent_push_ids_ 里做幂等去重。改成随机数就会
        // 让重试被当成新消息，用户看到重复的 Agent 回复气泡。
        request.set_request_id(message.msg_id + "@" + comet_id);
        request.set_scene(room_reply ? "group" : "single");
        *request.mutable_message() = chat_message;
        for (auto target : route.second) request.add_targets()->set_user_id(target);

        std::string push_payload;
        // 房间回复走 group_producer_（group push topic），单聊走 producer_。
        // 两个 topic 在 Job 侧对应独立的 consumer group，因此一个卡顶
        // 不会拖死另一个。
        auto* producer = room_reply ? group_producer_ : producer_;
        if (!request.SerializeToString(&push_payload) || !producer ||
            !producer->Send(comet_id, push_payload)) {
            LOG_ERROR << "Enqueue Hermes reply to Comet failed comet_id=" << comet_id
                      << ", request_id=" << request_id;
            continue;
        }
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_realtime_enqueued_total");
    }
    MetricsRegistry::Instance().Increment(ok ? "spark_push_hermes_replies_total"
                                             : "spark_push_hermes_errors_total");
    return true;
}

// 消费 ai_delta topic：把一段流式增量（打字机效果）实时推给客户端。
//
// 【与 HandleHermesReply 最大的不同：本函数几乎全部 return true】
// 数一下就会发现：载荷非法、字段缺失、bot 不认识、session 不匹配、
// 事件不规范、流已完成、增量重复、对方离线——**全部 return true**。
// 只有一种情况 return false：AgentRoutes 失败（Redis 故障）。
//
// 为何完全相反：return false 会不提交偏移量并重试，而增量是
// **临时数据**：
//   一条格式非法的增量，重试一百次仍然是非法的，只会堵住整个分区
//     （后面所有用户的打字机效果全部卡住）；
//   一条重复或迟到的增量，重试没有任何意义；
//   而 Redis 瞬时故障是真的可能下一次就好了，因此只有它值得重试。
// 这也正是 main.cpp 里给 ai_delta consumer 把 dead_letter_topic **清空**的原因：
// 既然几乎从不 return false，DLQ 就只会堆积垃圾。
//
// 【不持久化，因此也不需幂等字节】
// 本函数会读墙上时钟（now_ms）并把 msg_seq 写死为 0，而
// HandleHermesReply 那条路径上这两件事都是禁止的。差别在于：
// 增量**不进 message 表、不占 msg_seq、不走 outbox**（msg_seq=0 就是
// “不进入历史游标”的意思），因此没有“两次生成必须字节相同”的约束。
// 丢一段增量只是少看到一段中间过程，最终的 ai_reply 仍会完整到达。
bool MessageService::HandleHermesDelta(const std::string& payload,
                                       const std::string& kafka_key) {
    (void)kafka_key;
    nlohmann::json delta;
    try {
        // 这里用抛异常的 parse（而不是像 HandleHermesReply 那样用
        // parse(..., false) + is_discarded）：因为需要 e.what() 进日志。
        delta = nlohmann::json::parse(payload);
    } catch (const nlohmann::json::exception& e) {
        LOG_ERROR << "Discard malformed Hermes delta: " << e.what();
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_errors_total");
        // 注意：这里是 return **true**（丢弃并提交），不是 false。
        return true;
    }

    const std::string request_id = delta.value("request_id", "");
    const int64_t user_id = delta.value("user_id", 0LL);
    const int64_t bot_user_id = delta.value("bot_user_id", 0LL);
    const std::string text = delta.value("delta", "");
    const int delta_index = delta.value("delta_index", -1);
    if (request_id.empty() || user_id <= 0 || !agent_bot_users_.count(bot_user_id) ||
        text.empty() || delta_index < 0) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_errors_total");
        return true;
    }
    std::string session_id = delta.value("session_id", "");
    nlohmann::json room_request;
    const bool room_delta = session_id.rfind("r_", 0) == 0;
    std::string room_error;
    // 房间增量同样要过 ValidateReply：增量会带着 task_id 推给房间里
    // 所有人，不验归属就能把一个房间的中间过程泄露给另一个房间。
    if (room_delta && (!room_agent_service_ ||
        !room_agent_service_->ValidateReply(delta, &room_request, &room_error))) return true;
    const int64_t delta_room = room_delta ? room_request.at("collaboration").at("room_id").get<int64_t>() : 0;
    // 单聊场景下重建期望的 session_id 并强制比对（与 BuildHermesRequest
    // 里找 bot 的做法是同一套规则）。session_id 为空时允许用重建值补上
    //（旧版 bridge 不传这个字段），但传了就必须对得上。
    const auto expected_session = "s_" +
                                  std::to_string(std::min(user_id, bot_user_id)) + "_" +
                                  std::to_string(std::max(user_id, bot_user_id));
    if (session_id.empty()) session_id = expected_session;
    if (!room_delta && session_id != expected_session) return true;

    const bool progress = delta.value("progress", nlohmann::json()) == true;
    nlohmann::json normalized_event = {
        {"schema", "sparkpush.agent_event.v1"},
        {"type", progress ? "assistant_progress" : "assistant_delta"},
        {"data", {{"text", text}}}};
    // 如果载荷里已经带了 agent_event，就把它规范化后与上面重建的版本
    // **逐字段比对**，不一致就丢。这又是一个白名单式校验（与
    // ValidateReply 同一思路）：不信任上游给的结构，只拿它验证自己的重建。
    // 尤其注意 `envelope.value("terminal", true)` 的默认值是 **true**：
    // 缺少 terminal 字段时直接判为非法。因为一个“终态”的增量不应该
    // 走 delta 通道（终态就是 ai_reply），默认值给 false 就会让一个
    // 缺字段的载荷冒充成普通增量。
    if (delta.contains("agent_event")) {
        if (!NormalizeAgentEvent(delta["agent_event"], &normalized_event) ||
            normalized_event["type"] !=
                (progress ? "assistant_progress" : "assistant_delta") ||
            normalized_event["data"]["text"] != text) {
            MetricsRegistry::Instance().Increment(
                "spark_push_agent_event_invalid_total");
            return true;
        }
        if (normalized_event.contains("envelope")) {
            const auto& envelope = normalized_event["envelope"];
            if (envelope.value("request_id", "") != request_id ||
                envelope.value("conversation_id", "") != session_id ||
                envelope.value("sequence", -1) != delta_index ||
                envelope.value("terminal", true)) {
                MetricsRegistry::Instance().Increment(
                    "spark_push_agent_event_invalid_total");
                return true;
            }
        }
    }
    // 两个去重闸门，顺序不能颠倒：
    //   IsHermesStreamCompleted  最终回复已经处理完了 -> 后续增量全部丢。
    //                            避开用户在已经看到完整回答后，
    //                            又被一串迟到的打字机片段干扰。
    //   MarkHermesDeltaSeen      同一个 (request_id, delta_index) 只推一次。
    //                            Kafka 至少一次语义下的重放全靠它。
    // 两者都是进程内状态（重启就没了），而这是可接受的：
    // 重启后重放一段增量的后果只是客户端看到一小段重复文本，
    // 而最终的 ai_reply 仍然会被 HandleHermesReply 幂等处理。
    if (IsHermesStreamCompleted(request_id)) return true;

    const std::string delta_id =
        "hermes_delta:" + request_id + ":" + std::to_string(delta_index);
    if (MarkHermesDeltaSeen(delta_id)) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_duplicate_total");
        return true;
    }

    std::map<std::string, std::vector<int64_t>> comets;
    if (!AgentRoutes(delta_room, user_id, &comets)) {
        LOG_ERROR << "Get Hermes delta recipient routes failed user_id="
                  << std::to_string(user_id);
        return false;
    }
    if (comets.empty()) {
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_offline_total");
        return true;
    }

    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    nlohmann::json content = {
        {"type", "hermes_delta"},
        {"request_id", request_id},
        {"session_id", session_id},
        {"from_user_id", bot_user_id},
        {"to_user_id", user_id},
        {"delta_index", delta_index},
        {"delta", text},
        {"progress", progress},
        {"source", "hermes"},
        {"agent_event", normalized_event},
    };

    if (room_delta) {
        content["group_id"] = delta_room;
        content["actor_user_id"] = user_id;
        if(room_request.at("collaboration").contains("task_id")) content["task_id"] = room_request.at("collaboration").at("task_id");
        else {
            content["agent_id"]=room_request.at("agent_id");
            content["agent_name"]=room_request.at("agent_name");
            content["source_msg_id"]=room_request.at("source_msg_id");
        }
        content.erase("to_user_id");
    }

    ChatMessage message;
    // msg_id 与 client_msg_id 都用 delta_id（"hermes_delta:<request_id>:<index>"）：
    // 它们是 Comet 侧幂等去重的依据。同一个增量重发时 id 不变，
    // recent_push_ids_ 会把重复丢掉，因此客户端不会看到重影。
    message.set_msg_id(delta_id);
    message.set_session_id(session_id);
    message.set_msg_seq(0);  // 增量不进入历史游标。
    message.set_sender_id(bot_user_id);
    message.set_timestamp_ms(now_ms);
    message.set_msg_type("hermes_delta");
    message.set_content_json(content.dump());
    message.set_client_msg_id(delta_id);

    for (const auto& route : comets) {
        const auto& comet_id = route.first;
        PushToCometRequest request;
        request.set_comet_id(comet_id);
        request.set_request_id(delta_id + "@" + comet_id);
        request.set_scene(room_delta ? "group" : "single");
        *request.mutable_message() = message;
        for (auto target : route.second) request.add_targets()->set_user_id(target);
        std::string push_payload;
        auto* producer = room_delta ? group_producer_ : producer_;
        if (!request.SerializeToString(&push_payload) || !producer ||
            !producer->Send(comet_id, push_payload)) {
            LOG_ERROR << "Enqueue Hermes delta to Comet failed comet_id=" << comet_id
                      << ", request_id=" << request_id;
            MetricsRegistry::Instance().Increment(
                "spark_push_hermes_stream_delta_errors_total");
            continue;
        }
        MetricsRegistry::Instance().Increment(
            "spark_push_hermes_stream_delta_enqueued_total");
    }
    return true;
}

// 算出一条 Agent 回复/增量应该推给哪些 comet。
//
// 房间场景下推给**全部成员**（而不只是提问者）：共享 Agent 的回答
// 属于整个房间，其他开发者需要看到进展。因此这里会走一次
// ListRoomMembers（MySQL），成本比单聊高得多。
//
// 失败语义很严格：任何一个环节查不出来就整体 return false。
// 这与 Submit 里“部分路由查失败就 continue”不同：那里只是少推一个
// 节点，而这里如果静默忽略，一个本该看到回复的成员就会永远看不到。
// 调用方拿到 false 后会 return false 让 Kafka 重试。
bool MessageService::AgentRoutes(int64_t room, int64_t user,
                                std::map<std::string, std::vector<int64_t>>* routes) {
    if (!redis_store_) return false;
    // 单聊：只有提问者一个人。注意 users 是先构造再可能被整个覆盖，
    // 因此房间分支不需要先把 user 放进去。
    std::vector<int64_t> users{user};
    std::string error;
    if (room && (!group_member_dao_ || !group_member_dao_->ListRoomMembers(room, &users, &error))) return false;
    // 用 std::map 而不是 unordered_map：遍历顺序确定，因此日志与指标
    // 在多次运行之间可重现，排查时能对比。
    for (auto id : users) {
        std::vector<std::string> comets;
        if (!redis_store_->GetUserRoutes(id, &comets)) return false;
        for (const auto& comet : comets) (*routes)[comet].push_back(id);
    }
    return true;
}

// 广播的共用实现：拼一个 BroadcastTaskRequest 发到 broadcast_task topic，
// 由 Job 消费后逐个 comet 扇出。
//
// 广播**不走 outbox**（与单聊/房间的 PersistToTopic 不同）：它没有
// msg_seq、不进会话历史，因此没有“客户端重连后补推”这个语义，
// 也就没有必须可恢复的理由。代价是：Kafka 发送失败时这条广播
// 就真没了（只能看日志里的 task_id）。
//
// rate_limit 参数区分两个调用方：Broadcast（gRPC，限流）与
// BroadcastAdmin（HTTP 管理面，不限流）。管理面不限流是因为它已经
// 过了 requireAdmin 门禁，而且人手点击不会构成风暴。
BroadcastResult MessageService::PublishBroadcast(const std::string& scope,
                                                 int64_t group_id,
                                                 const std::string& content_json,
                                                 bool rate_limit) {
    BroadcastResult result;
    result.scope = scope;
    result.group_id = group_id;
    if (rate_limit &&
        !rate_limiter_.Allow("broadcast", scope.empty() ? "all" : scope)) {
        MetricsRegistry::Instance().Increment(
            "spark_push_rate_limited_total_broadcast");
        result.status =
            ServiceStatus{429, "broadcast rate limit exceeded; retry later"};
        return result;
    }
    if (!broadcast_producer_) {
        result.status = ServiceStatus{500, "broadcast producer not initialized"};
        result.task_id.clear();
        return result;
    }

    // 简单生成一个 task_id：当前毫秒时间戳 + 随机数
    //
    // 已知弱点（诚实标注）：全仓库**没有任何地方调 srand**，因此 rand()
    // 每次进程启动都从同一个种子开始，产生完全相同的序列。
    // 意味着“随机数”那一段实际只等价于一个进程内计数器：
    // 两次广播在同一毫秒内且 rand() 位置相同时就会擞出同一个 task_id
    //（概率极低，但 rand() 在这里并没有提供它看起来提供的那种保护）。
    // task_id 只用于日志追查（不当幂等键也不当主键），因此后果有界；
    // 但工程里已经有 common/security.h 的 GenerateSecureToken，
    // 真要修应该换成它而不是给 rand() 加 srand。
    int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    std::string task_id =
        "bcast-" + std::to_string(now_ms) + "-" + std::to_string(rand());

    BroadcastTaskRequest task;
    task.set_task_id(task_id);
    task.set_scope(scope);
    task.set_group_id(group_id);
    task.set_content_json(content_json);
    // scope 白名单只有 "all" 与 "group"（注意：这是**内部**词汇，
    // HTTP 层用的 "chatroom" 已经在 BroadcastAdmin 里转换过了）。
    if (scope != "all" && scope != "group") {
        result.status = {400, "unsupported broadcast scope"};
        return result;
    }
    if (scope == "group") {
        // 房间广播在 **Logic 侧就把成员列表展开**，而不是让 Job 去查：
        // Job 可能有多实例并发消费，在那里查会让每个实例都重复一次
        // 成员查询；而且展开时刻固定在发布时，避开了“Job 延迟消费时
        // 成员已经变了”这种不可重现的行为。
        // 代价：成员很多时这个 protobuf 会变大（每个成员一个 int64）。
        std::vector<int64_t> members;
        std::string error;
        if (group_id <= 0 || !group_member_dao_ ||
            !group_member_dao_->ListRoomMembers(group_id, &members, &error)) {
            result.status = {400, "cannot resolve broadcast room membership"};
            return result;
        }
        for (auto user : members) task.add_user_ids(user);
    }

    std::string payload;
    if (!task.SerializeToString(&payload)) {
        result.status = ServiceStatus{500, "serialize BroadcastTaskRequest failed"};
        result.task_id.clear();
        return result;
    }
    LOG_INFO << "Sending broadcast task " << task_id
             << ", payload_bytes=" << payload.size();
    if (!broadcast_producer_->Send(task_id, payload)) {
        result.status = ServiceStatus{500, "send broadcast task to kafka failed"};
        result.task_id.clear();
        return result;
    }

    result.status = ServiceStatus{0, "ok"};
    result.task_id = task_id;
    return result;
}

// 【重要】这是一条与 Submit **完全不同**的旧路径，读代码时不要把它
// 当成“ HTTP 版的 Submit”。它被 /api/message/send 使用，名字里的
// Legacy 是字面意思。具体差异：
//
//   不走会话级串行锁   没有 KeyedMutex，同会话并发 HTTP 发送不互斥
//   不验 token          靠调用方（handleSendMessage）已经做完 requireUser
//   client_msg_id 为空  因此**没有幂等去重**：客户端重试会产生两条消息
//   走 AppendMessage    而不是 AppendMessageHotPath：前者会同步写
//                       message 表 + 标 reservation 为 Persisted(stage=3)
//                       + 更新会话 last_msg_seq，**完全绕开 Kafka**
//   不做实时扇出      没有任何 push_single/push_group 发送，也没有
//                       accepted 标志（result.accepted 永远是 false）
//
// 实际后果：走本接口发出的消息，对方**不会实时收到**，只能在自己
// 下一次拉历史/同步时看到；而且 HTTP 响应里不会带 ACK_STAGE_ACCEPTED。
// 真正的客户端走的是 WebSocket -> Comet -> gRPC -> Submit 那条路。
// 本函数存在的价值是给脚本/测试/web_demo 一个最简单的发消息入口。
// 不要拿它当“如何发一条消息”的参考实现。
UpstreamResult MessageService::SendLegacyText(int64_t sender, int64_t recipient,
                                              const std::string& content) {
    UpstreamResult result;
    if (sender <= 0 || recipient <= 0 || content.empty()) {
        result.status = {400, "from_user_id/to_user_id/content required"};
        return result;
    }
    if (content.size() > 64 * 1024) {
        result.status = {413, "message exceeds 64 KiB"};
        return result;
    }
    User user;
    std::string user_error;
    // 两次调用共用同一个 user 变量：这里只关心“能不能查到且未封禁”，
    // 不需要保留具体内容。因此第二次会把第一次的结果覆盖掉，没关系。
    // 注意收件人也要查：给一个已注销/已封禁的账号发消息应该当场拒，
    // 而不是落库后永远没人看到。
    if (!GetActiveUser(sender, &user, &user_error) ||
        !GetActiveUser(recipient, &user, &user_error)) {
        result.status = {403, "sender or recipient is unavailable"};
        return result;
    }
    if (!rate_limiter_.Allow("single", std::to_string(sender))) {
        result.status = {429, "single rate limit exceeded; retry later"};
        return result;
    }
    if (!store_) {
        result.status = {500, "conversation store not initialized"};
        return result;
    }
    Session session;
    std::string error;
    if (!store_->GetOrCreateSingleSession(sender, recipient, &session, &error)) {
        result.status = {500, "get session failed: " + error};
        return result;
    }
    const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
    const auto wire = nlohmann::json{{"type", "single_chat"}, {"session_id", session.id},
        {"from_user_id", sender}, {"to_user_id", recipient}, {"content", {{"text", content}}},
        {"create_time", now}}.dump();
    // client_msg_id 传空串：这就是上面说的“没有幂等去重”。
    // AppendMessage -> Reserve 里遇到空 client_msg_id 会用
    // GenerateSecureToken 生成一个随机值（见 message_reservation.cpp），
    // 因此每次调用都是一个全新的幂等键，重试必然产生新消息。
    if (!store_->AppendMessage(session, sender, "text", wire, now, "",
                               &result.message, &error)) {
        result.status = {500, "append message failed: " + error};
        return result;
    }
    // 只置 has_message，**不置 accepted**：本路径不走 Kafka，因此
    // “accepted = persist 事件已获 delivery report” 这个语义根本不成立。
    // 调用方（HandleUpstreamMessage）据此不会设 ACK_STAGE_ACCEPTED。
    // 但 HTTP handler 不用 UpstreamResult 的 accepted，它只回
    // session_id/msg_id/msg_seq，因此这个差异在 HTTP 响应里看不出来。
    result.has_message = true;
    return result;
}
// 广播的两个入口。差别只有两个：鉴权与限流。
//   Broadcast       gRPC 入口，验的是 "admin" 角色密钥（见 grpc_service.cpp），
//                   已经鉴权过了，因此这里不再查 actor，但**要限流**。
//   BroadcastAdmin  HTTP 入口，actor 由 requireAdmin 验完后代入 kAdminUserId，
//                   这里再跑一道 RequireAdministrator 做第二层保险，**不限流**。
BroadcastResult MessageService::Broadcast(const std::string& scope, int64_t group,
                                          const std::string& content) {
    return PublishBroadcast(scope, group, content, true);
}
BroadcastResult MessageService::BroadcastAdmin(int64_t actor, const std::string& scope,
                                               int64_t room, const std::string& text) {
    const auto access = RequireAdministrator(actor);
    if (!access.ok()) return {access, {}};
    if (text.empty()) return {{400, "text is required"}, {}};
    // HTTP 层的词汇是 "all"/"chatroom"，内部词汇是 "all"/"group"。
    // 转换在本函数里完成，因此 PublishBroadcast 只认后者。
    // 两套词汇共存是历史原因（见 http_server.cpp 里 handleAdminBroadcast
    // 的注释），不要把其中一个“统一”掉而不同步改客户端。
    if (scope != "all" && scope != "chatroom")
        return {{400, "scope must be all or chatroom"}, {}};
    if (scope == "chatroom" && room <= 0)
        return {{400, "room_id required when scope=chatroom"}, {}};
    const std::string real_scope = scope == "all" ? "all" : "group";
    const int64_t group = scope == "chatroom" ? room : 0;
    // Keep the legacy HTTP payload and rate-limit policy unchanged.
    //
    // 译：保留旧版 HTTP 负载格式与限流策略不变。
    //
    // 负载形状 {"type":"system_broadcast","scope":..,"content":{"text":..}}
    // 是客户端硬编码识别的，改了就会让旧前端把广播当成普通消息渲染。
    // group_id 只在房间广播时才加：全体广播没有房间概念，
    // 写个 0 进去反而会让客户端去找一个不存在的房间。
    // 最后一个参数 false = 不限流。
    nlohmann::json content = {{"type", "system_broadcast"}, {"scope", real_scope},
                              {"content", {{"text", text}}}};
    if (group > 0) content["group_id"] = group;
    return PublishBroadcast(real_scope, group, content.dump(), false);
}

}  // namespace sparkpush
