#include "kafka_consumer.h"

#include "logging.h"
#include "metrics.h"

#include <librdkafka/rdkafkacpp.h>
#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>
#include <unordered_map>
#include <nlohmann/json.hpp>

namespace sparkpush {
namespace {
// Kafka records may contain invalid UTF-8 or binary data; retain exact bytes.
std::string Hex(const std::string& bytes) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(bytes.size() * 2);
    for (unsigned char c : bytes) {
        result += digits[c >> 4];
        result += digits[c & 15];
    }
    return result;
}
}

// 析构时主动停止消费，确保后台线程退出。
KafkaConsumer::~KafkaConsumer() {
    Stop();
}

// 初始化消费者，配置集群、消费组与订阅主题，并注册消息回调。
//
// 本函数大量篇幅在做**配置一致性校验**，因为错配会造成静默的消息丢失：
//   启用 DLQ 时必须关闭 auto commit：自动提交会在业务处理前就推位点，
//     失败的消息既不会重试也不会进 DLQ，直接消失。
//   dead_letter_topic 不能等于源 topic：否则失败消息会被自己重新消费，
//     形成无限重试循环。
//   processing_workers > 1 时也必须关 auto commit：并行处理下位点
//     必须由 ProcessBatch 在整批成功后统一提交。
//
// 这些校验失败时直接返回 false（让服务起不来），而不是降级运行。
bool KafkaConsumer::Init(const std::string& brokers,
                         const std::string& group_id,
                         const std::string& topic,
                         std::function<bool(const std::string&, const std::string&)> callback,
                         const Options& options) {
    if (thread_.joinable() || consumer_) return false;
    dead_letter_producer_.reset();
    failed_ = false;
    topic_ = topic;
    group_id_ = group_id;
    callback_ = std::move(callback);
    options_ = options;
    if (options_.processing_workers < 1 || options_.processing_workers > 64 ||
        options_.max_batch_records < options_.processing_workers ||
        options_.max_batch_records > 1024 || options_.batch_window_ms < 0 ||
        (options_.processing_workers > 1 && options_.enable_auto_commit)) return false;
    if (!options_.dead_letter_topic.empty()) {
        if (options_.enable_auto_commit || options_.dead_letter_timeout_ms <= 0 ||
            options_.dead_letter_topic == topic_) return false;
        dead_letter_producer_ = std::make_unique<KafkaProducer>();
        if (!dead_letter_producer_->Init(brokers, options_.dead_letter_topic))
            return false;
    }

    std::string errstr;
    // 创建全局配置
    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    bool config_ok = true;
    auto set_config = [&](const std::string& key, const std::string& value) {
        if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
            config_ok = false;
            LOG_ERROR << "KafkaConsumer rejected config key=" << key;
        }
    };
    set_config("bootstrap.servers", brokers);
    set_config("group.id", group_id);
    // 关闭 partition EOF 事件：否则读到分区末尾时会收到一个 err 非 0 的
    // 伪消息，落到 default 分支被当成错误刷日志。
    set_config("enable.partition.eof", "false");
    set_config("auto.offset.reset", options_.auto_offset_reset);
    set_config("enable.auto.commit", options_.enable_auto_commit ? "true" : "false");
    // enable.auto.offset.store=false 是关键：它把「消费到」与「已处理完」
    // 解耦。默认行为是 consume() 返回就自动记下位点，那样即使业务
    // 失败，下次重启也不会重放。关掉后必须显式 commitSync 才推位点。
    if (dead_letter_producer_ || options_.processing_workers > 1)
        set_config("enable.auto.offset.store", "false");
    set_config("auto.commit.interval.ms", std::to_string(options_.auto_commit_interval_ms));
    // max.poll.interval.ms 必须大于单条消息的最坏处理时间（包含重试与
    // DLQ 发送），否则 broker 会认为消费者卡死而触发组再平衡，
    // 造成消息重复处理。
    set_config("max.poll.interval.ms", std::to_string(options_.max_poll_interval_ms));
    set_config("session.timeout.ms", std::to_string(options_.session_timeout_ms));
    if (!config_ok) {
        delete conf;
        return false;
    }

    consumer_.reset(RdKafka::KafkaConsumer::create(conf, errstr));
    delete conf;
    if (!consumer_) {
        LOG_ERROR << "KafkaConsumer create failed: " << errstr;
        return false;
    }

    // 订阅目标 topic
    std::vector<std::string> topics = {topic_};
    RdKafka::ErrorCode err = consumer_->subscribe(topics);
    if (err != RdKafka::ERR_NO_ERROR) {
        LOG_ERROR << "KafkaConsumer subscribe failed: " << RdKafka::err2str(err);
        return false;
    }
    return true;
}

// 启动消费线程，避免重复启动。
void KafkaConsumer::Start() {
    if (running_ || thread_.joinable() || failed_ || !consumer_) {
        return;
    }
    running_ = true;
    thread_ = std::thread(&KafkaConsumer::Loop, this);
}

// 停止消费并释放底层 consumer 资源。
void KafkaConsumer::Stop() {
    running_ = false;
    if (thread_.joinable()) {
        thread_.join();
    }
    if (consumer_) {
        consumer_->close();
        consumer_.reset();
    }
}

// 阻塞拉取消息的主循环，使用超时便于及时退出。
//
// consume(1000) 的 1s 超时就是停机响应上限：Stop() 置 running_=false 后，
// 最多 1s 循环就会退出，不需要额外的唤醒机制。
//
// 两条路径：
//   processing_workers == 1  逐条处理并逐条 commitSync（HandleMessage 默认 commit=true）
//   processing_workers > 1   先攒批再交给 ProcessBatch 并行处理，
//                            整批成功后统一提交
// 错误消息（err != NO_ERROR）总是走逐条路径：它们不需要并行，
// 且 HandleMessage 里会按错误码分类处理。
void KafkaConsumer::Loop() {
    // 简单的阻塞拉取循环，使用 1 秒超时以便及时感知停止信号
    while (running_) {
        std::unique_ptr<RdKafka::Message> msg(consumer_->consume(1000));
        if (!msg) {
            continue;
        }
        if (options_.processing_workers == 1 || msg->err() != RdKafka::ERR_NO_ERROR) {
            HandleMessage(msg.get());
            continue;
        }
        // 攒批：在 batch_window_ms 窗口内尽量凑足 max_batch_records 条。
        // 窗口很短（默认 10ms），目的是在不明显增加延迟的前提下
        // 把多条记录的 commit 开销合并。
        std::vector<std::unique_ptr<RdKafka::Message>> batch;
        batch.push_back(std::move(msg));
        const auto until = std::chrono::steady_clock::now() +
            std::chrono::milliseconds(options_.batch_window_ms);
        while (running_ && batch.size() < static_cast<size_t>(options_.max_batch_records) &&
               std::chrono::steady_clock::now() < until) {
            std::unique_ptr<RdKafka::Message> next(consumer_->consume(1));
            if (next && next->err() == RdKafka::ERR_NO_ERROR)
                batch.push_back(std::move(next));
            // 攒批期间也可能拉到错误消息（比如分区错误），它们不能入批，
            // 必须就地走逐条处理。TIMED_OUT 是正常现象，忽略。
            else if (next && next->err() != RdKafka::ERR__TIMED_OUT)
                HandleMessage(next.get());
        }
        ProcessBatch(batch);
    }
    if (failed_) {
        // Release assignment promptly so a replacement can replay the record.
        // 进入 failed_ 状态后立即 close 并释放 consumer：这会主动退出
        // 消费组并交出分区，让另一个实例（或重启后的自己）能立即
        // 接管并从未提交的位点重放。若只是停循环不 close，分区会被
        // 占着直到 session.timeout，恢复窗口白白变长。
        consumer_->close();
        consumer_.reset();
    }
}

// 按错误码分类处理 Kafka 消息，确保业务回调后再提交位点。
//
// 本函数是整个可靠消费模型的核心，不变量是：
//   **只有在「业务成功」或「DLQ 已确认写入」之后才提交位点。**
// 两者都不满足时不提交，消息会在下次重启/再平衡时重放。
//
// commit 参数：逐条路径传 true；ProcessBatch 传 false，因为并行模式下
// 位点必须由整批统一提交，否则后完成的记录会把位点推到前面
// 还没成功的记录之后，造成它们永久丢失。
bool KafkaConsumer::HandleMessage(RdKafka::Message* message, bool commit) {
    switch (message->err()) {
        case RdKafka::ERR_NO_ERROR: {
            // 消费滞后指标：用消息自带的 broker 时间戳与当前时间差。
            // 注意这是全局 gauge（名字不分 topic），后写者会覆盖先写者；
            // 作为「量级提示」足够，不能当精确的每 topic 滞后看。
            const auto timestamp = message->timestamp();
            if (timestamp.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE &&
                timestamp.timestamp > 0) {
                const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
                MetricsRegistry::Instance().Set(
                    "spark_push_kafka_lag_ms",
                    std::max<int64_t>(0, now_ms - timestamp.timestamp));
            }
            std::string key;
            if (message->key()) {
                key = *message->key();
            }
            std::string value;
            if (message->len())
                value.assign(static_cast<const char*>(message->payload()), message->len());
            // 业务回调重试。默认 3 次，递增退避（backoff * attempt）。
            //
            // handled 的初值是 !callback_：没设回调时视为「已处理」，
            // 避开因为配置遗漏而把所有消息都退到 DLQ。
            //
            // 回调抛异常被吃掉并记为失败：异常穿透到这里会终止消费线程，
            // 一个脏消息就能永久停掉整个 consumer。
            bool handled = !callback_;
            const int attempts = std::max(1, options_.max_processing_attempts);
            for (int attempt = 1; callback_ && attempt <= attempts; ++attempt) {
                try {
                    handled = callback_(key, value);
                } catch (...) {
                    // Exception text can contain message bodies/credentials.
                    // 故意不打 ex.what()：异常文本里可能带着消息正文
                    // （含用户聊天内容）或连接串。只记 topic，
                    // 具体原因靠 offset 去 DLQ 里查。
                    LOG_ERROR << "Kafka business callback threw topic=" << topic_;
                    handled = false;
                }
                if (handled) {
                    break;
                }
                if (attempt < attempts) {
                    LOG_WARN << "Kafka business callback failed, retrying topic="
                             << message->topic_name()
                             << " partition=" << message->partition()
                             << " offset=" << message->offset()
                             << " attempt=" << attempt << "/" << attempts;
                    std::this_thread::sleep_for(std::chrono::milliseconds(
                        std::max(0, options_.processing_retry_backoff_ms) * attempt));
                }
            }

            // 重试耗尽后的死信处理。
            //
            // DLQ 记录里存的是 **hex 编码**的 key 与 payload（而不是原文）：
            // Kafka records may contain invalid UTF-8 or binary data。
            // 本项目的事件是 protobuf 序列化字节，含大量不可打印字符；
            // 直接当字符串存会丢失字节或被 JSON 转义破坏，hex 能无损保留。
            // 重放工具 tools/dlq_replay.cpp 会把它解回来。
            //
            // id 用 [group, topic, partition, offset] 四元组：天然唯一且可幂等，
            // 同一条消息重复进 DLQ 时能用同一 id 去重。
            if (!handled) {
                LOG_ERROR << "Kafka processing exhausted topic="
                          << message->topic_name()
                          << " partition=" << message->partition()
                          << " offset=" << message->offset();
                if (dead_letter_producer_) {
                    try {
                        const auto id = nlohmann::json::array(
                            {group_id_, topic_, message->partition(), message->offset()}).dump();
                        nlohmann::json failure = {
                            {"schema", "sparkpush.dead_letter.v1"}, {"id", id},
                            {"consumer_group", group_id_}, {"topic", topic_},
                            {"partition", message->partition()}, {"offset", message->offset()},
                            {"timestamp_ms", timestamp.timestamp},
                            {"key_is_null", message->key() == nullptr},
                            {"payload_is_null", message->payload() == nullptr},
                            {"key_hex", Hex(key)}, {"payload_hex", Hex(value)},
                            {"attempts", attempts}, {"reason", "processing_exhausted"}};
                        if (options_.failure_recovery) {
                            auto recovery = options_.failure_recovery(key, value);
                            if (!recovery.topic.empty()) {
                                failure["recovery"] = {{"topic", recovery.topic},
                                    {"key_hex", Hex(recovery.key)},
                                    {"payload_hex", Hex(recovery.payload)}};
                            }
                        }
                        handled = dead_letter_producer_->SendAndWait(
                            id, failure.dump(), options_.dead_letter_timeout_ms);
                    } catch (...) {
                        handled = false;
                    }
                    if (!handled) {
                        // DLQ 写入也没确认 -> **停机**，而不是跳过这条消息。
                        //
                        // 这是整个消费模型里最重要的一个决策：此时消息既没
                        // 被业务处理，也没进 DLQ。若选择提交位点继续跑，
                        // 这条消息就永久消失了且无人知晓。因此宁可把
                        // consumer 停掉（failed_=true），让运维介入，
                        // 也绝不静默丢数据。位点不提交，重启后会重放。
                        failed_ = true;
                        running_ = false;
                        MetricsRegistry::Instance().Set(
                            "spark_push_kafka_consumer_failed_" + topic_, 1);
                        LOG_ERROR << "Kafka consumer halted: DLQ unconfirmed topic=" << topic_;
                        return false;
                    }
                    MetricsRegistry::Instance().Increment(
                        "spark_push_kafka_dead_letters_total_" + topic_);
                }
            }

            // Durable mode requires business success or confirmed DLQ delivery.
            // Other consumers retain their existing best-effort failure policy.
            //
            // 用 commitSync 而不是异步 commit：必须知道提交是否成功。
            // 提交失败时，若开了 DLQ（= durable 模式）就停机；
            // 否则只记日志继续跑（best-effort 消费者允许少量重复）。
            if (commit && !options_.enable_auto_commit && consumer_) {
                RdKafka::ErrorCode commit_err = consumer_->commitSync(message);
                if (commit_err != RdKafka::ERR_NO_ERROR) {
                    LOG_ERROR << "Kafka offset commit failed: "
                              << RdKafka::err2str(commit_err);
                    if (dead_letter_producer_) {
                        failed_ = true;
                        running_ = false;
                        MetricsRegistry::Instance().Set(
                            "spark_push_kafka_consumer_failed_" + topic_, 1);
                    }
                }
            }
            if (!handled && options_.processing_workers > 1) return false;
            break;
        }
        case RdKafka::ERR__TIMED_OUT:
            // 正常超时，无需日志
            break;
        default:
            LOG_ERROR << "Kafka consume error: " << message->errstr();
            break;
    }
    return !failed_;
}

// 并行处理一批记录，整批成功后统一提交位点。
//
// 分道（lane）策略：同一 Kafka key 的所有记录固定分到同一条道，
// 道内串行执行。Logic 发 ai_request 时用 session_id 作 key，因此
// 这保证了**同一会话的 Agent 请求严格按序执行**，而不同会话可以并行。
// 没有这个约束，同一会话的两轮对话可能乱序进模型，上下文就错了。
//
// 新 key 分到当前最短的道（负载均衡）；assigned 记住 key->lane 映射，
// 保证同 key 不会因后续记录而换道。
void KafkaConsumer::ProcessBatch(std::vector<std::unique_ptr<RdKafka::Message>>& batch) {
    // Logic publishes ai_request with session_id as its Kafka key. Assign all
    // records for a key to the same lane, preserving order within and across
    // batches. Other sessions can execute on independent lanes.
    const size_t lanes = std::min(batch.size(), static_cast<size_t>(options_.processing_workers));
    std::vector<std::vector<size_t>> jobs(lanes);
    std::unordered_map<std::string, size_t> assigned;
    for (size_t i = 0; i < batch.size(); ++i) {
        const std::string key = batch[i]->key() ? *batch[i]->key() : std::string{};
        auto it = assigned.find(key);
        if (it == assigned.end()) {
            const auto lane = std::min_element(jobs.begin(), jobs.end(),
                [](const auto& a, const auto& b) { return a.size() < b.size(); }) - jobs.begin();
            it = assigned.emplace(key, lane).first;
        }
        jobs[it->second].push_back(i);
    }
    std::atomic<bool> durable{true};
    std::vector<std::thread> workers;
    MetricsRegistry::Instance().Set("spark_push_kafka_batch_records_" + topic_, batch.size());
    for (const auto& lane : jobs) {
        workers.emplace_back([&, lane] {
            for (size_t index : lane) {
                if (failed_ || !HandleMessage(batch[index].get(), false)) {
                    durable = false;
                    break;
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    // A failed batch is replayed in full. In particular a later completion can
    // never advance an offset past an earlier unfinished/failed record.
    //
    // 整批重放而不是只重放失败那条：Kafka 的位点是单调的，无法表达
    // 「跳着提交」。若只提交成功的那些，中间失败记录之后的位点
    // 一旦推上去，失败记录就再也不会被重放。
    // 代价是成功的记录会被重复处理，因此业务回调必须幂等
    // （persist 靠 uk_session_seq，ai_request 靠 request_id）。
    if (!durable || failed_) {
        failed_ = true; running_ = false;
        return;
    }
    // 逐条 commitSync：commitSync(record) 提交的是「该记录之后」的位点，
    // 因此必须按 offset 递增顺序提交。batch 本身就是 poll 顺序，天然有序。
    // 任一条提交失败就停机：位点状态已不确定，继续跑可能造成
    // 大量重复处理或静默跳过。
    for (const auto& record : batch) {
        const auto error = consumer_->commitSync(record.get());
        if (error != RdKafka::ERR_NO_ERROR) {
            LOG_ERROR << "Parallel Kafka batch commit failed topic=" << topic_;
            failed_ = true; running_ = false;
            return;
        }
    }
    MetricsRegistry::Instance().Set("spark_push_kafka_batch_records_" + topic_, 0);
}

}  // namespace sparkpush
