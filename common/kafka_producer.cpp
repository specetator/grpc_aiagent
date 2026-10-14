#include "kafka_producer.h"

#include "logging.h"

#include <librdkafka/rdkafkacpp.h>

#include <chrono>
#include <condition_variable>
#include <memory>
#include <thread>

namespace sparkpush {

// delivery report 回调：librdkafka 在 poll 线程里调用它，告知一条消息
// 是否已被 broker 接受。
//
// 先在锁内摘下表项再锁外 notify：避免持 delivery_mutex_ 时去拿
// state->mutex，与 SendAndWait 的加锁顺序相反会死锁。
//
// 找不到表项时静默返回：SendAndWait 超时后调用方已离开，但表项
// 仍在，迟到的 report 会把它清掉；而 Send（无 opaque）的消息根本
// 不会进表，msg_opaque() 为空就直接返回。
class KafkaProducer::DeliveryCallback final : public RdKafka::DeliveryReportCb {
 public:
    explicit DeliveryCallback(KafkaProducer* owner) : owner_(owner) {}

    void dr_cb(RdKafka::Message& message) override {
        if (!owner_ || !message.msg_opaque()) return;
        std::shared_ptr<DeliveryState> state;
        {
            std::lock_guard<std::mutex> lock(owner_->delivery_mutex_);
            auto it = owner_->deliveries_.find(message.msg_opaque());
            if (it == owner_->deliveries_.end()) return;
            state = it->second;      // 拷一份 shared_ptr，保证锁外访问安全
            owner_->deliveries_.erase(it);
        }
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->done = true;
            state->ok = message.err() == RdKafka::ERR_NO_ERROR;
        }
        state->cv.notify_one();
    }

 private:
    KafkaProducer* owner_{nullptr};   // 不拥有；producer 先于本对象销毁前会停 poll
};

KafkaProducer::KafkaProducer() = default;

// 析构顺序不能颠倒：先停 poll 线程，再 flush，最后清表。
//
// 先停 poll 是因为 flush 自己会触发 delivery report 回调；若 poll 线程
// 还在跑，两边会同时调 dr_cb 争锁。清表放最后，避免 flush 期间
// 回调找不到表项而丢失唤醒。
//
// flush 给 3 秒上限：进程退出时不能无限等，宁可丢少量未发完的
// 异步消息（实时 topic 本来就不保证投递），也不能让停机卡死。
// 注意 persist topic 走的是 SendAndWait，已经在业务层同步确认过，
// 不依赖这里的 flush 兜底。
KafkaProducer::~KafkaProducer() {
    poll_running_ = false;
    if (poll_thread_.joinable()) {
        poll_thread_.join();
    }
    if (producer_) {
        producer_->flush(3000);
    }
    std::lock_guard<std::mutex> lock(delivery_mutex_);
    deliveries_.clear();
}

// 后台 poll 循环。
//
// poll(5) 而不是 poll(0)：阻塞最多 5ms 等事件，避开忙等把 CPU 跑满；
// 5ms 也足够短，不会让 delivery report 明显延迟。
//
// producer_ 为空时退化为 sleep：这只会发生在 Init 失败但对象仍存活
// 的短暂窗口，防御性地避开空指针解引用。
void KafkaProducer::PollLoop() {
    while (poll_running_) {
        if (producer_) {
            producer_->poll(5);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    }
}

// 初始化生产者：配置可靠性参数、创建全局 conf，并绑定目标 topic。
bool KafkaProducer::Init(const std::string& brokers, const std::string& topic) {
    std::string errstr;
    topic_name_ = topic;

    RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    delivery_callback_ = std::make_unique<DeliveryCallback>(this);
    auto set_conf = [&](const std::string& key, const std::string& value) {
        if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
            LOG_ERROR << "KafkaProducer set " << key << " failed: " << errstr;
            return false;
        }
        return true;
    };

    // 兼容老版本 librdkafka 的配置键名差异（compression / keepalive）
    auto set_conf_any = [&](std::initializer_list<std::pair<std::string, std::string>> kvs) {
        for (const auto& kv : kvs) {
            if (set_conf(kv.first, kv.second)) {
                return true;
            }
        }
        return false;
    };
    // 同一键尝试多个候选值，取第一组成功的配置
    auto set_conf_with_values = [&](const std::string& key,
                                    std::initializer_list<std::string> values) {
        for (const auto& v : values) {
            if (set_conf(key, v)) {
                return true;
            }
        }
        return false;
    };

    // 基础 broker 和可靠性、性能参数
    if (!set_conf("bootstrap.servers", brokers)) {
        delete conf;
        return false;
    }
    if (conf->set("dr_cb", delivery_callback_.get(), errstr) !=
        RdKafka::Conf::CONF_OK) {
        LOG_ERROR << "KafkaProducer set dr_cb failed: " << errstr;
        delete conf;
        return false;
    }
    // 可靠性核心：acks=all 要求所有 ISR 副本确认才算成功；
    // enable.idempotence=true 给每条消息带上序列号，broker 据此去重，
    // 因此下面的 retries=5 不会造成分区内重复。
    //
    // 两者都是硬要求，失败则直接让 Init 返回 false：缺了它们，
    // persist_message 的「accepted = 已 durable」承诺就不成立，
    // 而这是整个可靠性模型的地基。
    if (!set_conf("acks", "all") || !set_conf("enable.idempotence", "true")) {
        delete conf;
        return false;
    }
    // compression.type 在新版本可用，老版本需使用 compression.codec
    // 如果 zstd 未编译进 librdkafka，则依次回退 lz4 -> snappy -> gzip -> none
    bool compression_ok =
        // set_conf_any({{"compression.type", "zstd"}, {"compression.codec", "zstd"}}) ||
        set_conf_any({{"compression.type", "lz4"}, {"compression.codec", "lz4"}}) ||
        set_conf_any({{"compression.type", "snappy"}, {"compression.codec", "snappy"}}) ||
        set_conf_any({{"compression.type", "gzip"}, {"compression.codec", "gzip"}});
    if (!compression_ok) {
        // 仍失败则显式关闭压缩，避免启动时刷屏错误
        set_conf_with_values("compression.type", {"none"});
        set_conf_with_values("compression.codec", {"none"});
        LOG_WARN << "KafkaProducer compression algorithms unavailable, fallback to none";
    }
    // 批量与延迟的权衡：linger.ms=5 让生产者最多等 5ms 攒批，
    // 配合 batch.num.messages=1000 显著提高吞吐。
    // 5ms 对 IM 是可接受的额外延迟（人眼不可感知），换来的是
    // 压缩比与网络往返次数的大幅改善。
    set_conf("linger.ms", "5");
    set_conf("batch.num.messages", "1000");
    // retries + backoff 对付 broker 瞬时不可用。因为有幂等生产者，
    // 重试不会产生重复消息。message.timeout.ms=60000 是重试总上限，
    // 超过就把消息以失败上报给 dr_cb。
    set_conf("retries", "5");
    set_conf("retry.backoff.ms", "500");
    // socket.keepalive.enable 为新键，部分老版本使用 socket.keepalive
    set_conf_any({{"socket.keepalive.enable", "true"}, {"socket.keepalive", "true"}});
    set_conf("message.timeout.ms", "60000");
    // 本地发送队列上限（条数 + 字节）。这是生产端的背压闸：
    // 队列满时 produce 会返回 QUEUE_FULL，Send 据此返回 false，
    // 上层就能感知到「Kafka 跟不上」而不是默默堆积到 OOM。
    set_conf("queue.buffering.max.messages", "10000");
    set_conf("queue.buffering.max.kbytes", "32768");
    if (!topic_name_.empty()) {
        set_conf("client.id", "spark_push_producer_" + topic_name_);
    }

    producer_.reset(RdKafka::Producer::create(conf, errstr));
    delete conf;
    if (!producer_) {
        LOG_ERROR << "KafkaProducer set brokers failed: " << errstr;
        return false;
    }

    // 绑定 topic
    RdKafka::Conf* topic_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    topic_.reset(
        RdKafka::Topic::create(producer_.get(), topic_name_, topic_conf, errstr));
    delete topic_conf;
    if (!topic_) {
        LOG_ERROR << "KafkaProducer create topic failed: " << errstr;
        return false;
    }

    // 后台 poll：异步 produce 依赖定期 poll 才会真正发出
    poll_running_ = true;
    poll_thread_ = std::thread(&KafkaProducer::PollLoop, this);

    LOG_INFO << "KafkaProducer initialized topic=" << topic_name_
             << " (async produce + background poll)";
    return true;
}

// 异步发送。用于实时 topic，不等 broker 确认。
//
// RK_MSG_COPY 让 librdkafka 自己拷一份负载：value 是局部引用，
// 函数返回后就可能销毁，不拷会在真正发送时读到野内存。
// PARTITION_UA = unassigned，交给 librdkafka 按 key 哈希选分区。
//
// 末尾的 poll(0) 是必要的：它给 librdkafka 一个机会把队列里的消息
// 推出去并处理回调。不等价于 flush，因此不会阻塞。
bool KafkaProducer::Send(const std::string& key, const std::string& value) {
    if (!producer_ || !topic_) {
        return false;
    }

    RdKafka::ErrorCode err = producer_->produce(
        topic_.get(),
        RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(value.data()),
        value.size(),
        key.empty() ? nullptr : &key,
        nullptr);
    if (err != RdKafka::ERR_NO_ERROR) {
        LOG_ERROR << "Kafka produce failed: " << RdKafka::err2str(err);
        return false;
    }

    // 高性能路径：仅 poll 触发发送，不在每条消息上 flush。
    // 可靠性依赖 acks/idempotence/retries 与析构时 flush；吞吐优先。
    producer_->poll(0);
    return true;
}

// 同步发送并等 delivery report。persist_message 的 outbox 语义全靠它。
//
// 与 Send 的关键差异是第七个参数：把 state.get() 作为 opaque 交给
// librdkafka，它会在 dr_cb 里原样回传，从而把异步回调与本次调用对上。
//
// 顺序必须是「先登记表项，再 produce」：如果先 produce，poll 线程
// 可能在登记完成前就回调 dr_cb，那时查表会扑空，等待方就永远醒不了。
//
// 超时返回 false 时故意**不**把表项从 deliveries_ 里删掉：
// 那条消息可能仍在 broker 队列里，迟到的 report 需要能找到并清理它。
// 靠 shared_ptr，即使调用方栈帧已销毁也不会访问野内存。
bool KafkaProducer::SendAndWait(const std::string& key,
                                const std::string& value, int timeout_ms) {
    if (!producer_ || !topic_ || timeout_ms <= 0) return false;

    auto state = std::make_shared<DeliveryState>();
    void* opaque = state.get();
    {
        std::lock_guard<std::mutex> lock(delivery_mutex_);
        deliveries_[opaque] = state;
    }

    RdKafka::ErrorCode err = producer_->produce(
        topic_.get(), RdKafka::Topic::PARTITION_UA,
        RdKafka::Producer::RK_MSG_COPY,
        const_cast<char*>(value.data()), value.size(),
        key.empty() ? nullptr : &key, opaque);
    if (err != RdKafka::ERR_NO_ERROR) {
        std::lock_guard<std::mutex> lock(delivery_mutex_);
        deliveries_.erase(opaque);
        LOG_ERROR << "Kafka durable produce failed: " << RdKafka::err2str(err);
        return false;
    }
    producer_->poll(0);

    std::unique_lock<std::mutex> lock(state->mutex);
    const bool completed = state->cv.wait_for(
        lock, std::chrono::milliseconds(timeout_ms),
        [&state] { return state->done; });
    if (!completed) {
        LOG_ERROR << "Kafka durable delivery timeout topic=" << topic_name_;
        return false;
    }
    if (!state->ok) {
        LOG_ERROR << "Kafka durable delivery failed topic=" << topic_name_;
    }
    return state->ok;
}

}  // namespace sparkpush
