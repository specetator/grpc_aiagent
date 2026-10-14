#pragma once

#include <librdkafka/rdkafkacpp.h>

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace sparkpush {

// 简单封装 rdkafka 生产者，提供初始化与发送接口。
//
// 两种发送语义（这是本类最重要的区分）：
//   Send         异步、不等确认。用于 push_single/push_group/broadcast_task
//                这类实时 topic：投递失败不影响已成立的 accepted，
//                客户端重连后由 sync 补齐，因此吞吐优先。
//   SendAndWait  同步等 delivery report。用于 persist_message：
//                accepted_ack 的语义就是「事件已进 Kafka」，不等确认
//                就无法成立这个承诺。
//
// 可靠性配置在 Init 里统一设：acks=all + enable.idempotence=true。
// 幂等生产者让 librdkafka 内部的 retries 不会造成 broker 侧重复，
// 这是「至少一次事件 + 幂等消费」链路的前提。
class KafkaProducer {
public:
    KafkaProducer();
    // 析构时会尝试 flush，减少未发送消息。
    ~KafkaProducer();

    // 初始化生产者并绑定目标 topic。一个实例只绑一个 topic，
    // 因此 Logic 会同时持有 single/group/broadcast/persist 四个生产者。
    bool Init(const std::string& brokers, const std::string& topic);
    // 发送单条消息，key 可选用于分区（异步，不逐条 flush）
    //
    // key 的作用不只是分区：把同一 comet_id（或 session_id）的消息
    // 映射到同一分区，才能在分区内保持 msg_seq 顺序。
    bool Send(const std::string& key, const std::string& value);
    // 发送并等待 librdkafka delivery report，用于持久化 topic 的 outbox 语义。
    //
    // 超时返回 false，但消息**可能仍会最终送达**（broker 慢而非丢）。
    // 调用方因此必须用同一个 msg_id 重试，而不是重新分配序号，
    // 否则会在历史里产生重复消息。参见 MessageService::PersistToTopic。
    bool SendAndWait(const std::string& key, const std::string& value,
                     int timeout_ms);

private:
    // 单次同步发送的完成凭证。
    //
    // 必须用 shared_ptr 持有：SendAndWait 超时返回后它的栈帧就没了，
    // 但 delivery report 可能稍后才到。shared_ptr 让 dr_cb 仍能找到
    // 并安全地清理表项，不会访问已销毁的栈内存。
    struct DeliveryState {
        std::mutex mutex;
        std::condition_variable cv;
        bool done{false};
        bool ok{false};
    };

    class DeliveryCallback;
    // 后台 poll 线程。librdkafka 的异步 produce 必须靠定期 poll 才会
    // 真正发出并回调 delivery report；没有它消息会一直积在本地队列里。
    void PollLoop();

    std::unique_ptr<RdKafka::Producer> producer_;
    std::unique_ptr<RdKafka::Topic> topic_;
    std::string topic_name_;
    std::atomic<bool> poll_running_{false};
    std::thread poll_thread_;
    std::unique_ptr<DeliveryCallback> delivery_callback_;
    std::mutex delivery_mutex_;
    // opaque 指针 -> 等待状态。用 state.get() 作键而不是自增 id：
    // 地址天然唯一，且 librdkafka 会把 opaque 原样回传给 dr_cb，
    // 无需额外维护计数器与映射。
    std::unordered_map<void*, std::shared_ptr<DeliveryState>> deliveries_;
};

}  // namespace sparkpush
