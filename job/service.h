#pragma once

#include "config.h"
#include "kafka_consumer.h"
#include "message_dao.h"
#include "metrics_http_server.h"
#include "mysql_pool.h"
#include "session_dao.h"
#include "spark_push.grpc.pb.h"
#include "thread_pool.h"

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>

namespace sparkpush {

    // JobRunner 负责从 Kafka 拉取任务并调用 Comet 进行推送的核心调度器。
    //
    // 它是「阶段解耦」的实现体：四个独立 consumer group 分别处理
    // persist / single / group / broadcast，互不阻塞。这样广播风暴不会
    // 拖慢单聊投递，MySQL 卡住也不会阻止实时消息下发。
    //
    // | 场景 | topic | group | 处理 |
    // | persist | persist_message | *_persist | 幂等落库（全系统唯一写 message 表的地方）|
    // | single | push_single | *_single | PushStream 投递 |
    // | group | push_group | *_group | 聊天室 fanout |
    // | broadcast | broadcast_task | *_broadcast | 全量/定向广播 |
    //
    // 关键不变量：**只有 Comet 回 reply 后才提交 Kafka offset**。
    // 投递失败就不提交，消息会在重启/再平衡后重放。
    class JobRunner {
    public:
        // 使用配置构造 JobRunner，初始化线程池等基础资源。
        // 构造不做网络操作，真正的连接建立在 Init()。
        explicit JobRunner(const Config& cfg);

        // 初始化 Kafka 消费者、MySQL outbox consumer 与 Comet 长连接。
        bool Init();

        // 启动两个 Kafka 消费者，进入拉取消息状态。
        void Start();

        // 停止消费者与线程池，确保退出时资源释放。
        // 顺序很重要：先停 Kafka 回调（不再产生新请求），再停流，
        // 最后停 delivery_ack 线程池。
        void Stop();
        // 任一 consumer 进入不可恢复状态就返回 true。
        // 主循环据此退出并返回非 0，让容器编排重启进程。
        // 典型触发原因：DLQ 写入未确认、位点提交失败。
        bool Failed() const {
            return persist_consumer_.failed() || single_consumer_.failed() ||
                   group_consumer_.failed() || broadcast_consumer_.failed();
        }

    private:
        struct StreamState;

        // 兼容旧 push topic 的普通推送消息回调。
        // split_scene_topics_=false 时用它；否则由下面两个分场景回调接手。
        bool HandleMessage(const std::string& key, const std::string& value);
        bool HandleSingleMessage(const std::string& key,
                                 const std::string& value);
        bool HandleGroupMessage(const std::string& key,
                                const std::string& value);
        // 两个分场景回调的共同实现。scene_hint 用于补全旧事件里
        // 缺失的 scene 字段（参见 ProcessPushRequest 里的推导）。
        bool HandleMessageForScene(const std::string& key,
                                   const std::string& value,
                                   const std::string& scene_hint);

        // 广播任务的回调处理：解析广播请求并下发到各 Comet。
        // 对每个已配置的 comet_id 调 BuildBroadcastPush 生成定向请求。
        bool HandleBroadcastTask(const std::string& key, const std::string& value);

        // persist topic 的回调：反序列化后交给 PersistMessage。
        // 解析失败也返回 false（而不是 true 跳过）：脏消息要进 DLQ
        // 留存待查，静默跳过就永久丢了。
        bool HandlePersistMessage(const std::string& key,
                                  const std::string& value);

        // 幂等落库四步：建会话 -> 插消息 -> 标 outbox 已落库 -> 推水位。
        bool PersistMessage(const PersistMessageRequest& request);

        // 将解析好的 PushToCometRequest 发送到指定 Comet。
        // 流优先，Unary 兜底；两者都失败才计 delivery_lost。
        bool ProcessPushRequest(const PushToCometRequest& req);

        // 根据 comet_id 获取或创建对应的 gRPC Stub。
        CometService::Stub* GetStub(const std::string& comet_id);

        // ===== 每个 comet_id 一条双向 PushStream =====
        void InitStreams();
        void StreamWriterLoop(const std::string& comet_id);
        void StreamReaderLoop(const std::string& comet_id);
        // 重连时会把旧流上所有 pending 请求标为失败，
        // 让它们回到 Kafka 重试路径而不是静默丢失。
        bool ReconnectStream(const std::string& comet_id,
                             StreamState* state);
        // 同步语义：入队后阻塞等 reply。靠 PromiseState 把异步流
        // 桥接回 Kafka 消费的同步模型。
        bool SendToStream(const PushToCometRequest& request,
                          PushToCometReply* reply);
        // 向消息发送者所在的 Comet 回 delivered_ack（不是接收方）。
        void SendDeliveryAck(const PushToCometRequest& request);

        // 解析配置中的 comet_targets，填充 id -> address 映射。
        void ParseCometTargets();

        Config cfg_;
        // delivered_ack 用独立线程池：它是一次额外的同步 RPC，
        // 在流读线程里做会阻塞后续 reply 的分发。
        ThreadPool delivery_ack_pool_;
        KafkaConsumer single_consumer_;
        KafkaConsumer group_consumer_;
        KafkaConsumer broadcast_consumer_;
        KafkaConsumer persist_consumer_;
        // true 时用三套分场景 topic；false 时退回旧的单一 push topic。
        bool split_scene_topics_{true};
        MetricsHttpServer metrics_server_;
        std::unordered_map<std::string, std::string> comet_addrs_;
        std::unordered_map<std::string, std::unique_ptr<CometService::Stub>> comet_stubs_;
        std::mutex stub_mu_;   // 保护 comet_stubs_（Unary 兜底路径会并发建 stub）

        // 只有 persist 路径需要写库，因此 DAO 只在这里持有。
        std::unique_ptr<MySqlConnectionPool> mysql_pool_;
        std::unique_ptr<SessionDao> session_dao_;
        std::unique_ptr<MessageDao> message_dao_;

        // 异步流 -> 同步调用的桥。Kafka 消费线程阻塞在 cv 上，
        // 流读线程收到 reply 后置 done 并 notify。
        // 必须用 shared_ptr：重连时需要把所有 pending 的 completion
        // 批量标为失败，而那时请求方的栈帧可能已经不在。
        struct PromiseState {
            std::mutex mutex;
            std::condition_variable cv;
            bool done{false};
            bool ok{false};
            PushToCometReply response;
        };

        struct PendingRequest {
            PushToCometRequest request;   // 保留原请求，供 SendDeliveryAck 使用
            std::shared_ptr<PromiseState> completion;
        };

        // 一条到 Comet 的双向流状态。与 Comet 侧 StreamState 对称，
        // 但多了 channel/stub（每个 Comet 地址不同，需各自持有）。
        struct StreamState {
            std::shared_ptr<grpc::Channel> channel;
            std::unique_ptr<CometService::Stub> stub;
            std::unique_ptr<grpc::ClientContext> context;
            std::unique_ptr<grpc::ClientReaderWriter<PushToCometRequest,
                                                     PushToCometReply>> stream;
            std::thread writer_thread;
            std::thread reader_thread;
            std::queue<PendingRequest> queue;   // 有界，上限 push_stream_queue_max
            std::mutex queue_mutex;
            std::condition_variable queue_cv;
            std::unordered_map<std::string, PendingRequest> pending;  // request_id -> 请求
            std::mutex pending_mutex;
            std::atomic<bool> stream_broken{false};   // 读线程置位，写线程消费
        };

        std::unordered_map<std::string, std::unique_ptr<StreamState>> streams_;
        std::mutex stream_mu_;
        std::atomic<bool> streams_running_{false};
    };

}  // namespace sparkpush
