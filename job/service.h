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
    class JobRunner {
    public:
        // 使用配置构造 JobRunner，初始化线程池等基础资源。
        explicit JobRunner(const Config& cfg);

        // 初始化 Kafka 消费者、MySQL outbox consumer 与 Comet 长连接。
        bool Init();

        // 启动两个 Kafka 消费者，进入拉取消息状态。
        void Start();

        // 停止消费者与线程池，确保退出时资源释放。
        void Stop();

    private:
        struct StreamState;

        // 兼容旧 push topic 的普通推送消息回调。
        bool HandleMessage(const std::string& key, const std::string& value);
        bool HandleSingleMessage(const std::string& key,
                                 const std::string& value);
        bool HandleGroupMessage(const std::string& key,
                                const std::string& value);
        bool HandleMessageForScene(const std::string& key,
                                   const std::string& value,
                                   const std::string& scene_hint);

        // 广播任务的回调处理：解析广播请求并下发到各 Comet。
        bool HandleBroadcastTask(const std::string& key, const std::string& value);

        bool HandlePersistMessage(const std::string& key,
                                  const std::string& value);

        bool PersistMessage(const PersistMessageRequest& request);

        // 将解析好的 PushToCometRequest 发送到指定 Comet。
        bool ProcessPushRequest(const PushToCometRequest& req);

        // 根据 comet_id 获取或创建对应的 gRPC Stub。
        CometService::Stub* GetStub(const std::string& comet_id);

        void InitStreams();
        void StreamWriterLoop(const std::string& comet_id);
        void StreamReaderLoop(const std::string& comet_id);
        bool ReconnectStream(const std::string& comet_id,
                             StreamState* state);
        bool SendToStream(const PushToCometRequest& request,
                          PushToCometReply* reply);
        void SendDeliveryAck(const PushToCometRequest& request);

        // 解析配置中的 comet_targets，填充 id -> address 映射。
        void ParseCometTargets();

        Config cfg_;
        ThreadPool delivery_ack_pool_;
        KafkaConsumer single_consumer_;
        KafkaConsumer group_consumer_;
        KafkaConsumer broadcast_consumer_;
        KafkaConsumer persist_consumer_;
        bool split_scene_topics_{true};
        MetricsHttpServer metrics_server_;
        std::unordered_map<std::string, std::string> comet_addrs_;
        std::unordered_map<std::string, std::unique_ptr<CometService::Stub>> comet_stubs_;
        std::mutex stub_mu_;

        std::unique_ptr<MySqlConnectionPool> mysql_pool_;
        std::unique_ptr<SessionDao> session_dao_;
        std::unique_ptr<MessageDao> message_dao_;

        struct PromiseState {
            std::mutex mutex;
            std::condition_variable cv;
            bool done{false};
            bool ok{false};
            PushToCometReply response;
        };

        struct PendingRequest {
            PushToCometRequest request;
            std::shared_ptr<PromiseState> completion;
        };

        struct StreamState {
            std::shared_ptr<grpc::Channel> channel;
            std::unique_ptr<CometService::Stub> stub;
            std::unique_ptr<grpc::ClientContext> context;
            std::unique_ptr<grpc::ClientReaderWriter<PushToCometRequest,
                                                     PushToCometReply>> stream;
            std::thread writer_thread;
            std::thread reader_thread;
            std::queue<PendingRequest> queue;
            std::mutex queue_mutex;
            std::condition_variable queue_cv;
            std::unordered_map<std::string, PendingRequest> pending;
            std::mutex pending_mutex;
            std::atomic<bool> stream_broken{false};
        };

        std::unordered_map<std::string, std::unique_ptr<StreamState>> streams_;
        std::mutex stream_mu_;
        std::atomic<bool> streams_running_{false};
    };

}  // namespace sparkpush
