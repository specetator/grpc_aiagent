#include "service.h"

#include "logging.h"

#include <chrono>
#include <sstream>

namespace sparkpush {

    // 构造函数：保存配置并初始化 RPC 线程池（至少 4 线程）。
    JobRunner::JobRunner(const Config& cfg)
        : cfg_(cfg),
          rpc_pool_(cfg.job_rpc_worker_threads > 0 ? cfg.job_rpc_worker_threads : 4,
                    "job_rpc_pool") {}

    // 初始化：解析 Comet 目标，准备 Kafka 消费者和线程池。
    bool JobRunner::Init() {
        ParseCometTargets();
        if (cfg_.kafka_brokers.empty()) {
            LOG_ERROR << "Kafka brokers not configured";
            return false;
        }

        KafkaConsumer::Options consumer_opts;
        consumer_opts.enable_auto_commit = false;  // 由业务决定提交时机

        // 普通推送消费者，订阅 push topic。
        if (!consumer_.Init(cfg_.kafka_brokers,
                            cfg_.kafka_consumer_group,
                            cfg_.kafka_push_topic,
                            std::bind(&JobRunner::HandleMessage,
                                      this,
                                      std::placeholders::_1,
                                      std::placeholders::_2),
                            consumer_opts)) {
            LOG_ERROR << "Kafka consumer init failed";
            return false;
        }

        // 广播任务消费者：订阅 broadcast_task topic。
        if (!broadcast_consumer_.Init(
                cfg_.kafka_brokers,
                cfg_.kafka_consumer_group,
                cfg_.kafka_broadcast_topic,
                std::bind(&JobRunner::HandleBroadcastTask,
                          this,
                          std::placeholders::_1,
                          std::placeholders::_2),
                consumer_opts)) {
            LOG_ERROR << "Kafka broadcast consumer init failed";
            return false;
        }

        rpc_pool_.Start();  // 启动 RPC 线程池，准备执行异步任务
        return true;
    }

    // 启动两个 Kafka 消费者。
    void JobRunner::Start() {
        consumer_.Start();
        broadcast_consumer_.Start();
    }

    // 停止所有正在运行的任务和消费者。
    void JobRunner::Stop() {
        consumer_.Stop();
        broadcast_consumer_.Stop();
        rpc_pool_.Stop();
    }

    // 解析配置中的 comet_targets，填充 comet_addrs_ 映射。
    void JobRunner::ParseCometTargets() {
        comet_addrs_.clear();
        if (cfg_.comet_targets.empty()) {
            return;
        }

        std::stringstream ss(cfg_.comet_targets);
            std::string item;
            while (std::getline(ss, item, ',')) {
                auto pos = item.find('=');
                if (pos == std::string::npos) {
                    continue;
                }
                std::string id = item.substr(0, pos);
                std::string addr = item.substr(pos + 1);
                LOG_INFO << "Configured comet target: id=" << id
                         << ", addr=" << addr;
            if (!id.empty() && !addr.empty()) {
                comet_addrs_[id] = addr;
            }
        }
    }

    // 获取指定 comet 的 Stub；若不存在则创建并缓存。
    CometService::Stub* JobRunner::GetStub(const std::string& comet_id) {
        std::lock_guard<std::mutex> lock(stub_mu_);  // 多线程访问时保护缓存

        auto it = comet_stubs_.find(comet_id);
        if (it != comet_stubs_.end()) {
            return it->second.get();
        }

        auto addr_it = comet_addrs_.find(comet_id);
        if (addr_it == comet_addrs_.end()) {
            return nullptr;  // 未配置的 comet_id
        }

        auto channel = grpc::CreateChannel(addr_it->second,
                                           grpc::InsecureChannelCredentials());
        auto stub = CometService::NewStub(channel);
        auto* ptr = stub.get();
        comet_stubs_[comet_id] = std::move(stub);
        return ptr;
    }

    // Kafka push topic 回调：解析请求并投递到 RPC 线程池。
    void JobRunner::HandleMessage(const std::string& key,
                                  const std::string& value) {
        (void)key;
        rpc_pool_.Submit([this, payload = value]() {
            PushToCometRequest req;
            if (!req.ParseFromString(payload)) {
                LOG_ERROR << "Failed to parse PushToCometRequest from Kafka";
                return;
            }
            ProcessPushRequest(req);
        });
    }

    // 将 PushToCometRequest 下发给对应的 Comet，并检查返回。
    void JobRunner::ProcessPushRequest(const PushToCometRequest& req) {
        const std::string& comet_id = req.comet_id();
        CometService::Stub* stub = GetStub(comet_id);
        if (!stub) {
            LOG_ERROR << "Unknown comet_id " << comet_id;
            return;
        }

        PushToCometReply reply;
        grpc::ClientContext ctx;
        LOG_INFO << "Processing PushToComet for comet_id=" << comet_id
                 << ", msg_id=" << req.message().msg_id();

        auto status = stub->PushToComet(&ctx, req, &reply);
        if (!status.ok()) {
            LOG_ERROR << "PushToComet RPC failed: " << status.error_message();
            return;
        }
        if (reply.error().code() != 0) {
            LOG_ERROR << "Comet response error: " << reply.error().message();
        }
    }

    // 广播任务回调：将广播内容构造为消息并推送到所有 Comet。
    void JobRunner::HandleBroadcastTask(const std::string& key,
                                        const std::string& value) {
        (void)key;
        BroadcastTaskRequest task;
        if (!task.ParseFromString(value)) {
            LOG_ERROR << "Failed to parse BroadcastTaskRequest from Kafka";
            return;
        }

        LOG_INFO << "HandleBroadcastTask, task_id=" << task.task_id()
                 << " scope=" << task.scope();

        // 简化实现：对所有已配置 comet 做“全体在线用户广播”。
        // 注意：这里没有 per-user 未读计数，仅做实时推送。

        // 构造一个 ChatMessage，承载广播内容。
        ChatMessage msg;
        msg.set_msg_id(task.task_id());
        msg.set_session_id("broadcast");
        msg.set_msg_seq(0);
        msg.set_sender_id(0);
        int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
        msg.set_timestamp_ms(now_ms);
        msg.set_msg_type("broadcast");
        msg.set_content_json(task.content_json());

        // 将该广播下发到所有 comet。
        for (const auto& kv : comet_addrs_) {
            const std::string& comet_id = kv.first;
            CometService::Stub* stub = GetStub(comet_id);
            if (!stub) {
                LOG_ERROR << "Unknown comet_id in broadcast: " << comet_id;
                continue;
            }

            PushToCometRequest req;
            req.set_comet_id(comet_id);
            *req.mutable_message() = msg;
            // targets 为空，后续 comet 侧可以扩展为空则表示“全量广播”。

            PushToCometReply reply;
            grpc::ClientContext ctx;
            auto status = stub->PushToComet(&ctx, req, &reply);
            if (!status.ok()) {
                LOG_ERROR << "Broadcast PushToComet RPC failed for comet "
                          << comet_id << ": " << status.error_message();
                continue;
            }
            if (reply.error().code() != 0) {
                LOG_ERROR << "Broadcast PushToComet error from comet "
                          << comet_id << ": " << reply.error().message();
            }
        }
    }

}  // namespace sparkpush



