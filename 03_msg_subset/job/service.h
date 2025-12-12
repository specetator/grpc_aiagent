#pragma once

#include <grpcpp/grpcpp.h>

#include <mutex>
#include <string>
#include <unordered_map>

#include "config.h"
#include "kafka_consumer.h"
#include "spark_push.grpc.pb.h"
#include "thread_pool.h"

namespace sparkpush {

// JobRunner 负责从 Kafka 拉取任务并调用 Comet 进行推送的核心调度器。
class JobRunner {
   public:
    explicit JobRunner(const Config& cfg);

    bool Init();
    void Start();
    void Stop();

   private:
    void HandleMessage(const std::string& key, const std::string& value);
    void ProcessPushRequest(const PushToCometRequest& req);
    CometService::Stub* GetStub(const std::string& comet_id);
    void ParseCometTargets();

    Config cfg_;
    KafkaConsumer consumer_;
    std::unordered_map<std::string, std::string> comet_addrs_;
    std::unordered_map<std::string, std::unique_ptr<CometService::Stub>>
        comet_stubs_;
    std::mutex stub_mu_;
    ThreadPool rpc_pool_;
};

}  // namespace sparkpush
