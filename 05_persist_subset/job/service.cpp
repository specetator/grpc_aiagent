#include "service.h"

#include <chrono>
#include <sstream>
#include <vector>

#include "logging.h"

namespace sparkpush {

JobRunner::JobRunner(const Config& cfg)
    : cfg_(cfg),
      rpc_pool_(cfg.job_rpc_worker_threads > 0 ? cfg.job_rpc_worker_threads : 4,
                "job_rpc_pool") {}

bool JobRunner::Init() {
    ParseCometTargets();
    if (cfg_.kafka_brokers.empty()) {
        LOG_ERROR << "Kafka brokers not configured";
        return false;
    }

    KafkaConsumer::Options consumer_opts;
    consumer_opts.enable_auto_commit = false;

    if (!consumer_.Init(cfg_.kafka_brokers, cfg_.kafka_consumer_group,
                        cfg_.kafka_push_topic,
                        std::bind(&JobRunner::HandleMessage, this,
                                  std::placeholders::_1, std::placeholders::_2),
                        consumer_opts)) {
        LOG_ERROR << "Kafka consumer init failed";
        return false;
    }

    rpc_pool_.Start();
    return true;
}

void JobRunner::Start() { consumer_.Start(); }

void JobRunner::Stop() {
    consumer_.Stop();
    rpc_pool_.Stop();
}

void JobRunner::ParseCometTargets() {
    comet_addrs_.clear();
    if (cfg_.comet_targets.empty()) {
        return;
    }

    std::stringstream ss(cfg_.comet_targets);
    std::string item;
    while (std::getline(ss, item, ',')) {
        auto pos = item.find('=');
        if (pos == std::string::npos) continue;
        std::string id = item.substr(0, pos);
        std::string addr = item.substr(pos + 1);
        LOG_INFO << "Configured comet target: id=" << id << ", addr=" << addr;
        if (!id.empty() && !addr.empty()) {
            comet_addrs_[id] = addr;
        }
    }
}

CometService::Stub* JobRunner::GetStub(const std::string& comet_id) {
    std::lock_guard<std::mutex> lock(stub_mu_);
    auto it = comet_stubs_.find(comet_id);
    if (it != comet_stubs_.end()) {
        return it->second.get();
    }
    auto addr_it = comet_addrs_.find(comet_id);
    if (addr_it == comet_addrs_.end()) {
        return nullptr;
    }
    auto channel = grpc::CreateChannel(addr_it->second,
                                       grpc::InsecureChannelCredentials());
    auto stub = CometService::NewStub(channel);
    auto* ptr = stub.get();
    comet_stubs_[comet_id] = std::move(stub);
    return ptr;
}

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

void JobRunner::ProcessPushRequest(const PushToCometRequest& req) {
    const std::string& comet_id = req.comet_id();  // comet_id
    if (comet_id.empty()) {
        // comet_id 为空但 targets
        // 也为空：通常是离线占位/持久化路径，无法路由到用户， 广播只会让 comet
        // 报 "targets empty but unsupported session_id"。
        // 仅当是明确的广播语义（session_id="broadcast"）时才允许 targets
        // 为空广播。
        if (req.targets_size() == 0 &&
            req.message().session_id() != "broadcast") {
            LOG_WARN
                << "Skip PushToComet with empty comet_id and empty targets, "
                   "session_id="
                << req.message().session_id()
                << ", msg_id=" << req.message().msg_id();
            return;
        }
        // logic 的 HTTP 推送/离线占位可能不带 comet_id。
        // 为避免路由缺失导致丢消息：当 comet_id 为空时，广播到所有已配置的
        // comet， 由各 comet 在本机连接集合内自行匹配 targets。
        if (comet_addrs_.empty()) {
            LOG_ERROR
                << "No comet targets configured, drop PushToComet with empty "
                   "comet_id, msg_id="
                << req.message().msg_id();
            return;
        }
        std::vector<std::string> comet_ids;
        comet_ids.reserve(comet_addrs_.size());
        for (const auto& kv : comet_addrs_) {
            comet_ids.push_back(kv.first);
        }
        LOG_WARN << "Broadcast PushToComet with empty comet_id to "
                 << comet_ids.size()
                 << " comets, msg_id=" << req.message().msg_id()
                 << ", targets_size=" << req.targets_size();
        for (const auto& cid : comet_ids) {
            CometService::Stub* stub = GetStub(cid);
            if (!stub) {
                LOG_ERROR << "Unknown comet_id " << cid
                          << " while broadcasting, msg_id="
                          << req.message().msg_id();
                continue;
            }
            PushToCometRequest req2(req);
            req2.set_comet_id(cid);
            PushToCometReply reply;
            grpc::ClientContext ctx;
            auto status = stub->PushToComet(&ctx, req2, &reply);
            if (!status.ok()) {
                LOG_ERROR
                    << "PushToComet RPC failed while broadcasting to comet_id="
                    << cid << ": " << status.error_message();
                continue;
            }
            if (reply.error().code() != 0) {
                LOG_ERROR
                    << "Comet response error while broadcasting to comet_id="
                    << cid << ": " << reply.error().message();
            }
        }
        return;
    }
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

}  // namespace sparkpush
