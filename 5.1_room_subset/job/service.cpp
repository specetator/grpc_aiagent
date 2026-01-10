#include "service.h"

#include <chrono>
#include <sstream>
#include <unordered_set>
#include <vector>

#include "logging.h"

namespace sparkpush {

// JobRunner 构造函数
// 初始化配置对象和 RPC 线程池
// 线程池大小从配置读取，默认为 4 个工作线程
JobRunner::JobRunner(const Config& cfg)
    : cfg_(cfg),
      rpc_pool_(cfg.job_rpc_worker_threads > 0 ? cfg.job_rpc_worker_threads : 4,
                "job_rpc_pool") {}

// 初始化 JobRunner
// 步骤：
// 1. 解析 Comet 目标节点配置
// 2. 检查 Kafka brokers 配置
// 3. 初始化 Kafka 消费者（关闭自动提交）
// 4. 启动 RPC 工作线程池
bool JobRunner::Init() {
    // 解析 Comet 目标节点列表，构建 comet_addrs_ 映射表
    ParseCometTargets();

    // 检查 Kafka brokers 是否配置
    if (cfg_.kafka_brokers.empty()) {
        LOG_ERROR << "Kafka brokers not configured";
        return false;
    }

    // 配置 Kafka 消费者选项
    KafkaConsumer::Options consumer_opts;
    consumer_opts.enable_auto_commit =
        false;  // 关闭自动提交，由消费者手动控制偏移量

    // 初始化 Kafka 消费者
    // 绑定 HandleMessage 作为消息处理回调函数
    if (!consumer_.Init(cfg_.kafka_brokers, cfg_.kafka_consumer_group,
                        cfg_.kafka_push_topic,
                        std::bind(&JobRunner::HandleMessage, this,
                                  std::placeholders::_1, std::placeholders::_2),
                        consumer_opts)) {
        LOG_ERROR << "Kafka consumer init failed";
        return false;
    }

    // 启动 RPC 线程池，准备处理推送请求
    rpc_pool_.Start();
    return true;
}

// 启动 JobRunner
// 启动 Kafka 消费者，开始接收和处理消息
void JobRunner::Start() { consumer_.Start(); }

// 停止 JobRunner
// 先停止 Kafka 消费者，再停止 RPC 线程池，确保优雅关闭
void JobRunner::Stop() {
    consumer_.Stop();  // 停止接收新消息
    rpc_pool_.Stop();  // 等待所有正在处理的 RPC 请求完成
}

// 解析 Comet 目标节点配置
// 配置格式：id1=addr1,id2=addr2,...
// 例如：comet1=localhost:50051,comet2=localhost:50052
void JobRunner::ParseCometTargets() {
    comet_addrs_.clear();

    // 如果配置为空，直接返回
    if (cfg_.comet_targets.empty()) {
        return;
    }

    // 使用逗号分割配置字符串
    std::stringstream ss(cfg_.comet_targets);
    std::string item;
    while (std::getline(ss, item, ',')) {
        // 查找等号位置，分离 ID 和地址
        auto pos = item.find('=');
        if (pos == std::string::npos) continue;  // 格式不正确，跳过

        std::string id = item.substr(0, pos);     // 提取 Comet ID
        std::string addr = item.substr(pos + 1);  // 提取 Comet 地址
        LOG_INFO << "Configured comet target: id=" << id << ", addr=" << addr;

        // 只有 ID 和地址都不为空时才添加到映射表
        if (!id.empty() && !addr.empty()) {
            comet_addrs_[id] = addr;
        }
    }
}

// 获取指定 Comet 节点的 gRPC Stub
// 实现连接池功能，首次请求时创建连接并缓存，后续请求直接使用缓存
// 线程安全：使用互斥锁保护 comet_stubs_ 映射表
CometService::Stub* JobRunner::GetStub(const std::string& comet_id) {
    std::lock_guard<std::mutex> lock(stub_mu_);  // 加锁保护共享资源

    // 1. 检查缓存中是否已存在该 Comet 的 Stub
    auto it = comet_stubs_.find(comet_id);
    if (it != comet_stubs_.end()) {
        return it->second.get();  // 直接返回缓存的 Stub
    }

    // 2. 查找该 Comet ID 对应的地址
    auto addr_it = comet_addrs_.find(comet_id);
    if (addr_it == comet_addrs_.end()) {
        return nullptr;  // Comet ID 不存在配置中
    }

    // 3. 创建 gRPC 通道和 Stub（使用不安全的凭证）
    auto channel = grpc::CreateChannel(addr_it->second,
                                       grpc::InsecureChannelCredentials());
    auto stub = CometService::NewStub(channel);
    auto* ptr = stub.get();

    // 4. 缓存新创建的 Stub，便于后续复用
    comet_stubs_[comet_id] = std::move(stub);
    return ptr;
}

// 处理从 Kafka 接收到的消息
// 该函数在 Kafka 消费者线程中被调用
// 为避免阻塞消费者线程，将实际处理任务提交到 RPC 线程池异步执行
void JobRunner::HandleMessage(const std::string& key,
                              const std::string& value) {
    (void)key;  // 当前未使用 Kafka 消息的 key

    // 将消息处理任务提交到线程池异步执行
    // 使用 lambda 捕获 value 的副本，避免引用失效
    rpc_pool_.Submit([this, payload = value]() {
        // 反序列化 Protobuf 消息
        PushToCometRequest req;
        if (!req.ParseFromString(payload)) {
            LOG_ERROR << "Failed to parse PushToCometRequest from Kafka";
            return;
        }

        // 处理推送请求
        ProcessPushRequest(req);
    });
}

// 处理推送请求
// 根据请求中的 comet_ids 字段决定推送目标：
// - 如果指定了 comet_ids，则推送到指定的 Comet 节点
// - 如果未指定，则广播到所有已配置的 Comet 节点
void JobRunner::ProcessPushRequest(const PushToCometRequest& req) {
    // 1. 确定目标 Comet 节点列表
    std::vector<std::string> comet_ids;
    if (req.comet_ids_size() > 0) {
        // 使用请求中指定的 Comet 节点
        comet_ids.reserve(req.comet_ids_size());
        for (const auto& cid : req.comet_ids()) {
            comet_ids.push_back(cid);
        }
    } else {
        // 未指定时，广播到所有已配置的 Comet 节点
        for (const auto& kv : comet_addrs_) {
            comet_ids.push_back(kv.first);
        }
    }

    // 2. 检查是否有可用的 Comet 节点
    if (comet_ids.empty()) {
        LOG_ERROR << "No comet targets available, drop msg_id="
                  << req.message().msg_id();
        return;
    }

    // 3. 遍历目标 Comet 节点，依次发送推送请求
    for (const auto& cid : comet_ids) {
        // 3.1 获取该 Comet 节点的 gRPC Stub
        CometService::Stub* stub = GetStub(cid);
        if (!stub) {
            LOG_ERROR << "Unknown comet_id " << cid
                      << " for msg_id=" << req.message().msg_id();
            continue;  // Stub 获取失败，跳过该节点
        }

        // 3.2 构造发送给单个 Comet 的请求（关键：按 comet_id 过滤 targets）
        //
        // 说明：
        // - logic/message_helper 会为同一 user_id 在不同 comet_id 上生成多个
        // targets
        // - 如果 job 不按 cid 过滤 targets，会导致每个 comet 收到包含“其他
        // comet”
        //   的 targets，从而在 comet 端按 user_id 推送时出现重复投递
        PushToCometRequest req2;
        *req2.mutable_message() = req.message();
        req2.add_comet_ids(cid);  // 便于 Comet 端日志记录

        // 如果 target.comet_id
        // 为空，认为是“未绑定节点”的目标（兼容旧逻辑/离线场景） 否则只保留
        // target.comet_id == cid 的目标
        for (const auto& t : req.targets()) {
            if (t.comet_id().empty() || t.comet_id() == cid) {
                *req2.add_targets() = t;
            }
        }

        // 若过滤后没有目标用户，直接跳过，避免无意义 RPC
        if (req.targets_size() > 0 && req2.targets_size() == 0) {
            LOG_INFO << "Skip PushToComet to comet_id=" << cid
                     << " because no matching targets. msg_id="
                     << req.message().msg_id();
            continue;
        }

        // 3.3 发起 gRPC 调用
        PushToCometReply reply;
        grpc::ClientContext ctx;
        LOG_INFO << "PushToComet to comet_id=" << cid
                 << ", msg_id=" << req.message().msg_id();
        auto status = stub->PushToComet(&ctx, req2, &reply);

        // 3.4 检查 RPC 调用是否成功
        if (!status.ok()) {
            LOG_ERROR << "PushToComet RPC failed to comet_id=" << cid << ": "
                      << status.error_message();
            continue;  // RPC 失败，跳过该节点，继续处理下一个
        }

        // 3.5 检查业务层返回的错误码
        if (reply.error().code() != 0) {
            LOG_ERROR << "Comet response error to comet_id=" << cid << ": "
                      << reply.error().message();
        }
    }
}

}  // namespace sparkpush
