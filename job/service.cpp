#include "service.h"

#include "grpc_keepalive.h"
#include "logging.h"
#include "metrics.h"

#include <algorithm>
#include <chrono>
#include <sstream>
#include <thread>
#include <vector>

namespace sparkpush {

JobRunner::JobRunner(const Config& cfg)
    : cfg_(cfg), delivery_ack_pool_(4, "job_delivery_ack") {}

bool JobRunner::Init() {
    ParseCometTargets();
    if (cfg_.kafka_brokers.empty() || cfg_.kafka_single_topic.empty() ||
        cfg_.kafka_group_topic.empty() || cfg_.kafka_persist_topic.empty()) {
        LOG_ERROR << "Kafka scene/persist topics not configured";
        return false;
    }

    // Job 是持久化 topic 的唯一 MySQL 消费者，写库失败时不提交 Kafka 位点。
    mysql_pool_ = std::make_unique<MySqlConnectionPool>();
    MySqlConfig mysql_config;
    mysql_config.host = cfg_.mysql_host;
    mysql_config.port = cfg_.mysql_port;
    mysql_config.user = cfg_.mysql_user;
    mysql_config.password = cfg_.mysql_password;
    mysql_config.db = cfg_.mysql_db;
    mysql_config.pool_size = cfg_.mysql_pool_size > 0 ? cfg_.mysql_pool_size : 4;
    mysql_config.min_pool_size = cfg_.mysql_pool_min_size > 0
                                     ? cfg_.mysql_pool_min_size
                                     : mysql_config.pool_size;
    mysql_config.max_pool_size = cfg_.mysql_pool_max_size > 0
                                     ? cfg_.mysql_pool_max_size
                                     : std::max(mysql_config.min_pool_size,
                                                mysql_config.pool_size);
    mysql_config.idle_timeout_ms = cfg_.mysql_idle_timeout_ms;
    if (!mysql_pool_->Init(mysql_config)) {
        LOG_ERROR << "Job MySQL pool init failed";
        return false;
    }
    session_dao_ = std::make_unique<SessionDao>(mysql_pool_.get());
    message_dao_ = std::make_unique<MessageDao>(mysql_pool_.get());
    group_member_dao_ = std::make_unique<GroupMemberDao>(mysql_pool_.get());
    delivery_outbox_ = std::make_unique<DeliveryOutbox>(mysql_pool_.get());
    std::string schema_error;
    if (!delivery_outbox_->EnsureSchema(&schema_error)) {
        LOG_ERROR << "Job delivery outbox schema init failed: " << schema_error;
        return false;
    }
    redis_pool_ = std::make_unique<RedisConnectionPool>();
    RedisConfig redis_config;
    redis_config.host = cfg_.redis_host;
    redis_config.port = cfg_.redis_port;
    redis_config.password = cfg_.redis_password;
    redis_config.db = cfg_.redis_db;
    redis_config.pool_size = cfg_.redis_pool_size;
    redis_config.max_pool_size = cfg_.redis_max_pool_size;
    redis_config.connect_timeout_ms = cfg_.redis_connect_timeout_ms;
    redis_config.rw_timeout_ms = cfg_.redis_rw_timeout_ms;
    redis_config.idle_timeout_ms = cfg_.redis_idle_timeout_ms;
    redis_config.health_check_interval_ms = cfg_.redis_health_check_interval_ms;
    if (!redis_pool_->Init(redis_config)) {
        LOG_ERROR << "Job Redis routing pool init failed";
        return false;
    }
    // Route changes matter immediately during retry/reconnect.
    redis_store_ = std::make_unique<RedisStore>(redis_pool_.get(), 0);

    KafkaConsumer::Options consumer_opts;
    consumer_opts.enable_auto_commit = false;
    consumer_opts.max_processing_attempts = 3;

    split_scene_topics_ = cfg_.kafka_single_topic != cfg_.kafka_group_topic;
    if (!single_consumer_.Init(
            cfg_.kafka_brokers, cfg_.kafka_consumer_group + "_single",
            cfg_.kafka_single_topic,
            [this](const std::string& key, const std::string& value) {
                return split_scene_topics_ ? HandleSingleMessage(key, value)
                                           : HandleMessage(key, value);
            },
            consumer_opts)) {
        LOG_ERROR << "Kafka single consumer init failed";
        return false;
    }
    if (split_scene_topics_ &&
        !group_consumer_.Init(
            cfg_.kafka_brokers, cfg_.kafka_consumer_group + "_group",
            cfg_.kafka_group_topic,
            std::bind(&JobRunner::HandleGroupMessage, this,
                      std::placeholders::_1, std::placeholders::_2),
            consumer_opts)) {
        LOG_ERROR << "Kafka group consumer init failed";
        return false;
    }
    if (!broadcast_consumer_.Init(
            cfg_.kafka_brokers, cfg_.kafka_consumer_group + "_broadcast",
            cfg_.kafka_broadcast_topic,
            std::bind(&JobRunner::HandleBroadcastTask, this,
                      std::placeholders::_1, std::placeholders::_2),
            consumer_opts)) {
        LOG_ERROR << "Kafka broadcast consumer init failed";
        return false;
    }
    consumer_opts.dead_letter_topic = cfg_.kafka_persist_topic + ".dlq";
    consumer_opts.processing_cancelled = [this] { return !delivery_running_.load(); };
    // Independent session keys group-commit concurrently; each key remains
    // serial, and Kafka only commits a batch after every record is durable.
    consumer_opts.processing_workers = std::clamp(cfg_.delivery_workers, 1, 32);
    consumer_opts.max_batch_records = 64;
    consumer_opts.batch_window_ms = 5;
    if (!persist_consumer_.Init(
            cfg_.kafka_brokers, cfg_.kafka_consumer_group + "_persist",
            cfg_.kafka_persist_topic,
            std::bind(&JobRunner::HandlePersistMessage, this,
                      std::placeholders::_1, std::placeholders::_2),
            consumer_opts)) {
        LOG_ERROR << "Kafka persist consumer init failed";
        return false;
    }
    return true;
}

void JobRunner::Start() {
    delivery_ack_pool_.Start();
    if (cfg_.metrics_port > 0 && !metrics_server_.Start(cfg_.metrics_port)) {
        LOG_ERROR << "Job metrics HTTP server start failed on port "
                  << cfg_.metrics_port;
    }
    if (cfg_.use_push_stream) InitStreams();
    streams_running_ = cfg_.use_push_stream && !streams_.empty();
    delivery_running_ = true;
    const int delivery_workers = std::clamp(cfg_.delivery_workers, 1, 32);
    for (int worker = 0; worker < delivery_workers; ++worker) {
        delivery_threads_.emplace_back(&JobRunner::DeliveryLoop, this);
    }
    single_consumer_.Start();
    if (split_scene_topics_) group_consumer_.Start();
    broadcast_consumer_.Start();
    persist_consumer_.Start();
    LOG_INFO << "Job started, push_stream="
             << (streams_running_ ? "true" : "false");
}

void JobRunner::Stop() {
    // Wake an in-flight SQL persistence retry before joining its Kafka thread.
    delivery_running_ = false;
    delivery_wait_cv_.notify_all();
    // 先停止 Kafka 回调，避免停止流后仍有新请求入队。
    persist_consumer_.Stop();
    single_consumer_.Stop();
    if (split_scene_topics_) group_consumer_.Stop();
    broadcast_consumer_.Stop();

    for (auto& worker : delivery_threads_) {
        if (worker.joinable()) worker.join();
    }
    delivery_threads_.clear();

    streams_running_ = false;
    for (auto& item : streams_) {
        item.second->queue_cv.notify_all();
        if (item.second->context) item.second->context->TryCancel();
    }
    for (auto& item : streams_) {
        if (item.second->writer_thread.joinable()) {
            item.second->writer_thread.join();
        }
    }
    streams_.clear();
    delivery_ack_pool_.Stop();
    metrics_server_.Stop();
    if (mysql_pool_) mysql_pool_->Stop();
}

void JobRunner::ParseCometTargets() {
    comet_addrs_.clear();
    std::stringstream stream(cfg_.comet_targets);
    std::string item;
    while (std::getline(stream, item, ',')) {
        const auto pos = item.find('=');
        if (pos == std::string::npos) continue;
        const std::string id = item.substr(0, pos);
        const std::string address = item.substr(pos + 1);
        if (!id.empty() && !address.empty()) {
            comet_addrs_[id] = address;
            LOG_INFO << "Configured comet target id=" << id
                     << ", addr=" << address;
        }
    }
}

CometService::Stub* JobRunner::GetStub(const std::string& comet_id) {
    std::lock_guard<std::mutex> lock(stub_mu_);
    auto it = comet_stubs_.find(comet_id);
    if (it != comet_stubs_.end()) return it->second.get();
    auto address_it = comet_addrs_.find(comet_id);
    if (address_it == comet_addrs_.end()) return nullptr;
    auto channel = CreateKeepaliveChannel(address_it->second);
    auto stub = CometService::NewStub(channel);
    auto* result = stub.get();
    comet_stubs_[comet_id] = std::move(stub);
    return result;
}

void JobRunner::InitStreams() {
    for (const auto& item : comet_addrs_) {
        auto state = std::make_unique<StreamState>();
        state->channel = CreateKeepaliveChannel(item.second);
        state->stub = CometService::NewStub(state->channel);
        state->context = std::make_unique<grpc::ClientContext>();
        state->stream = state->stub->PushStream(state->context.get());
        if (!state->stream) {
            LOG_ERROR << "Failed to create Comet push stream id=" << item.first;
            continue;
        }
        streams_[item.first] = std::move(state);
    }
    streams_running_ = !streams_.empty();
    for (const auto& item : streams_) {
        item.second->reader_thread =
            std::thread(&JobRunner::StreamReaderLoop, this, item.first);
        item.second->writer_thread =
            std::thread(&JobRunner::StreamWriterLoop, this, item.first);
    }
}

bool JobRunner::ReconnectStream(const std::string& comet_id,
                                StreamState* state) {
    if (!state || !streams_running_) return false;
    auto address_it = comet_addrs_.find(comet_id);
    if (address_it == comet_addrs_.end()) return false;

    if (state->context) state->context->TryCancel();
    if (state->stream) {
        state->stream->WritesDone();
    }
    if (state->reader_thread.joinable() &&
        state->reader_thread.get_id() != std::this_thread::get_id()) {
        state->reader_thread.join();
    }
    if (state->stream) {
        state->stream->Finish();
        state->stream.reset();
    }

    // 旧流上尚未收到 reply 的请求必须回到 Kafka 重试路径，不能提前确认位点。
    std::vector<std::shared_ptr<PromiseState>> failed;
    {
        std::lock_guard<std::mutex> lock(state->pending_mutex);
        for (auto& item : state->pending) {
            failed.push_back(item.second.completion);
        }
        state->pending.clear();
    }
    for (const auto& completion : failed) {
        {
            std::lock_guard<std::mutex> lock(completion->mutex);
            if (!completion->done) {
                completion->done = true;
                completion->ok = false;
            }
        }
        completion->cv.notify_all();
    }

    state->context = std::make_unique<grpc::ClientContext>();
    state->channel = CreateKeepaliveChannel(address_it->second);
    state->stub = CometService::NewStub(state->channel);
    state->stream = state->stub->PushStream(state->context.get());
    if (!state->stream) return false;
    state->stream_broken = false;
    state->reader_thread =
        std::thread(&JobRunner::StreamReaderLoop, this, comet_id);
    MetricsRegistry::Instance().Increment("spark_push_reconnect_total");
    return true;
}

void JobRunner::SendDeliveryAck(const PushToCometRequest& request) {
    if (request.ack_comet_id().empty() || request.ack_user_id() <= 0) return;
    CometService::Stub* stub = GetStub(request.ack_comet_id());
    if (!stub) {
        MetricsRegistry::Instance().Increment(
            "spark_push_delivery_ack_failed_total");
        return;
    }
    PushDeliveryAckRequest ack;
    ack.set_user_id(request.ack_user_id());
    *ack.mutable_message() = request.message();
    SimpleReply reply;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(cfg_.push_rpc_deadline_ms));
    const auto status = stub->PushDeliveryAck(&context, ack, &reply);
    if (!status.ok() || reply.error().code() != 0) {
        MetricsRegistry::Instance().Increment(
            "spark_push_delivery_ack_failed_total");
        return;
    }
    MetricsRegistry::Instance().Increment("spark_push_delivery_ack_total");
}

void JobRunner::StreamReaderLoop(const std::string& comet_id) {
    auto it = streams_.find(comet_id);
    if (it == streams_.end()) return;
    StreamState* state = it->second.get();
    if (!state || !state->stream) return;

    // 只有这个线程读取 stream，按 request_id 将 Comet reply 分发给等待者。
    auto* stream = state->stream.get();
    PushToCometReply reply;
    while (streams_running_ && stream->Read(&reply)) {
        PendingRequest pending;
        bool found = false;
        {
            std::lock_guard<std::mutex> lock(state->pending_mutex);
            auto pending_it = state->pending.find(reply.request_id());
            if (pending_it != state->pending.end()) {
                pending = std::move(pending_it->second);
                state->pending.erase(pending_it);
                found = true;
            }
        }
        if (!found) {
            MetricsRegistry::Instance().Increment(
                "spark_push_stream_unmatched_reply_total");
            continue;
        }

        const bool ok = reply.error().code() == 0;
        if (ok && reply.delivered_count() > 0) {
            // Job 已经拿到 Comet 的处理结果，才允许向来源 Comet 回传
            // delivered_ack；Write() 成功本身不代表服务端已处理。
            delivery_ack_pool_.Submit(
                [this, request = pending.request]() { SendDeliveryAck(request); });
        }
        {
            std::lock_guard<std::mutex> lock(pending.completion->mutex);
            if (!pending.completion->done) {
                pending.completion->response = reply;
                pending.completion->done = true;
                pending.completion->ok = ok;
            }
        }
        pending.completion->cv.notify_all();
    }

    if (streams_running_) {
        state->stream_broken = true;
        MetricsRegistry::Instance().Increment(
            "spark_push_stream_read_failed_total");
        state->queue_cv.notify_one();
    }

    std::vector<std::shared_ptr<PromiseState>> failed;
    {
        std::lock_guard<std::mutex> lock(state->pending_mutex);
        for (auto& item : state->pending) {
            failed.push_back(item.second.completion);
        }
        state->pending.clear();
    }
    for (const auto& completion : failed) {
        {
            std::lock_guard<std::mutex> lock(completion->mutex);
            if (!completion->done) {
                completion->done = true;
                completion->ok = false;
            }
        }
        completion->cv.notify_all();
    }
}

void JobRunner::StreamWriterLoop(const std::string& comet_id) {
    auto it = streams_.find(comet_id);
    if (it == streams_.end()) return;
    StreamState* state = it->second.get();
    const int max_backoff = std::max(cfg_.push_stream_reconnect_base_ms,
                                     cfg_.push_stream_reconnect_max_ms);

    int idle_backoff = std::max(10, cfg_.push_stream_reconnect_base_ms);
    while (streams_running_) {
        PendingRequest pending;
        bool have_req = false;
        {
            std::unique_lock<std::mutex> lock(state->queue_mutex);
            const int wait_ms =
                (state->stream_broken || !state->stream ||
                 idle_backoff > cfg_.push_stream_reconnect_base_ms)
                    ? idle_backoff
                    : 200;
            state->queue_cv.wait_for(lock, std::chrono::milliseconds(wait_ms),
                                     [&] {
                                         return !state->queue.empty() ||
                                                !streams_running_;
                                     });
            if (!streams_running_ && state->queue.empty()) break;
            if (!state->queue.empty()) {
                pending = std::move(state->queue.front());
                state->queue.pop();
                have_req = true;
            }
        }
        if (!have_req) {
            if (streams_running_ &&
                (state->stream_broken || !state->stream)) {
                ReconnectStream(comet_id, state);
                idle_backoff = std::min(max_backoff, idle_backoff * 2);
            } else {
                idle_backoff = std::max(10, cfg_.push_stream_reconnect_base_ms);
            }
            continue;
        }
        idle_backoff = std::max(10, cfg_.push_stream_reconnect_base_ms);

        bool sent = false;
        bool completed_ok = false;
        int backoff = std::max(10, cfg_.push_stream_reconnect_base_ms);
        for (int attempt = 0; attempt < 4 && streams_running_; ++attempt) {
            if (state->stream_broken || !state->stream) {
                if (!ReconnectStream(comet_id, state)) {
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(backoff));
                    backoff = std::min(max_backoff, backoff * 2);
                    continue;
                }
            }

            {
                std::scoped_lock lock(state->pending_mutex,
                                      pending.completion->mutex);
                if (pending.completion->done) {
                    completed_ok = pending.completion->ok;
                    break;
                }
                state->pending[pending.request.request_id()] = pending;
            }

            if (state->stream && state->stream->Write(pending.request)) {
                sent = true;
                break;
            }
            MetricsRegistry::Instance().Increment(
                "spark_push_stream_write_failed_total");

            // 当前 request 由外层 Kafka consumer 重试；旧流上的其他请求由
            // ReconnectStream 一并失败，避免把没有 reply 的消息提交掉。
            {
                std::lock_guard<std::mutex> lock(state->pending_mutex);
                auto pending_it = state->pending.find(
                    pending.request.request_id());
                if (pending_it != state->pending.end() &&
                    pending_it->second.completion == pending.completion) {
                    state->pending.erase(pending_it);
                }
            }
            {
                std::lock_guard<std::mutex> lock(pending.completion->mutex);
                if (pending.completion->done) {
                    completed_ok = pending.completion->ok;
                    break;
                }
            }
            if (!streams_running_) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
            if (!ReconnectStream(comet_id, state)) {
                backoff = std::min(max_backoff, backoff * 2);
            }
        }

        if (sent) {
            MetricsRegistry::Instance().Increment(
                "spark_push_stream_write_success_total");
        } else if (!completed_ok) {
            MetricsRegistry::Instance().Increment(
                "spark_push_stream_request_failed_total");
            {
                std::lock_guard<std::mutex> lock(pending.completion->mutex);
                if (!pending.completion->done) {
                    pending.completion->done = true;
                    pending.completion->ok = false;
                }
            }
            pending.completion->cv.notify_all();
        }
    }

    if (state->context) state->context->TryCancel();
    if (state->stream) {
        state->stream->WritesDone();
    }
    if (state->reader_thread.joinable() &&
        state->reader_thread.get_id() != std::this_thread::get_id()) {
        state->reader_thread.join();
    }
    if (state->stream) {
        state->stream->Finish();
        state->stream.reset();
    }

    std::vector<std::shared_ptr<PromiseState>> failed;
    {
        std::lock_guard<std::mutex> lock(state->pending_mutex);
        for (auto& item : state->pending) {
            failed.push_back(item.second.completion);
        }
        state->pending.clear();
    }
    for (const auto& completion : failed) {
        {
            std::lock_guard<std::mutex> lock(completion->mutex);
            if (!completion->done) {
                completion->done = true;
                completion->ok = false;
            }
        }
        completion->cv.notify_all();
    }
}

bool JobRunner::SendToStream(const PushToCometRequest& request,
                             PushToCometReply* reply) {
    auto it = streams_.find(request.comet_id());
    if (it == streams_.end() || !it->second || request.request_id().empty()) {
        return false;
    }
    StreamState* state = it->second.get();
    auto completion = std::make_shared<PromiseState>();
    {
        std::lock_guard<std::mutex> lock(state->queue_mutex);
        if (static_cast<int>(state->queue.size()) >= cfg_.push_stream_queue_max) {
            MetricsRegistry::Instance().Increment(
                "spark_push_stream_queue_rejected_total");
            return false;
        }
        state->queue.push({request, completion});
    }
    state->queue_cv.notify_one();
    std::unique_lock<std::mutex> lock(completion->mutex);
    const bool completed = completion->cv.wait_for(
        lock, std::chrono::milliseconds(std::max(1, cfg_.push_rpc_deadline_ms)),
        [&] { return completion->done; });
    if (!completed) {
        lock.unlock();
        {
            std::lock_guard<std::mutex> pending_lock(state->pending_mutex);
            auto pending_it = state->pending.find(request.request_id());
            if (pending_it != state->pending.end() &&
                pending_it->second.completion == completion) {
                state->pending.erase(pending_it);
            }
        }
        {
            std::lock_guard<std::mutex> completion_lock(completion->mutex);
            if (!completion->done) {
                completion->done = true;
                completion->ok = false;
            }
        }
        completion->cv.notify_all();
        MetricsRegistry::Instance().Increment(
            "spark_push_stream_reply_timeout_total");
        return false;
    }
    if (reply) *reply = completion->response;
    return completion->ok;
}

bool JobRunner::HandleMessage(const std::string& key,
                              const std::string& value) {
    return HandleMessageForScene(key, value, "");
}

bool JobRunner::HandleSingleMessage(const std::string& key,
                                    const std::string& value) {
    return HandleMessageForScene(key, value, "single");
}

bool JobRunner::HandleGroupMessage(const std::string& key,
                                   const std::string& value) {
    return HandleMessageForScene(key, value, "group");
}

bool JobRunner::HandleMessageForScene(const std::string& key,
                                      const std::string& value,
                                      const std::string& scene_hint) {
    (void)key;
    PushToCometRequest request;
    if (!request.ParseFromString(value)) {
        LOG_ERROR << "[DLQ][Push] invalid PushToCometRequest";
        return true;
    }
    if (request.request_id().empty()) {
        request.set_request_id(request.message().msg_id() + "@" +
                               request.comet_id());
    }
    if (request.scene().empty()) {
        if (!scene_hint.empty()) {
            request.set_scene(scene_hint);
        } else if (request.message().session_id().rfind("r_", 0) == 0) {
            request.set_scene("group");
        } else {
            request.set_scene("single");
        }
    }
    return ProcessPushRequest(request);
}

bool JobRunner::ProcessPushRequest(const PushToCometRequest& request) {
    if (request.comet_id().empty()) return false;
    const std::string scene = request.scene().empty()
                                  ? (request.message().session_id().rfind("r_", 0) == 0
                                         ? "group"
                                         : "single")
                                  : request.scene();
    MetricsRegistry::Instance().Increment("spark_push_delivery_attempt_total");
    MetricsRegistry::Instance().Increment(
        "spark_push_delivery_attempt_total_" + scene);
    const int64_t now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
    if (request.message().timestamp_ms() > 0) {
        const int64_t age = std::max<int64_t>(
            0, now_ms - request.message().timestamp_ms());
        MetricsRegistry::Instance().Observe("spark_push_delivery_latency_ms",
                                            age);
        MetricsRegistry::Instance().Set("spark_push_kafka_lag_ms", age);
    }

    if (streams_running_ && cfg_.use_push_stream) {
        PushToCometReply reply;
        const bool ok = SendToStream(request, &reply);
        if (ok) {
            MetricsRegistry::Instance().Increment(
                "spark_push_delivery_success_total");
            MetricsRegistry::Instance().Increment(
                "spark_push_delivery_success_total_" + scene);
            return true;
        }
        // 长连接回复超时或目标 stream 暂不可用时，使用带 request_id 去重
        // 的 Unary RPC 兜底；只有两条路径都失败才记为最终丢失。
        MetricsRegistry::Instance().Increment(
            "spark_push_stream_reply_failed_total");
    }

    CometService::Stub* stub = GetStub(request.comet_id());
    if (!stub) return false;
    for (int attempt = 0; attempt < 3; ++attempt) {
        PushToCometReply reply;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() +
                             std::chrono::milliseconds(cfg_.push_rpc_deadline_ms));
        const auto status = stub->PushToComet(&context, request, &reply);
        if (status.ok() && reply.error().code() == 0) {
            MetricsRegistry::Instance().Increment(
                "spark_push_delivery_success_total");
            MetricsRegistry::Instance().Increment(
                "spark_push_delivery_success_total_" + scene);
            if (reply.delivered_count() > 0) {
                delivery_ack_pool_.Submit(
                    [this, request]() { SendDeliveryAck(request); });
            }
            return true;
        }
        if (attempt < 2) std::this_thread::sleep_for(
            std::chrono::milliseconds(100 * (attempt + 1)));
    }
    MetricsRegistry::Instance().Increment("spark_push_delivery_lost_total");
    MetricsRegistry::Instance().Increment(
        "spark_push_delivery_lost_total_" + scene);
    return false;
}

bool JobRunner::HandleBroadcastTask(const std::string& key,
                                    const std::string& value) {
    (void)key;
    BroadcastTaskRequest task;
    if (!task.ParseFromString(value)) {
        LOG_ERROR << "[DLQ][Broadcast] invalid BroadcastTaskRequest";
        return true;
    }
    ChatMessage message;
    message.set_msg_id(task.task_id());
    message.set_session_id("broadcast");
    message.set_timestamp_ms(std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count());
    message.set_msg_type("broadcast");
    message.set_content_json(task.content_json());

    bool ok = true;
    for (const auto& item : comet_addrs_) {
        PushToCometRequest request;
        request.set_comet_id(item.first);
        request.set_request_id(task.task_id() + "@" + item.first);
        request.set_scene("broadcast");
        *request.mutable_message() = message;
        ok = ProcessPushRequest(request) && ok;
    }
    return ok;
}

bool JobRunner::PersistMessage(const PersistMessageRequest& request, std::string* error) {
    if (!session_dao_ || !message_dao_) return false;
    const auto& pb = request.message();
    Message message;
    message.msg_id = pb.msg_id();
    message.session_id = pb.session_id();
    message.msg_seq = pb.msg_seq();
    message.sender_id = pb.sender_id();
    message.timestamp_ms = pb.timestamp_ms();
    message.msg_type = pb.msg_type();
    message.content_json = pb.content_json();
    message.client_msg_id = pb.client_msg_id();

    for (int attempt = 0; attempt < 4; ++attempt) {
        std::string err;
        Session session;
        bool session_ok = false;
        if (request.scene() == "single") {
            session_ok = session_dao_->GetOrCreateSingleSession(
                request.user1_id(), request.user2_id(), &session, &err);
        } else {
            session_ok = session_dao_->GetOrCreateRoomSession(
                request.room_id(), &session, &err);
        }
        if (session_ok && message_dao_->InsertMessage(message, &err) &&
            session_dao_->UpdateLastMessageSeqAtLeast(message.session_id,
                                                       message.msg_seq,
                                                       &err)) {
            return true;
        }
        if (error) *error = err;
        if (err.rfind("message sequence collision", 0) == 0) return false;
        if (attempt < 3) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(100 * (attempt + 1)));
        } else {
            LOG_ERROR << "Persistence retries exhausted msg_id=" << message.msg_id
                      << " error=" << err;
        }
    }
    return false;
}

bool JobRunner::HandlePersistMessage(const std::string& key,
                                     const std::string& value) {
    (void)key;
    PersistMessageRequest request;
    if (!request.ParseFromString(value)) {
        LOG_ERROR << "Invalid PersistMessageRequest; retain in durable DLQ";
        return false;
    }
    const auto& message = request.message();
    if (message.sender_id() <= 0 || message.msg_seq() <= 0 || message.session_id().empty() ||
        message.session_id().size() > 128 || message.msg_id() != message.session_id() + "-" + std::to_string(message.msg_seq()) ||
        (request.scene() != "single" && request.scene() != "chatroom") ||
        (request.scene() == "single" && (request.user1_id() <= 0 || request.user2_id() <= 0 ||
         message.session_id() != "s_" + std::to_string(std::min(request.user1_id(), request.user2_id())) +
                                 "_" + std::to_string(std::max(request.user1_id(), request.user2_id())))) ||
        (request.scene() == "chatroom" && (request.room_id() <= 0 ||
         message.session_id() != "r_" + std::to_string(request.room_id())))) {
        LOG_ERROR << "Invalid persistence event identity; retain in durable DLQ";
        return false;
    }
    std::string error;
    // Session, history and outbox share one COMMIT; a crash cannot expose a
    // durable message without its delivery task. ClaimReady retains its gate.
    int retry = 0;
    while (delivery_running_) {
        if (delivery_outbox_ && delivery_outbox_->PersistAtomically(request, &error)) {
            MetricsRegistry::Instance().Increment("spark_push_persist_success_total");
            delivery_wait_cv_.notify_all();
            return true;
        }
        MetricsRegistry::Instance().Increment("spark_push_persist_failed_total");
        if (error.rfind("invalid delivery persistence event", 0) == 0 ||
            error.rfind("message sequence collision", 0) == 0) {
            // Poison records remain visible in the existing durable DLQ.
            return false;
        }
        MetricsRegistry::Instance().Increment("spark_push_persist_retry_total");
        const int delay = std::min(3000, 100 * (1 << std::min(5, retry++)));
        std::unique_lock<std::mutex> lock(delivery_wait_mutex_);
        delivery_wait_cv_.wait_for(lock, std::chrono::milliseconds(delay),
                                  [this] { return !delivery_running_; });
    }
    // KafkaConsumer must not commit or DLQ a callback cancelled by Stop.
    return false;
}

bool JobRunner::DeliverTask(const DeliveryOutbox::Task& task, int lease_ms) {
    if (!redis_store_ || !delivery_outbox_) return false;
    const auto& event = task.request;
    std::vector<int64_t> recipients(event.recipient_user_ids().begin(),
                                    event.recipient_user_ids().end());
    std::string error;
    auto lease_refreshed_at = std::chrono::steady_clock::now();
    const auto renew_if_due = [&] {
        if (!delivery_running_) return false;
        const auto now = std::chrono::steady_clock::now();
        if (now - lease_refreshed_at < std::chrono::milliseconds(lease_ms / 3)) return true;
        if (!delivery_outbox_->Renew(task, lease_ms, &error)) return false;
        lease_refreshed_at = std::chrono::steady_clock::now();
        return true;
    };
    if (recipients.empty()) {
        if (event.scene() == "single") {
            // Old Kafka events did not carry explicit fanout recipients.
            recipients = {event.user1_id(), event.user2_id()};
        } else if (!group_member_dao_ ||
                   !group_member_dao_->ListRoomMembers(event.room_id(), &recipients, &error)) {
            return false;
        }
    }
    std::sort(recipients.begin(), recipients.end());
    recipients.erase(std::unique(recipients.begin(), recipients.end()), recipients.end());
    std::unordered_map<std::string, std::vector<int64_t>> routes;
    size_t checked_recipients = 0;
    for (int64_t user_id : recipients) {
        if (++checked_recipients % 64 == 0 &&
            !renew_if_due()) return false;
        if (user_id <= 0) continue;
        std::vector<std::string> comets;
        if (!redis_store_->GetUserRoutes(user_id, &comets)) return false;
        for (const auto& comet : comets) routes[comet].push_back(user_id);
    }
    for (const auto& route : routes) {
        std::vector<int64_t> receiver_users, sender_users;
        for (const auto user_id : route.second) {
            if (event.ack_user_id() > 0 && user_id == event.ack_user_id()) sender_users.push_back(user_id);
            else receiver_users.push_back(user_id);
        }
        auto push = [&](const std::vector<int64_t>& users, bool sender_sync) {
            if (users.empty()) return true;
            if (!renew_if_due()) return false;
            PushToCometRequest request;
            request.set_comet_id(route.first);
            request.set_request_id(event.message().msg_id() + "@" + route.first +
                                   (sender_sync ? "@sender_sync" : ""));
            // A sender's other devices must not count as receiver delivery.
            if (!sender_sync) {
                request.set_ack_comet_id(event.ack_comet_id());
                request.set_ack_user_id(event.ack_user_id());
            }
            request.set_scene(event.scene() == "single" ? "single" : "group");
            *request.mutable_message() = event.message();
            for (int64_t user_id : users) request.add_targets()->set_user_id(user_id);
            return ProcessPushRequest(request);
        };
        if (!push(receiver_users, false) || !push(sender_users, true)) return false;
    }
    // No live route means that device receipt/offline sync owns delivery.
    return true;
}

void JobRunner::DeliveryLoop() {
    // Stream wait plus three unary attempts, backoffs and SQL renewal budget.
    const int lease_ms = std::max(60000, cfg_.push_rpc_deadline_ms * 8 + 15000);
    auto next_backlog = std::chrono::steady_clock::now();
    while (delivery_running_) {
        DeliveryOutbox::Task task;
        bool found = false;
        std::string error;
        if (!delivery_outbox_->ClaimReady(lease_ms, &task, &found, &error)) {
            MetricsRegistry::Instance().Increment("spark_push_delivery_outbox_claim_errors_total");
        } else if (found) {
            MetricsRegistry::Instance().Increment("spark_push_delivery_outbox_attempts_total");
            const bool delivered = DeliverTask(task, lease_ms);
            if (delivered && delivery_outbox_->Complete(task, &error)) {
                MetricsRegistry::Instance().Increment("spark_push_delivery_outbox_completed_total");
            } else {
                // Never discard an online failure. The SQL task survives restart.
                const int delay_ms = std::min(30000, 100 * (1 << std::min(8, task.attempts - 1)));
                delivery_outbox_->Retry(task, delay_ms, &error);
                MetricsRegistry::Instance().Increment("spark_push_delivery_outbox_retries_total");
            }
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= next_backlog) {
            int64_t count = 0;
            if (delivery_outbox_->Backlog(&count, &error)) {
                MetricsRegistry::Instance().Set("spark_push_delivery_outbox_backlog", count);
            }
            next_backlog = now + std::chrono::seconds(2);
        }
        if (!found) {
            std::unique_lock<std::mutex> lock(delivery_wait_mutex_);
            delivery_wait_cv_.wait_for(lock, std::chrono::milliseconds(100));
        }
    }
}

}  // namespace sparkpush
