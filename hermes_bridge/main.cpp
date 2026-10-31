#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <csignal>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "config.h"
#include "native_control.h"
#include "hermes_client.h"
#include "grpc_inference_client.h"
#include "kafka_consumer.h"
#include "kafka_producer.h"
#include "logging.h"
#include "metrics.h"
#include "agent_event.h"
#include "text_metrics.h"
#include "image_attachment.h"

namespace {

std::atomic<bool> g_running{true};

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

uint64_t Fnv1a64(const std::string& value, uint64_t seed) {
    uint64_t hash = seed;
    for (const unsigned char byte : value) {
        hash ^= byte;
        hash *= 1099511628211ULL;
    }
    return hash;
}

// 事件 id 由 request_id 和序号确定性地算出来，不读随机源。
// Kafka 重放同一条请求时，下游看到的 event_id 必须相同，
// 否则客户端会把同一段增量当成两条。
std::string ProjectedEventId(const std::string& request_id, int sequence) {
    const std::string material = request_id + std::string(1, '\0') +
                                 std::to_string(sequence);
    const auto left = Fnv1a64(material, 1469598103934665603ULL);
    const auto right = Fnv1a64(material, 1099511628211ULL);
    char value[37]{};
    std::snprintf(value, sizeof(value), "evt_%016llx%016llx",
                  static_cast<unsigned long long>(left),
                  static_cast<unsigned long long>(right));
    return value;
}

void HandleSignal(int) { g_running = false; }

enum class ArgResult { kOk, kHelp, kError };

ArgResult ParseArgs(int argc, char** argv, std::string* config_path) {
    *config_path = "conf/hermes_bridge.conf";
    bool positional = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") return ArgResult::kHelp;
        if (arg == "-c" || arg == "--config") {
            if (++i >= argc) return ArgResult::kError;
            *config_path = argv[i];
        } else if (!arg.empty() && arg[0] == '-') {
            return ArgResult::kError;
        } else if (!positional) {
            *config_path = arg;
            positional = true;
        } else {
            return ArgResult::kError;
        }
    }
    return ArgResult::kOk;
}

void PrintUsage(const char* program) {
    std::fprintf(stderr,
                 "用法：%s [--config <配置文件>]\n"
                 "默认配置：conf/hermes_bridge.conf\n",
                 program);
}

}  // namespace

namespace sparkpush {

class HermesBridgeRunner {
   public:
    explicit HermesBridgeRunner(const HermesBridgeConfig& config)
        : config_(config), client_(config) {
        if (config_.inference_backend == "grpc") {
            grpc_client_ = std::make_unique<GrpcInferenceClient>(config_, &g_running);
            model_client_ = grpc_client_.get();
        } else {
            model_client_ = &client_;
        }
    }

    bool Init() {
        if (!reply_producer_.Init(config_.kafka_brokers,
                                  config_.reply_topic)) {
            LOG_ERROR << "Hermes bridge reply producer init failed";
            return false;
        }
        if (!delta_producer_.Init(config_.kafka_brokers,
                                  config_.delta_topic)) {
            LOG_ERROR << "Hermes bridge delta producer init failed";
            return false;
        }
        KafkaConsumer::Options options;
        options.enable_auto_commit = false;
        options.auto_offset_reset = "earliest";
        options.max_processing_attempts = 3;
        options.dead_letter_topic = config_.request_topic + ".dlq";
        // Poll resumes after a bounded durable batch. A worst-case batch may
        // contain only one session, so budget for serial processing of every
        // record, including business retries and DLQ confirmation.
        // 译：一次 poll 要能撑完一整批的处理时间。最坏情况下一批全是同一会话，
        // 只能串行，所以按「每条记录的超时 × 重试」来估，把业务重试和 DLQ 确认也算进去。
        // 估少了，Kafka 会认为消费者卡住并把分区收走，正在跑的请求会被另一个实例再跑一次。
        const int64_t poll_budget = 3LL * (config_.request_timeout_ms +
            static_cast<int64_t>(config_.reply_delivery_timeout_ms)) + 65000;
        const auto batch_budget = poll_budget *
            (config_.processing_workers > 1 ? config_.max_batch_records : 1);
        if (batch_budget > std::numeric_limits<int>::max()) return false;
        options.max_poll_interval_ms = static_cast<int>(std::max<int64_t>(300000, batch_budget));
        options.processing_workers = config_.processing_workers;
        options.max_batch_records = config_.max_batch_records;
        if (!config_.native_control_bot_users.empty()) {
            options.interrupt_predicate = [this](const std::string&, const std::string& value) {
                return IsNativeInterrupt(value, config_.native_control_bot_users);
            };
        }
        options.failure_recovery = [this](const std::string& key, const std::string& value) {
            const auto request = nlohmann::json::parse(value, nullptr, false);
            if (!request.is_object()) return KafkaConsumer::RecoveryRecord{};
            if (request.contains("request_id") && !request["request_id"].is_string())
                return KafkaConsumer::RecoveryRecord{};
            const auto id = request.value("request_id", key);
            std::lock_guard<std::mutex> lock(cache_mutex_);
            const auto it = reply_cache_.find(id);
            if (it == reply_cache_.end()) return KafkaConsumer::RecoveryRecord{};
            // Retain an already generated answer for recovery to ai_reply;
            // replaying ai_request could execute Agent tools a second time.
            // 译：已经生成的回答留在缓存里，恢复时直接重发到 ai_reply。
            // 再消费一次 ai_request 会把 Agent 工具再执行一遍（发消息、改文件）。
            // 缓存没命中就返回空，交给消费者走正常重试或 DLQ。
            return KafkaConsumer::RecoveryRecord{config_.reply_topic, id, it->second};
        };
        if (!request_consumer_.Init(
                config_.kafka_brokers, config_.consumer_group,
                config_.request_topic,
                [this](const std::string& key, const std::string& value) {
                    return HandleRequest(key, value);
                },
                options)) {
            LOG_ERROR << "Hermes bridge request consumer init failed";
            return false;
        }
        return true;
    }

    void Start() { request_consumer_.Start(); }

    void Stop() { request_consumer_.Stop(); }
    bool failed() const { return request_consumer_.failed(); }

   private:
    // local_error 非空时原样返回（用法提示）。否则这是一条本 Bridge
    // 不认识的本地命令，给一句固定的「暂不支持」，不调用模型。
    std::string BuildLocalCommandReply(const nlohmann::json& command) const {
        const std::string local_error = command.value("local_error", "");
        if (!local_error.empty()) return local_error;

        const std::string input_name = command.value("input_name", "");
        return "暂不支持 Kernel Agent 命令 /" + input_name +
               "。发送 /help 查看当前可用命令。";
    }

    bool PublishReply(const std::string& key, const nlohmann::json& reply) {
        return reply_producer_.SendAndWait(
            key, reply.dump(), config_.reply_delivery_timeout_ms);
    }

    bool PublishDelta(const std::string& key, const nlohmann::json& delta) {
        // 增量是实时体验数据，最终完整回答仍由 ai_reply 负责可靠落库；
        // 因此这里不逐条等待 Kafka delivery，避免模型输出被消息确认反压。
        return delta_producer_.Send(key, delta.dump());
    }

    bool HandleRequest(const std::string& key, const std::string& value) {
        nlohmann::json request;
        try {
            request = nlohmann::json::parse(value);
        } catch (const std::exception&) {
            LOG_ERROR << "Hermes bridge invalid ai_request JSON";
            return false;
        }
        if (!request.is_object()) return false;
        const std::string request_id = request.value("request_id", key);
        if (request_id.empty()) {
            LOG_ERROR << "Hermes bridge request_id is empty";
            return false;
        }

        nlohmann::json reply;
        {
            std::lock_guard<std::mutex> lock(cache_mutex_);
            auto it = reply_cache_.find(request_id);
            if (it != reply_cache_.end()) {
                try {
                    reply = nlohmann::json::parse(it->second);
                } catch (...) {
                    reply_cache_.erase(it);
                }
            }
        }

        if (reply.is_null() || reply.empty()) {
            reply["request_id"] = request_id;
            reply["session_id"] = request.value("session_id", "");
            reply["user_id"] = request.value("user_id", 0LL);
            reply["bot_user_id"] = request.value("bot_user_id", 0LL);
            if (request.contains("collaboration")) reply["collaboration"] = request["collaboration"];
            reply["client_msg_id"] = request.value("client_msg_id", "");
            const auto messages =
                request.value("messages", nlohmann::json::array());
            auto command =
                request.value("command", nlohmann::json::object());
            if (!command.is_object()) command = nlohmann::json::object();
            const bool call_model = request.value("call_model", true);
            const std::string command_name = command.value("name", "");
            HermesChatOptions chat_options;
            chat_options.request_id = request_id;
            chat_options.collaboration = request.value("collaboration", nlohmann::json());
            chat_options.retry = call_model && command_name == "retry";
            chat_options.session_id = request.value("session_id", "");
            chat_options.context_start_seq =
                request.value("context_start_seq", 0LL);
            if (request.value("model_override", false)) {
                chat_options.model = request.value("model", "");
                chat_options.provider = request.value("provider", "");
            }
            if (request.contains("images") && request["images"].is_array() &&
                !request["images"].empty()) {
                std::string image_error;
                if (!sparkpush::LoadPiImagesFromDir(
                        config_.attachment_dir, request["images"],
                        &chat_options.images, &image_error)) {
                    LOG_ERROR << "Hermes bridge failed to load images: "
                              << image_error;
                    return false;
                }
            }
            const std::string effective_model =
                chat_options.model.empty() ?
                    (config_.inference_backend == "grpc" ? config_.inference_model : config_.hermes_model)
                                           : chat_options.model;
            const std::string effective_provider = chat_options.provider;
            reply["model_override"] = request.value("model_override", false);
            reply["model_state_confirmed"] = false;
            reply["effective_model"] = effective_model;
            reply["effective_provider"] = effective_provider;
            reply["command_handled_locally"] = !call_model;
            if (command.is_object() && command.contains("name")) {
                reply["command"] = command.value("name", "");
            }
            const int64_t request_started_at_ms = NowMs();
            size_t prompt_bytes = 0;
            size_t prompt_unicode_chars = 0;
            if (messages.is_array()) {
                for (const auto& item : messages) {
                    if (item.is_object() && item.contains("content") &&
                        item.at("content").is_string()) {
                        const auto metrics =
                            MeasureText(item.at("content").get<std::string>());
                        prompt_bytes += metrics.utf8_bytes;
                        prompt_unicode_chars += metrics.unicode_chars;
                    }
                }
            }
            const bool streamed_request = call_model && config_.streaming;
            if (LengthAuditEnabled()) {
                LOG_INFO << "length_audit stage=bridge_request request_id="
                         << request_id << " provider="
                         << (effective_provider.empty() ? "adapter-state" : effective_provider)
                         << " model=" << effective_model
                         << " stream=" << (streamed_request ? "true" : "false")
                         << " max_tokens=unset max_output_tokens=unset"
                         << " prompt_bytes=" << prompt_bytes
                         << " prompt_chars=" << prompt_unicode_chars
                         << " prompt_message_count="
                         << (messages.is_array() ? messages.size() : 0);
            }
            HermesChatResult chat_result;
            std::string error;
            int delta_count = 0;
            size_t provider_delta_bytes = 0;
            size_t provider_delta_chars = 0;
            bool delta_publish_failed = false;
            int64_t first_delta_at_ms = 0;
            std::string delta_buffer;
            nlohmann::json latest_agent_envelope = nlohmann::json::object();
            chat_options.on_agent_event = [&](const nlohmann::json& event) {
                if (event.is_object() && event.contains("envelope") &&
                    event["envelope"].is_object()) {
                    latest_agent_envelope = event["envelope"];
                }
            };
            auto last_delta_flush = std::chrono::steady_clock::now();
            auto projected_event = [&](const char* kind, const std::string& text,
                                       int sequence) {
                nlohmann::json event = {{"schema", "sparkpush.agent_event.v1"},
                    {"type", kind}, {"data", {{"text", text}}}};
                if (!latest_agent_envelope.empty()) {
                    auto envelope = latest_agent_envelope;
                    // Bridge batching is a channel projection, so its sequence
                    // follows the emitted legacy delta order.
                    // 译：Bridge 的合并是通道上的投影，序号跟已发出的 delta 顺序走，
                    // 不跟模型原始 token 序号走。Logic 侧要求终态 sequence == delta_count。
                    envelope["sequence"] = sequence;
                    envelope["event_id"] = ProjectedEventId(request_id, sequence);
                    envelope["terminal"] = false;
                    event["envelope"] = std::move(envelope);
                }
                return event;
            };
            auto flush_delta = [&]() {
                if (delta_buffer.empty()) return;
                if (first_delta_at_ms == 0) {
                    first_delta_at_ms = NowMs();
                    MetricsRegistry::Instance().Observe(
                        "spark_push_hermes_ttft_ms",
                        first_delta_at_ms - request_started_at_ms);
                }
                nlohmann::json delta = {
                    {"request_id", request_id},
                    {"session_id", request.value("session_id", "")},
                    {"user_id", request.value("user_id", 0LL)},
                    {"bot_user_id", request.value("bot_user_id", 0LL)},
                    {"delta_index", delta_count},
                    {"delta", delta_buffer},
                    {"created_at_ms", NowMs()},
                };
                delta["agent_event"] =
                    projected_event("assistant_delta", delta_buffer, delta_count);
                if (request.contains("collaboration")) delta["collaboration"] = request["collaboration"];
                if (!PublishDelta(request_id, delta)) {
                    delta_publish_failed = true;
                    MetricsRegistry::Instance().Increment(
                        "spark_push_hermes_stream_delta_publish_errors_total");
                } else {
                    MetricsRegistry::Instance().Increment(
                        "spark_push_hermes_stream_deltas_total");
                }
                ++delta_count;
                delta_buffer.clear();
                last_delta_flush = std::chrono::steady_clock::now();
            };
            auto on_delta = [&](const std::string& value) {
                if (!call_model || !config_.streaming || value.empty()) return;
                const auto metrics = MeasureText(value);
                provider_delta_bytes += metrics.utf8_bytes;
                provider_delta_chars += metrics.unicode_chars;
                delta_buffer += value;
                const auto elapsed = std::chrono::duration_cast<
                    std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - last_delta_flush)
                                          .count();
                // 首个 token 立即显示；后续按字节数或时间合并，避免一个
                // token 产生一个 Kafka/Job/Comet 消息。
                // 48 字节或 50ms 合并一次。阈值改小会把 Kafka/Job/Comet 的消息数放大，
                // 改大则首屏之后的字来得更顿。第一个 token 不受这两个阈值限制。
                if (first_delta_at_ms == 0 || delta_buffer.size() >= 48 ||
                    elapsed >= 50) {
                    flush_delta();
                }
            };
            chat_options.on_progress = [&](const std::string& status) {
                if (!call_model || !config_.streaming || status.empty()) return;
                flush_delta();
                nlohmann::json progress = {{"request_id", request_id},
                    {"session_id", request.value("session_id", "")}, {"user_id", request.value("user_id", 0LL)},
                    {"bot_user_id", request.value("bot_user_id", 0LL)}, {"delta_index", delta_count++},
                    {"delta", status}, {"progress", true}, {"created_at_ms", NowMs()}};
                progress["agent_event"] = projected_event(
                    "assistant_progress", status,
                    progress["delta_index"].get<int>());
                if (request.contains("collaboration")) progress["collaboration"] = request["collaboration"];
                if (!PublishDelta(request_id, progress)) delta_publish_failed = true;
            };
            bool ok = true;
            // call_model 为假且没有 local_error 的已知命令走 /agent/control，
            // 不把命令文本送给模型。local_error 非空会落到下面的本地回复。
            // 创作类命令能进这一支，是因为 hermes_command.cpp 故意不填 local_error。
            if (!call_model && (command_name == "agent" || command_name == "model" || command_name == "reasoning" ||
                command_name == "new" || command_name == "retry" || command_name == "restart" || command_name == "status" ||
                command_name == "help" || command_name == "stop" || command_name == "approve" ||
                command_name == "deny" || command_name == "compact" ||
                command_name == "roleplay" || command_name == "write" ||
                command_name == "scene" || command_name == "character" || command_name == "world" ||
                command_name == "memory" || command_name == "remember" || command_name == "forget") &&
                command.value("local_error", "").empty()) {
                const auto argument = command.value("arguments", "");
                chat_options.control = {{"operation", command_name}};
                if (command_name == "agent") {
                    chat_options.control["operation"] = argument.empty() ? "list_agents" : "set_agent";
                    if (!argument.empty()) chat_options.control["target_agent"] = argument;
                } else if (command_name == "model") {
                    chat_options.control["operation"] = "list_models";
                    if (argument == "refresh") {
                        chat_options.control["refresh"] = true;
                    } else if (argument == "default" || argument == "reset") {
                        chat_options.control["operation"] = "reset_model";
                    } else if (!argument.empty() && argument.back() == ':') {
                        chat_options.control = {{"operation", "list_models"},
                            {"provider_filter", argument.substr(0, argument.size() - 1)}};
                    } else if (!argument.empty()) {
                        const auto colon = argument.find(':');
                        chat_options.control = {{"operation", "set_model"},
                            {"target_provider", argument.substr(0, colon)},
                            {"target_model", colon == std::string::npos ? "" : argument.substr(colon + 1)}};
                    }
                } else if (command_name == "reasoning") {
                    chat_options.control = {{"operation", argument.empty() ? "list_reasoning" : "set_reasoning"}};
                    if (!argument.empty()) chat_options.control["level"] = argument;
                } else if (command_name == "restart" && !argument.empty()) {
                    chat_options.control["operation"] = "restart_agent";
                } else if (command_name == "new" && !argument.empty()) {
                    chat_options.control["operation"] = "new_session";
                } else if (command_name == "roleplay" || command_name == "write" ||
                           command_name == "scene" || command_name == "character" ||
                           command_name == "world" || command_name == "memory" ||
                           command_name == "remember" || command_name == "forget" ||
                           command_name == "persona" || command_name == "branch" ||
                           command_name == "branches" || command_name == "canon" ||
                           command_name == "export") {
                    chat_options.control["operation"] = command_name;
                }
                if (!argument.empty()) chat_options.control["argument"] = argument;
                chat_options.control["command_seq"] = request.value("message_seq", 0LL);
                if (!chat_options.model.empty()) {
                    chat_options.control["model"] = chat_options.model;
                    chat_options.control["provider"] = chat_options.provider;
                }
                ok = client_.Chat(nlohmann::json::array(), chat_options, &chat_result, &error);
                if (!ok) error = "命令未成功，请发送 /" + command_name + " 刷新后重试（" + error + "）";
            } else if (!call_model) {
                chat_result.text = BuildLocalCommandReply(command);
            } else if (config_.streaming) {
                ok = model_client_->ChatStream(messages, chat_options, on_delta,
                                        &chat_result, &error);
            } else {
                ok = model_client_->Chat(messages, chat_options, &chat_result, &error);
            }
            if (ok && chat_result.metadata.contains("model_state")) {
                const auto& state = chat_result.metadata["model_state"];
                if (!state.is_object() || !state.contains("model") ||
                    !AgentModelValid(state["model"]) || !state.contains("override") ||
                    !state["override"].is_boolean()) {
                    ok = false;
                    error = "Agent 返回了无效的模型状态";
                } else {
                    reply["model_state_confirmed"] = true;
                    reply["model_override"] = state["override"];
                    reply["effective_model"] = state["model"]["id"];
                    reply["effective_provider"] = state["model"]["provider"];
                }
            }
            if (call_model && config_.streaming) flush_delta();
            if (ok && chat_result.agent_event.is_object() &&
                !chat_result.agent_event.empty()) {
                if (!chat_result.metadata.is_object()) {
                    chat_result.metadata = nlohmann::json::object();
                }
                auto final_event = chat_result.agent_event;
                if (final_event.contains("envelope") &&
                    final_event["envelope"].is_object()) {
                    // Intermediate events were coalesced into the Bridge's
                    // delta_index domain. Project the terminal event into the
                    // same sequence so the channel observes 0..N without a
                    // gap or collision.
                    // 译：中间事件已经被收进 Bridge 的 delta_index 空间。
                    // 终态事件也投影到同一个序号，通道上才能看到连续的 0..N，
                    // 不会和某一条 delta 撞号，也不会中间缺一号。
                    // PrepareHermesReply 会核对 sequence == delta_count，对不上整条拒。
                    final_event["envelope"]["sequence"] = delta_count;
                    final_event["envelope"]["event_id"] =
                        ProjectedEventId(request_id, delta_count);
                    final_event["envelope"]["terminal"] = true;
                }
                chat_result.metadata["agent_event"] = std::move(final_event);
            }
            reply["ok"] = ok;
            if (!ok && chat_result.terminal_status == "cancelled") reply["terminal_status"] = "cancelled";
            reply["streamed"] = call_model && config_.streaming;
            reply["delta_count"] = delta_count;
            if (LengthAuditEnabled()) {
                reply["provider_chunk_count"] =
                    static_cast<int64_t>(chat_result.stream_chunk_count);
                reply["provider_delta_bytes"] = provider_delta_bytes;
                reply["provider_delta_chars"] = provider_delta_chars;
            }
            reply["delta_publish_failed"] = delta_publish_failed;
            const int64_t completed_at_ms = NowMs();
            reply["request_started_at_ms"] = request_started_at_ms;
            reply["first_delta_at_ms"] = first_delta_at_ms;
            reply["completed_at_ms"] = completed_at_ms;
            reply["total_latency_ms"] = completed_at_ms - request_started_at_ms;
            // Keep the historical prompt_chars field for compatibility. The
            // explicit byte/code-point fields are debug-only extensions.
            // 译：prompt_chars 这个老字段里存的其实是字节数，为了兼容已有指标。
            // 真正的码点数列在 prompt_unicode_chars，而且只在长度审计打开时写入。
            reply["prompt_chars"] = prompt_bytes;
            if (LengthAuditEnabled()) {
                reply["prompt_bytes"] = prompt_bytes;
                reply["prompt_unicode_chars"] = prompt_unicode_chars;
            }
            reply["prompt_message_count"] = messages.is_array() ? messages.size() : 0;
            MetricsRegistry::Instance().Observe(
                "spark_push_hermes_total_latency_ms",
                completed_at_ms - request_started_at_ms);
            MetricsRegistry::Instance().Observe(
                "spark_push_hermes_prompt_chars",
                static_cast<int64_t>(prompt_bytes));
            if (!call_model) {
                MetricsRegistry::Instance().Increment(
                    "spark_push_hermes_commands_total_" +
                    command.value("name", "unknown"));
            }
            if (ok) {
                if (LengthAuditEnabled()) {
                    const auto response_metrics = MeasureText(chat_result.text);
                    reply["response_bytes"] = response_metrics.utf8_bytes;
                    reply["response_unicode_chars"] = response_metrics.unicode_chars;
                    reply["finish_reason"] = chat_result.finish_reason;
                    if (chat_result.metadata.is_object() &&
                        chat_result.metadata.contains("length_audit") &&
                        chat_result.metadata["length_audit"].is_object()) {
                        const auto& audit = chat_result.metadata["length_audit"];
                        for (const auto* key : {"provider", "public_provider", "model",
                                                 "max_tokens", "max_output_tokens",
                                                 "temperature", "stop", "timeout_s",
                                                 "provider_output_bytes",
                                                 "provider_output_chars", "finish_reason"}) {
                            if (audit.contains(key)) reply[std::string("audit_") + key] = audit[key];
                        }
                    }
                }
                reply["text"] = chat_result.text;
                if (chat_result.citations.is_array() &&
                    !chat_result.citations.empty()) {
                    reply["citations"] = chat_result.citations;
                }
                if (chat_result.metadata.is_object() &&
                    !chat_result.metadata.empty()) {
                    reply["response_metadata"] = chat_result.metadata;
                }
            } else {
                reply["error"] = error.empty() ? "Hermes request failed" : error;
                LOG_ERROR << "Hermes request failed request_id=" << request_id
                          << ": " << reply["error"].get<std::string>();
            }
            if (LengthAuditEnabled()) {
                LOG_INFO << "length_audit stage=bridge_final request_id="
                         << request_id << " provider="
                         << reply.value("effective_provider", "adapter-state")
                         << " model=" << reply.value("effective_model", "unknown")
                         << " stream=" << (reply.value("streamed", false) ? "true" : "false")
                         << " max_tokens=unset max_output_tokens=unset"
                         << " provider_bytes=" << reply.value("audit_provider_output_bytes", 0)
                         << " provider_chars=" << reply.value("audit_provider_output_chars", 0)
                         << " delta_bytes=" << reply.value("provider_delta_bytes", 0)
                         << " delta_chars=" << reply.value("provider_delta_chars", 0)
                         << " response_bytes=" << reply.value("response_bytes", 0)
                         << " response_chars=" << reply.value("response_unicode_chars", 0)
                         << " finish_reason=" << reply.value("finish_reason", "");
            }
            if (!ok && chat_result.native_unconfirmed) {
                // The Gateway will query the durable original Run on redelivery.
                // No error reply/cache may hide a later recovered success.
                return false;
            }
            const std::string serialized = reply.dump();
            {
                std::lock_guard<std::mutex> lock(cache_mutex_);
                // 满 10000 就整表清空，不是 LRU。清空之后，更早请求的失败恢复
                // 找不到答案，会重新执行 Agent 工具。这是进程内缓存，重启同样丢失。
                if (reply_cache_.size() >= 10000) reply_cache_.clear();
                reply_cache_[request_id] = serialized;
            }
        }

        if (!PublishReply(request_id, reply)) {
            LOG_ERROR << "Hermes bridge failed to publish ai_reply request_id="
                      << request_id;
            return false;
        }
        const auto metadata = reply.value("response_metadata", nlohmann::json::object());
        if (reply.value("ok", false) && metadata.is_object() &&
            metadata.value("runtime", "") == "hermes_native") {
            HermesChatOptions receipt;
            receipt.session_id = reply.value("session_id", "");
            receipt.control = {{"operation", "confirm_reply"}, {"request_id", request_id},
                               {"run_id", metadata.value("run_id", "")}};
            auto receipt_config = config_;
            receipt_config.request_timeout_ms = std::min(5000, config_.request_timeout_ms);
            HermesChatResult result;
            std::string error;
            if (!HermesClient(receipt_config).Chat(nlohmann::json::array(), receipt, &result, &error)) {
                // Kafka already accepted the reply. Retry the same cached result
                // and receipt; never rerun tools. IM deduplicates its request id.
                return false;
            }
        }
        LOG_INFO << "Hermes bridge completed request_id=" << request_id
                 << ", ok=" << (reply.value("ok", false) ? "true" : "false")
                 << ", streamed="
                 << (reply.value("streamed", false) ? "true" : "false")
                 << ", delta_count=" << reply.value("delta_count", 0);
        return true;
    }

    HermesBridgeConfig config_;
    HermesClient client_;
    std::unique_ptr<GrpcInferenceClient> grpc_client_;
    ModelClient* model_client_{nullptr};
    KafkaConsumer request_consumer_;
    KafkaProducer delta_producer_;
    KafkaProducer reply_producer_;
    std::mutex cache_mutex_;
    std::unordered_map<std::string, std::string> reply_cache_;
};

}  // namespace sparkpush

int main(int argc, char** argv) {
    std::string config_path;
    const auto result = ParseArgs(argc, argv, &config_path);
    if (result == ArgResult::kHelp) {
        PrintUsage(argv[0]);
        return 0;
    }
    if (result == ArgResult::kError) {
        PrintUsage(argv[0]);
        return 2;
    }

    sparkpush::InitLogging("hermes_bridge");
    sparkpush::HermesBridgeConfig config;
    std::string error;
    if (!sparkpush::LoadHermesBridgeConfig(config_path, &config, &error)) {
        LOG_ERROR << "Load Hermes bridge config failed: " << error;
        sparkpush::ShutdownLogging();
        return 1;
    }

    sparkpush::HermesBridgeRunner runner(config);
    if (!runner.Init()) {
        sparkpush::ShutdownLogging();
        return 1;
    }
    LOG_INFO << "Hermes bridge started, request_topic=" << config.request_topic
             << ", delta_topic=" << config.delta_topic
             << ", reply_topic=" << config.reply_topic
             << ", Hermes=" << config.hermes_base_url
             << ", streaming=" << (config.streaming ? "true" : "false");
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);
    runner.Start();
    while (g_running && !runner.failed())
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    runner.Stop();
    sparkpush::ShutdownLogging();
    return runner.failed() ? 1 : 0;
}
