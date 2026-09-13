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
#include "hermes_client.h"
#include "kafka_consumer.h"
#include "kafka_producer.h"
#include "logging.h"
#include "metrics.h"
#include "agent_event.h"

namespace {

std::atomic<bool> g_running{true};

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
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
        : config_(config), client_(config) {}

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
        // consume() is synchronous: allow the full bounded retry budget before
        // max.poll expiry. This does not add concurrency; a session worker will.
        const int64_t poll_budget = 3LL * (config_.request_timeout_ms +
            static_cast<int64_t>(config_.reply_delivery_timeout_ms)) + 65000;
        if (poll_budget > std::numeric_limits<int>::max()) return false;
        options.max_poll_interval_ms = static_cast<int>(std::max<int64_t>(300000, poll_budget));
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
    std::string BuildLocalCommandReply(const nlohmann::json& command) const {
        const std::string local_error = command.value("local_error", "");
        if (!local_error.empty()) return local_error;

        const std::string input_name = command.value("input_name", "");
        return "暂不支持 Pi Agent 命令 /" + input_name +
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
            reply["client_msg_id"] = request.value("client_msg_id", "");
            const auto messages =
                request.value("messages", nlohmann::json::array());
            auto command =
                request.value("command", nlohmann::json::object());
            if (!command.is_object()) command = nlohmann::json::object();
            const bool call_model = request.value("call_model", true);
            const std::string command_name = command.value("name", "");
            HermesChatOptions chat_options;
            chat_options.retry = call_model && command_name == "retry";
            chat_options.session_id = request.value("session_id", "");
            chat_options.context_start_seq =
                request.value("context_start_seq", 0LL);
            if (request.value("model_override", false)) {
                chat_options.model = request.value("model", "");
                chat_options.provider = request.value("provider", "");
            }
            const std::string effective_model =
                chat_options.model.empty() ? config_.hermes_model
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
            size_t prompt_chars = 0;
            if (messages.is_array()) {
                for (const auto& item : messages) {
                    if (item.is_object() && item.contains("content") &&
                        item.at("content").is_string()) {
                        prompt_chars += item.at("content").get<std::string>().size();
                    }
                }
            }
            HermesChatResult chat_result;
            std::string error;
            int delta_count = 0;
            bool delta_publish_failed = false;
            int64_t first_delta_at_ms = 0;
            std::string delta_buffer;
            auto last_delta_flush = std::chrono::steady_clock::now();
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
                delta_buffer += value;
                const auto elapsed = std::chrono::duration_cast<
                    std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - last_delta_flush)
                                          .count();
                // 首个 token 立即显示；后续按字节数或时间合并，避免一个
                // token 产生一个 Kafka/Job/Comet 消息。
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
                if (!PublishDelta(request_id, progress)) delta_publish_failed = true;
            };
            bool ok = true;
            if (!call_model && (command_name == "agent" || command_name == "model" || command_name == "reasoning" ||
                command_name == "new" || command_name == "retry" || command_name == "restart" || command_name == "status" ||
                command_name == "help" || command_name == "roleplay" || command_name == "write" ||
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
                           command_name == "remember" || command_name == "forget") {
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
                ok = client_.ChatStream(messages, chat_options, on_delta,
                                        &chat_result, &error);
            } else {
                ok = client_.Chat(messages, chat_options, &chat_result, &error);
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
            reply["ok"] = ok;
            reply["streamed"] = call_model && config_.streaming;
            reply["delta_count"] = delta_count;
            reply["delta_publish_failed"] = delta_publish_failed;
            const int64_t completed_at_ms = NowMs();
            reply["request_started_at_ms"] = request_started_at_ms;
            reply["first_delta_at_ms"] = first_delta_at_ms;
            reply["completed_at_ms"] = completed_at_ms;
            reply["total_latency_ms"] = completed_at_ms - request_started_at_ms;
            reply["prompt_chars"] = prompt_chars;
            reply["prompt_message_count"] = messages.is_array() ? messages.size() : 0;
            MetricsRegistry::Instance().Observe(
                "spark_push_hermes_total_latency_ms",
                completed_at_ms - request_started_at_ms);
            MetricsRegistry::Instance().Observe(
                "spark_push_hermes_prompt_chars",
                static_cast<int64_t>(prompt_chars));
            if (!call_model) {
                MetricsRegistry::Instance().Increment(
                    "spark_push_hermes_commands_total_" +
                    command.value("name", "unknown"));
            }
            if (ok) {
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
            const std::string serialized = reply.dump();
            {
                std::lock_guard<std::mutex> lock(cache_mutex_);
                if (reply_cache_.size() >= 10000) reply_cache_.clear();
                reply_cache_[request_id] = serialized;
            }
        }

        if (!PublishReply(request_id, reply)) {
            LOG_ERROR << "Hermes bridge failed to publish ai_reply request_id="
                      << request_id;
            return false;
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
