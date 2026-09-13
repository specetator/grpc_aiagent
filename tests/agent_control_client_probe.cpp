#include "hermes_client.h"
#include "hermes_reply.h"
#include <iostream>
#include <stdexcept>

void Require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
int main(int argc, char** argv) {
    if (argc != 2) return 2;
    try {
        sparkpush::HermesBridgeConfig config;
        config.hermes_base_url = argv[1];
        config.hermes_api_key = "local-test-only";
        config.request_timeout_ms = 5000;
        sparkpush::HermesClient client(config);
        sparkpush::HermesChatOptions options;
        options.session_id = "s_7_99";
        options.control = {{"operation", "list_models"}};
        sparkpush::HermesChatResult result;
        std::string error;
        Require(client.Chat(nlohmann::json::array(), options, &result, &error), "HTTP model query failed");
        Require(result.metadata["agent_event"]["data"]["presentation"]["models"].size() == 3,
                "HTTP catalog lost models");
        // Exercise the exact control metadata -> persisted IM history projection.
        const nlohmann::json reply = {{"request_id", "s_7_99-1"}, {"session_id", "s_7_99"},
            {"user_id", 7}, {"bot_user_id", 99}, {"ok", true}, {"completed_at_ms", 1750000000000LL},
            {"command_handled_locally", true}, {"command", "model"}, {"text", result.text},
            {"response_metadata", result.metadata}};
        sparkpush::PreparedHermesReply prepared;
        Require(sparkpush::PrepareHermesReply(reply.dump(), 99, "Pi", &prepared, &error),
                "gateway control cannot become an IM history card");
        Require(prepared.message.content_json.find("model_picker") != std::string::npos,
                "history card disappeared");
        options.control = {{"operation", "set_model"}, {"command_seq", 2},
            {"target_provider", "fixture"}, {"target_model", "other"}};
        Require(client.Chat(nlohmann::json::array(), options, &result, &error), "HTTP model selection failed");
        Require(result.metadata["model_state"]["model"]["id"] == "other", "selection not confirmed");
        options.control = nullptr;
        const auto messages = nlohmann::json::array({{{"role", "user"}, {"content", "which-model"}}});
        std::string preview;
        Require(client.ChatStream(messages, options, [&](const std::string& s) { preview += s; }, &result, &error),
                "generic streaming transport failed");
        Require(preview == "other" && result.text == "other", "selected model not used by next turn");
        options.control = {{"operation", "set_model"}, {"command_seq", 3},
            {"target_provider", "fixture"}, {"target_model", "missing"}};
        Require(!client.Chat(nlohmann::json::array(), options, &result, &error), "invalid model returned success");
        options.control = {{"operation", "reset_model"}, {"command_seq", 4}};
        Require(client.Chat(nlohmann::json::array(), options, &result, &error), "HTTP reset failed");
        Require(result.metadata["model_state"]["model"]["id"] == "default", "default not restored");
        for (const auto* operation : {"help", "status", "list_reasoning", "retry", "new"}) {
            options.control = {{"operation", operation}};
            Require(client.Chat(nlohmann::json::array(), options, &result, &error), "command query failed");
            Require(result.metadata["agent_event"]["data"]["presentation"]["kind"] == "command_card",
                    "generic command presentation lost");
            auto card_reply = reply;
            card_reply["command"] = operation;
            card_reply["text"] = result.text;
            card_reply["response_metadata"] = result.metadata;
            Require(sparkpush::PrepareHermesReply(card_reply.dump(), 99, "Pi", &prepared, &error),
                    "command card cannot persist");
            Require(prepared.message.content_json.find("command_card") != std::string::npos,
                    "command card not present in history");
        }
        options.control = {{"operation", "set_reasoning"}, {"level", "max"}, {"command_seq", 5}};
        Require(client.Chat(nlohmann::json::array(), options, &result, &error), "reasoning selection failed");
        Require(result.metadata["thinking_state"]["level"] == "max", "reasoning confirmation lost");
        options.control = nullptr;
        options.retry = true;
        Require(client.ChatStream(nlohmann::json::array(), options, [](const std::string&) {}, &result, &error),
                "retry with empty IM history failed");
        Require(result.text == "default", "retry did not use actual current model");
        Require(client.Chat(nlohmann::json::array(), options, &result, &error), "nonstream retry failed");
        options.retry = false;
        options.control = {{"operation", "new_session"}, {"command_seq", 10}};
        Require(client.Chat(nlohmann::json::array(), options, &result, &error), "new session failed");
        Require(result.metadata["context_start_seq"] == 10, "confirmed boundary lost");
        options.control = nullptr;
        options.retry = true;
        Require(!client.Chat(nlohmann::json::array(), options, &result, &error), "new session retried old question");
        std::cout << "C++ bridge/gateway control and AgentEvent integration passed\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
