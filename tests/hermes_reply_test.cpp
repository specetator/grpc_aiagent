#include "hermes_reply.h"
#include "agent_event.h"
#include <nlohmann/json.hpp>
#include <iostream>
#include <stdexcept>

namespace {
void Require(bool value, const char* reason) {
    if (!value) throw std::runtime_error(reason);
}
}

int main() {
    try {
        using namespace sparkpush;
        const nlohmann::json source = {
            {"request_id", "s_7_99-42"}, {"session_id", "s_7_99"},
            {"user_id", 7}, {"bot_user_id", 99}, {"ok", true},
            {"completed_at_ms", 1750000000123LL}, {"text", "最终答案🙂"}};
        PreparedHermesReply prepared;
        std::string error;
        Require(PrepareHermesReply(source.dump(), 99, "Pi", &prepared, &error), "valid reply rejected");
        int allocations = 0, publishes = 0;
        std::string first_bytes;
        auto allocate = [&](Message* msg, bool* fresh) {
            *fresh = ++allocations == 1;
            msg->msg_seq = 43; // Redis dedup hit returns the SAME sequence
            msg->msg_id = msg->session_id + "-43";
            return true;
        };
        auto persist = [&](const Message& msg) {
            Require(msg.timestamp_ms == 1750000000123LL, "unstable database timestamp");
            auto body = nlohmann::json::parse(msg.content_json);
            Require(body["create_time"] == msg.timestamp_ms && body["msg_seq"] == 43,
                    "history and online envelope differ");
            if (++publishes == 1) first_bytes = msg.content_json;
            Require(first_bytes == msg.content_json, "replay changed immutable message bytes");
            return publishes != 1; // timeout could mean Kafka received it already
        };
        bool fresh = false;
        Require(!PersistHermesReply(&prepared.message, &fresh, allocate, persist) && fresh,
                "unconfirmed persistence accepted");
        // Simulate restart: reconstruct from the Kafka record, no local cache.
        Require(PrepareHermesReply(source.dump(), 99, "Pi", &prepared, &error), "replay parse failed");
        Require(PersistHermesReply(&prepared.message, &fresh, allocate, persist) && !fresh,
                "Redis duplicate bypassed persistence retry");
        Require(publishes == 2, "answer lost after first publish failure");
        Require(!PersistHermesReply(&prepared.message, &fresh,
                    [](Message*, bool*) { return false; }, persist) && publishes == 2,
                "sequence failure still published");

        const nlohmann::json model = {{"provider", "fixture"}, {"id", "other"}, {"name", "Other"},
                                     {"headers", {{"Authorization", "must-not-escape"}}}};
        auto event = nlohmann::json{{"schema", "sparkpush.agent_event.v1"}, {"type", "assistant_final"},
            {"data", {{"text", source["text"]}, {"presentation", {{"kind", "model_picker"},
                {"models", nlohmann::json::array({model})}, {"current", model}}}}}};
        auto menu = source;
        menu["command_handled_locally"] = true;
        menu["command"] = "model";
        menu["response_metadata"] = {{"agent_event", event}};
        Require(PrepareHermesReply(menu.dump(), 99, "Pi", &prepared, &error), "model card rejected");
        auto content = nlohmann::json::parse(prepared.message.content_json)["content"];
        Require(content["agent_event"]["data"]["presentation"]["models"].size() == 1,
                "model card missing from persisted history");
        Require(prepared.message.content_json.find("must-not-escape") == std::string::npos,
                "runtime credentials escaped to IM");
        menu["response_metadata"]["agent_event"]["data"]["presentation"]["models"][0]["id"] = "bad\n/model";
        Require(!PrepareHermesReply(menu.dump(), 99, "Pi", &prepared, &error), "executable model action accepted");

        auto confirmed = source;
        confirmed["command_handled_locally"] = true;
        confirmed["command"] = "model";
        confirmed["client_msg_id"] = "model-action-1";
        confirmed["model_state_confirmed"] = true;
        confirmed["effective_provider"] = "fixture";
        confirmed["effective_model"] = "chosen";
        Require(PrepareHermesReply(confirmed.dump(), 99, "Pi", &prepared, &error), "model receipt rejected");
        auto receipt = nlohmann::json::parse(prepared.message.content_json)["content"]["agent_command_result"];
        Require(receipt["ok"] == true && receipt["client_msg_id"] == "model-action-1" &&
                receipt["model"]["id"] == "chosen", "confirmed model receipt missing from history");
        confirmed["ok"] = false;
        Require(PrepareHermesReply(confirmed.dump(), 99, "Pi", &prepared, &error), "failed command receipt rejected");
        receipt = nlohmann::json::parse(prepared.message.content_json)["content"]["agent_command_result"];
        Require(receipt["ok"] == false && !receipt.contains("model"), "failure reported successful selection");
        confirmed["ok"] = true;
        confirmed["model_state_confirmed"] = false;
        Require(PrepareHermesReply(confirmed.dump(), 99, "Pi", &prepared, &error), "unconfirmed command rejected");
        Require(nlohmann::json::parse(prepared.message.content_json)["content"]["agent_command_result"]["ok"] == false,
                "unconfirmed transport success changed model card");

        auto large = event;
        auto& catalog = large["data"]["presentation"]["models"];
        catalog = nlohmann::json::array();
        for (int i = 0; i < 300; ++i)
            catalog.push_back({{"provider", "hermes-custom-fixture"}, {"id", "model-" + std::to_string(i)},
                               {"name", std::string(60, 'x')}});
        menu["response_metadata"] = {{"agent_event", large}};
        Require(PrepareHermesReply(menu.dump(), 99, "Hermes", &prepared, &error), "full Hermes catalog rejected");
        catalog[0]["name"] = std::string(96001, 'x');
        menu["response_metadata"] = {{"agent_event", large}};
        Require(!PrepareHermesReply(menu.dump(), 99, "Hermes", &prepared, &error), "unbounded catalog accepted");
        Require(AgentCommandActionValid({{"id", "restart_confirm"}, {"label", "确认重启"}}), "restart card rejected");

        event["data"]["presentation"] = {{"kind", "command_card"}, {"title", "思考等级"},
            {"actions", nlohmann::json::array({{{"id", "reasoning_set"}, {"label", "max"}, {"value", "max"},
                                              {"command", "/malicious"}}})}};
        menu["command"] = "reasoning";
        menu["response_metadata"] = {{"agent_event", event}, {"context_start_seq", 100}};
        Require(PrepareHermesReply(menu.dump(), 99, "Pi", &prepared, &error), "reasoning card rejected");
        Require(prepared.message.content_json.find("/malicious") == std::string::npos,
                "arbitrary command escaped public projection");
        Require(nlohmann::json::parse(prepared.message.content_json)["content"]["hermes_context_start_seq"] == 100,
                "confirmed context missing in history");
        menu["response_metadata"]["agent_event"]["data"]["presentation"]["actions"][0]["value"] = "high\n/new now";
        Require(!PrepareHermesReply(menu.dump(), 99, "Pi", &prepared, &error), "injected reasoning command accepted");
        menu["response_metadata"]["agent_event"]["data"]["presentation"]["actions"][0] = {{"id", "exec"}, {"label", "Run"}};
        Require(!PrepareHermesReply(menu.dump(), 99, "Pi", &prepared, &error), "unknown executable action accepted");
        menu["response_metadata"]["agent_event"]["data"]["presentation"]["actions"][0] =
            {{"id", "agent_set"}, {"label", "Hermes"}, {"value", "hermes-technical"}};
        Require(PrepareHermesReply(menu.dump(), 99, "Pi", &prepared, &error), "Agent selection card rejected");
        Require(prepared.message.content_json.find("hermes-technical") != std::string::npos,
                "Agent card target lost from history");
        menu["response_metadata"]["agent_event"]["data"]["presentation"]["actions"][0] =
            {{"id", "agent_open"}, {"label", "Hermes"}, {"value", "900000000101"}};
        Require(PrepareHermesReply(menu.dump(), 99, "Pi", &prepared, &error), "Agent contact card rejected");
        Require(prepared.message.content_json.find("900000000101") != std::string::npos,
                "Agent contact lost from history");
        menu["response_metadata"]["agent_event"]["data"]["presentation"]["actions"][0]["value"] = "pi\n/new now";
        Require(!PrepareHermesReply(menu.dump(), 99, "Pi", &prepared, &error), "injected Agent target accepted");

        for (const auto& field : {"request_id", "session_id", "user_id", "bot_user_id", "ok", "completed_at_ms"}) {
            auto invalid = source;
            invalid.erase(field);
            Require(!PrepareHermesReply(invalid.dump(), 99, "Pi", &prepared, &error), "missing field accepted");
            invalid = source;
            invalid[field] = nlohmann::json::array();
            Require(!PrepareHermesReply(invalid.dump(), 99, "Pi", &prepared, &error), "wrong field type accepted");
        }
        for (const auto& change : {
                nlohmann::json{{"session_id", "s_8_99"}},
                nlohmann::json{{"bot_user_id", 98}},
                nlohmann::json{{"user_id", 7.5}},
                nlohmann::json{{"user_id", 18446744073709551615ULL}},
                nlohmann::json{{"completed_at_ms", 0}},
                nlohmann::json{{"completed_at_ms", -1}}}) {
            auto invalid = source;
            invalid.update(change);
            Require(!PrepareHermesReply(invalid.dump(), 99, "Pi", &prepared, &error), "invalid routing/time accepted");
        }
        for (const auto& invalid : {"[]", "null", "{\"private-message\""}) {
            Require(!PrepareHermesReply(invalid, 99, "Pi", &prepared, &error), "poison record accepted");
            Require(error == "invalid ai_reply envelope", "parser exposed payload");
        }
        std::cout << "Hermes reply retry and validation tests passed\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
