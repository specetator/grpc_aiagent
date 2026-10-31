#include "services/agent_connection_service.h"
#include <curl/curl.h>
#include <memory>
#include <set>
#include <stdexcept>

namespace sparkpush {
namespace {
size_t Collect(char* data, size_t size, size_t count, void* target) {
    auto* out = static_cast<std::string*>(target);
    const auto bytes = size * count;
    if (bytes > 256 * 1024 || out->size() > 256 * 1024 - bytes) return 0;
    out->append(data, bytes);
    return bytes;
}
}
AgentConnectionService::Transport AgentConnectionService::LocalGateway(int port, const std::string& key, const std::string& path) {
    if(path!="/v1/agent/connections" && path!="/v1/agent/room-control")throw std::invalid_argument("invalid Gateway service path");
    if (port < 1 || port > 65535 || key.empty() || key.find_first_of("\r\n") != std::string::npos)
        throw std::invalid_argument("invalid Agent Gateway configuration");
    static const auto initialized = curl_global_init(CURL_GLOBAL_DEFAULT);
    if (initialized != CURLE_OK) throw std::runtime_error("HTTP initialization failed");
    return [port, key, path](const nlohmann::json& input) {
        std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), curl_easy_cleanup);
        if (!curl) throw std::runtime_error("HTTP initialization failed");
        auto body = input.dump();
        std::string response;
        const auto url = "http://127.0.0.1:" + std::to_string(port) + path;
        curl_slist* raw = nullptr;
        raw = curl_slist_append(raw, "Content-Type: application/json");
        raw = curl_slist_append(raw, ("Authorization: Bearer " + key).c_str());
        std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(raw, curl_slist_free_all);
        curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl.get(), CURLOPT_PROXY, "");
        curl_easy_setopt(curl.get(), CURLOPT_FOLLOWLOCATION, 0L);
        curl_easy_setopt(curl.get(), CURLOPT_NOSIGNAL, 1L);
        curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT_MS, 1000L);
        curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT_MS, 8000L);
        curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, body.data());
        curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
        curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
        curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, Collect);
        curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &response);
        const auto result = curl_easy_perform(curl.get());
        long status = 0;
        curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status);
        if (result != CURLE_OK || status != 200) throw std::runtime_error("Agent Gateway unavailable");
        return nlohmann::json::parse(response);
    };
}
ServiceStatus AgentConnectionService::Execute(int64_t actor, const nlohmann::json& input, nlohmann::json* output) {
    if (!transport_) return {503, "用户 Agent 接入尚未启用"};
    if (actor <= 0 || !input.is_object() || input.dump().size() > 16384) return {400, "invalid connection request"};
    static const std::set<std::string> fields{"action", "runtime", "endpoint", "token", "revision"};
    for (auto it = input.begin(); it != input.end(); ++it)
        if (!fields.count(it.key()) || !it.value().is_string()) return {400, "invalid connection fields"};
    const auto action = input.value("action", "");
    if (action != "list" && action != "probe" && action != "save" && action != "delete") return {400, "invalid action"};
    if (active_.fetch_add(1) >= 2) { --active_; return {429, "连接服务繁忙，请稍后重试"}; }
    struct Release { std::atomic<int>& n; ~Release() { --n; } } release{active_};
    try {
        auto trusted = input;
        trusted["actor_user_id"] = actor;
        const auto result = transport_(trusted);
        const int code = result.at("code").get<int>();
        if (code != 0) return {code, result.value("message", "连接服务失败")};
        *output = result.at("data");
        return {};
    } catch (const std::exception&) {
        return {503, "连接服务不可用，请稍后重试"};
    }
}
}
