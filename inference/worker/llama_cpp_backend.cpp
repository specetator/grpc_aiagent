#include "worker/llama_cpp_backend.h"

#include <chrono>
#include <memory>
#include <stdexcept>
#include <sstream>

#include <curl/curl.h>
#include <nlohmann/json.hpp>

namespace sparkpush::inference {
namespace {
int64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
}

class StreamState {
 public:
  StreamState(const GenerateRequest& request, const std::atomic<bool>& cancelled,
      const std::function<bool(const GenerateChunk&)>& emit)
      : request_(request), cancelled_(cancelled), emit_(emit) {}

  static size_t Write(char* data, size_t size, size_t count, void* user) {
    auto* state = static_cast<StreamState*>(user);
    const size_t bytes = size * count;
    if (state->cancelled_.load() || !state->accepted_) return 0;
    for (size_t i = 0; i < bytes; ++i) {
      if (data[i] != '\r') state->pending_.push_back(data[i]);
    }
    if (state->pending_.size() > 1'048'576) {
      state->error_ = "llama.cpp SSE event exceeds 1 MiB";
      return 0;
    }
    size_t boundary;
    while ((boundary = state->pending_.find("\n\n")) != std::string::npos) {
      std::string event = state->pending_.substr(0, boundary);
      state->pending_.erase(0, boundary + 2);
      try {
        if (!state->Process(event)) return 0;
      } catch (...) {
        state->error_ = "invalid runtime SSE field type";
        return 0;  // Exceptions must not unwind through libcurl's C callback.
      }
    }
    return bytes;
  }

  static int Progress(void* user, curl_off_t, curl_off_t, curl_off_t, curl_off_t) {
    const auto* state = static_cast<const StreamState*>(user);
    return state->cancelled_.load() ? 1 : 0;
  }

  bool completed() const { return done_ && saw_finish_ && accepted_; }
  bool cancelled() const { return cancelled_.load() || !accepted_; }
  const std::string& error() const { return error_; }

 private:
  bool Process(std::string event) {
    std::string data;
    size_t start = 0;
    while (start <= event.size()) {
      const size_t end = event.find('\n', start);
      std::string line = event.substr(start, end == std::string::npos ?
          std::string::npos : end - start);
      if (!line.empty() && line.back() == '\r') line.pop_back();
      if (line.rfind("data:", 0) == 0) {
        if (!data.empty()) data += '\n';
        data += line.substr(line.size() > 5 && line[5] == ' ' ? 6 : 5);
      }
      if (end == std::string::npos) break;
      start = end + 1;
    }
    if (data.empty()) return true;
    if (data == "[DONE]") { done_ = true; return true; }
    auto json = nlohmann::json::parse(data, nullptr, false);
    if (!json.is_object() || json.contains("error")) {
      error_ = "llama.cpp returned an invalid or error SSE event";
      return false;
    }
    if (json.contains("usage") && json["usage"].is_object()) {
      const auto& usage = json["usage"];
      prompt_tokens_ = usage.value("prompt_tokens", uint64_t{0});
      completion_tokens_ = usage.value("completion_tokens", uint64_t{0});
      if (usage.contains("prompt_tokens_details") && usage["prompt_tokens_details"].is_object())
        cached_tokens_ = usage["prompt_tokens_details"].value("cached_tokens", uint64_t{0});
    }
    if (json.contains("timings") && json["timings"].is_object()) {
      prefill_ms_ = json["timings"].value("prompt_ms", 0.0);
      decode_ms_ = json["timings"].value("predicted_ms", 0.0);
    }
    if (!json.contains("choices") || !json["choices"].is_array() ||
        json["choices"].empty()) return true;
    const auto& choice = json["choices"][0];
    if (!choice.is_object()) { error_ = "invalid llama.cpp choice"; return false; }
    if (choice.contains("delta") && choice["delta"].is_object() &&
        choice["delta"].contains("content") &&
        choice["delta"]["content"].is_string()) {
      const std::string token = choice["delta"]["content"].get<std::string>();
      if (!token.empty()) {
        GenerateChunk chunk;
        chunk.set_request_id(request_.request_id());
        chunk.set_sequence(sequence_++);
        chunk.set_text_delta(token);
        chunk.set_timestamp_ms(NowMs());
        accepted_ = emit_(chunk);
        if (!accepted_) return false;
      }
    }
    if (choice.contains("finish_reason") && choice["finish_reason"].is_string()) {
      finish_reason_ = choice["finish_reason"].get<std::string>();
      saw_finish_ = true;
    }
    return true;
  }

 public:
  bool EmitTerminal() {
    GenerateChunk chunk;
    chunk.set_request_id(request_.request_id());
    chunk.set_sequence(sequence_);
    chunk.set_finished(true);
    chunk.set_finish_reason(finish_reason_);
    chunk.set_timestamp_ms(NowMs());
    chunk.set_prompt_tokens(prompt_tokens_);
    chunk.set_completion_tokens(completion_tokens_);
    chunk.set_cached_prompt_tokens(cached_tokens_);
    chunk.set_runtime_prefill_ms(prefill_ms_);
    chunk.set_runtime_decode_ms(decode_ms_);
    return emit_(chunk);
  }

 private:
  const GenerateRequest& request_;
  const std::atomic<bool>& cancelled_;
  const std::function<bool(const GenerateChunk&)>& emit_;
  std::string pending_;
  std::string error_;
  std::string finish_reason_;
  uint64_t sequence_{0};
  bool accepted_{true};
  bool saw_finish_{false};
  bool done_{false};
  uint64_t prompt_tokens_{0}, completion_tokens_{0}, cached_tokens_{0};
  double prefill_ms_{0}, decode_ms_{0};
};
}  // namespace

bool LlamaCppBackend::Generate(const GenerateRequest& request,
    const std::atomic<bool>& cancelled,
    const std::function<bool(const GenerateChunk&)>& emit) {
  if (endpoint_.rfind("http://", 0) != 0)
    throw std::runtime_error("llama.cpp endpoint must use http://");
  nlohmann::json messages = nlohmann::json::array();
  for (const auto& turn : request.chat_turns()) {
    if (turn.content().empty()) continue;
    if (turn.role() != "user" && turn.role() != "assistant" &&
        turn.role() != "system")
      throw std::runtime_error("unsupported chat role");
    messages.push_back({{"role", turn.role()}, {"content", turn.content()}});
  }
  if (messages.empty())
    messages.push_back({{"role", "user"}, {"content", request.prompt()}});
  nlohmann::json body = {
      {"model", request.model()}, {"messages", messages}, {"stream", true},
      {"max_tokens", request.max_tokens() == 0 ? 128 : request.max_tokens()},
      {"temperature", request.has_temperature() ? request.temperature() : 0.7f}};
  body["stream_options"] = {{"include_usage", true}};
  for (const auto& item : request.metadata()) {
    if (item.first == "seed") body["seed"] = std::stoi(item.second);
    if (runtime_ == "llamacpp" &&
        (item.first == "cache_prompt" || item.first == "ignore_eos"))
      body[item.first] = item.second == "true";
  }
  const std::string payload = body.dump();
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), &curl_easy_cleanup);
  if (!curl) throw std::runtime_error("curl initialization failed");
  std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(
      curl_slist_append(nullptr, "Content-Type: application/json"), &curl_slist_free_all);
  StreamState state(request, cancelled, emit);
  const std::string url = endpoint_ +
      (endpoint_.back() == '/' ? "v1/chat/completions" : "/v1/chat/completions");
  curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl.get(), CURLOPT_HTTPHEADER, headers.get());
  curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDS, payload.c_str());
  curl_easy_setopt(curl.get(), CURLOPT_POSTFIELDSIZE, payload.size());
  curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION, &StreamState::Write);
  curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, &state);
  curl_easy_setopt(curl.get(), CURLOPT_XFERINFOFUNCTION, &StreamState::Progress);
  curl_easy_setopt(curl.get(), CURLOPT_XFERINFODATA, &state);
  curl_easy_setopt(curl.get(), CURLOPT_NOPROGRESS, 0L);
  curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT_MS, 3000L);
  curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT_MS, 120000L);
  const CURLcode code = curl_easy_perform(curl.get());
  long http_status = 0;
  curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &http_status);
  if (state.cancelled()) return false;
  if (code != CURLE_OK || http_status != 200 || !state.error().empty() ||
      !state.completed())
    throw std::runtime_error("llama.cpp stream failed: " +
        (state.error().empty() ? std::string(curl_easy_strerror(code)) : state.error()) +
        ", HTTP " + std::to_string(http_status));
  return state.EmitTerminal();
}

void LlamaCppBackend::GetRuntimeStatus(WorkerInfo* info) {
  auto get = [&](const std::string& path, std::string* body) {
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> curl(curl_easy_init(), &curl_easy_cleanup);
    if (!curl) return false;
    const std::string url = endpoint_ + (endpoint_.back() == '/' ? path : "/" + path);
    curl_easy_setopt(curl.get(), CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl.get(), CURLOPT_CONNECTTIMEOUT_MS, 200L);
    curl_easy_setopt(curl.get(), CURLOPT_TIMEOUT_MS, 300L);
    curl_easy_setopt(curl.get(), CURLOPT_WRITEFUNCTION,
        +[](char* data, size_t size, size_t n, void* ptr) -> size_t {
          auto* out = static_cast<std::string*>(ptr);
          if (out->size() + size * n > 1048576) return 0;
          out->append(data, size * n);
          return size * n;
        });
    curl_easy_setopt(curl.get(), CURLOPT_WRITEDATA, body);
    const auto code = curl_easy_perform(curl.get());
    long status = 0;
    curl_easy_getinfo(curl.get(), CURLINFO_RESPONSE_CODE, &status);
    return code == CURLE_OK && status == 200;
  };
  std::string body;
  info->set_healthy(get("health", &body));
  body.clear();
  if (!get("metrics", &body)) return;  // Optional exporter, health remains authoritative.
  std::istringstream lines(body);
  std::string line;
  double running = 0, waiting = 0, kv = 0;
  bool found_running = false, found_waiting = false;
  while (std::getline(lines, line)) {
    if (line.empty() || line[0] == '#') continue;
    // A label value can itself contain spaces. The sample follows the closing
    // label brace (or the metric name for an unlabeled sample).
    const auto labels_end = line.find('}');
    const auto space = line.find_first_of(" \t", labels_end == std::string::npos ? 0 : labels_end + 1);
    if (space == std::string::npos) continue;
    const auto name = line.substr(0, line.find_first_of(" {"));
    try {
      const double value = std::stod(line.substr(space + 1));
      if (name == "llamacpp:requests_processing" || name == "vllm:num_requests_running") {
        running += value; found_running = true;
      }
      if (name == "llamacpp:requests_deferred" || name == "vllm:num_requests_waiting") {
        waiting += value; found_waiting = true;
      }
      if (name == "llamacpp:kv_cache_usage_ratio" || name == "vllm:kv_cache_usage_perc") kv = value;
    } catch (...) { /* Ignore unrelated exporter samples. */ }
  }
  if (found_running) info->set_runtime_running_requests(std::max(0.0, running));
  if (found_waiting) {
    info->set_runtime_waiting_requests(std::max(0.0, waiting));
    info->set_waiting_requests(info->runtime_waiting_requests());
  }
  info->set_kv_cache_usage_ratio(kv);
}

}  // namespace sparkpush::inference
