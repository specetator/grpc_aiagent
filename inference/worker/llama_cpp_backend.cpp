#include "worker/llama_cpp_backend.h"

#include <chrono>
#include <memory>
#include <stdexcept>

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
      if (!state->Process(event)) return 0;
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

}  // namespace sparkpush::inference
