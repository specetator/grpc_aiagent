#include "sse_parser.h"
#include "agent_event.h"

namespace sparkpush {
namespace {
constexpr size_t kMaxBytes = 16 * 1024 * 1024;

std::string Text(const nlohmann::json& value) {
  if (value.is_string()) return value.get<std::string>();
  std::string result;
  if (value.is_array()) {
    for (const auto& block : value) {
      if (block.is_string()) result += block.get<std::string>();
      else if (block.is_object() &&
               (block.value("type", "") == "text" ||
                block.value("type", "") == "output_text")) {
        result += block.value("text", "");
      }
    }
  }
  return result;
}
}  // namespace

bool HermesSseParser::Fail(const std::string& message, HermesChatResult* result,
                          std::string* error) {
  if (failure_.empty()) failure_ = message;
  if (error) *error = failure_;
  if (result) {
    result->text.clear();
    result->finish_reason.clear();
    result->stream_chunk_count = 0;
    result->agent_event = nlohmann::json::object();
    result->citations = nlohmann::json::array();
    result->metadata = nlohmann::json::object();
  }
  return false;
}

bool HermesSseParser::Feed(
    const std::string& chunk, HermesChatResult* result,
    const std::function<void(const std::string&)>& on_delta, std::string* error) {
  if (!failure_.empty()) return Fail(failure_, result, error);
  if (!result) return Fail("missing gateway stream result", result, error);
  if (pending_.size() + chunk.size() > kMaxBytes) {
    return Fail("gateway SSE buffer exceeds limit", result, error);
  }
  pending_ += chunk;
  size_t end;
  while ((end = pending_.find('\n')) != std::string::npos) {
    std::string line = pending_.substr(0, end);
    pending_.erase(0, end + 1);
    if (!line.empty() && line.back() == '\r') line.pop_back();
    if (line.empty()) {
      if (!ProcessEvent(result, on_delta, error)) return false;
    } else if (line.rfind("event:", 0) == 0) {
      event_name_ = line.substr(6);
      if (!event_name_.empty() && event_name_.front() == ' ') event_name_.erase(0, 1);
    } else if (line.rfind("data:", 0) == 0) {
      std::string data = line.substr(5);
      if (!data.empty() && data.front() == ' ') data.erase(0, 1);
      if (event_data_.size() + data.size() + 1 > kMaxBytes) {
        return Fail("gateway SSE event exceeds limit", result, error);
      }
      event_data_ += data + '\n';
    }
  }
  return true;
}

bool HermesSseParser::Finish(
    HermesChatResult* result,
    const std::function<void(const std::string&)>& on_delta, std::string* error) {
  if (!failure_.empty()) return Fail(failure_, result, error);
  if (!result) return Fail("missing gateway stream result", result, error);
  if (!pending_.empty() && !Feed("\n", result, on_delta, error)) return false;
  if (!ProcessEvent(result, on_delta, error)) return false;
  if (!done_ || (!final_ && !stopped_)) {
    return Fail("gateway stream ended without a successful terminal event", result, error);
  }
  if (!result || result->text.empty()) {
    return Fail("gateway returned an empty answer", result, error);
  }
  return true;
}

bool HermesSseParser::ProcessEvent(
    HermesChatResult* result,
    const std::function<void(const std::string&)>& on_delta, std::string* error) {
  if (event_data_.empty()) {
    event_name_.clear();
    return true;
  }
  std::string data = std::move(event_data_);
  event_data_.clear();
  std::string name = std::move(event_name_);
  event_name_.clear();
  while (!data.empty() && data.back() == '\n') data.pop_back();
  if (done_) return Fail("gateway sent data after stream completion", result, error);
  if (data == "[DONE]") {
    if (!final_ && !stopped_) return Fail("gateway omitted the successful final result", result, error);
    done_ = true;
    return true;
  }
  try {
    auto event = nlohmann::json::parse(data);
    if (!event.is_object()) return Fail("gateway SSE event is not an object", result, error);
    if (name == "agent.event") {
      nlohmann::json normalized;
      if (!NormalizeAgentEvent(event, &normalized))
        return Fail("invalid AgentEvent", result, error);
      if (on_agent_event_) on_agent_event_(normalized);
      const auto payload = event["data"];
      if (event["type"] == "error") {
        return Fail("Pi gateway reported an execution error", result, error);
      }
      if (event["type"] == "assistant_progress") {
        if (final_ || stopped_) return Fail("progress arrived after final answer", result, error);
        if (on_progress_) on_progress_(normalized["data"]["text"].get<std::string>());
        return true;
      }
      if (event["type"] == "assistant_delta") {
        event = {{"choices", nlohmann::json::array({{{"delta", {{"content", payload["text"]}}}}})}};
        name.clear();
      } else {
        result->agent_event = normalized;
        event = {{"final_response", payload["text"]},
                 {"response_metadata", payload.value("metadata", nlohmann::json::object())}};
        name = "agent.final";
      }
    }
    if (name == "error" || event.contains("error")) {
      return Fail("Pi gateway reported an execution error", result, error);
    }
    if (name == "pi.final" || name == "hermes.final" || name == "agent.final") {
      const std::string text = event.at("final_response").get<std::string>();
      if (text.empty() || text.size() > kMaxBytes) {
        return Fail("invalid gateway final answer size", result, error);
      }
      const auto metadata = event.value("response_metadata", nlohmann::json::object());
      if (!metadata.is_object()) return Fail("invalid gateway response metadata", result, error);
      if (final_ && (final_text_ != text || result->metadata != metadata)) {
        return Fail("conflicting gateway final answers", result, error);
      }
      final_ = true;
      final_text_ = text;
      result->text = text;
      result->metadata = metadata;
      const auto audit = metadata.find("length_audit");
      if (audit != metadata.end() && audit->is_object()) {
        result->finish_reason = audit->value("finish_reason", "");
      }
      result->citations = nlohmann::json::array();
      const auto it = metadata.find("citations");
      if (it != metadata.end() && it->is_array()) result->citations = *it;
      return true;
    }
    if (!event.contains("choices")) return true;  // Non-text progress event.
    if (!event["choices"].is_array()) return Fail("invalid gateway choices", result, error);
    if (event["choices"].empty()) return true;
    const auto& choice = event["choices"][0];
    if (!choice.is_object()) return Fail("invalid gateway choice", result, error);
    const std::string text = choice.contains("delta") && choice["delta"].is_object()
                                 ? Text(choice["delta"].value("content", choice["delta"].value("text", nlohmann::json()))) : "";
    if (!text.empty()) {
      if (final_ || stopped_) return Fail("gateway delta arrived after final answer", result, error);
      if (result->text.size() + text.size() > kMaxBytes) {
        return Fail("gateway answer exceeds limit", result, error);
      }
      result->text += text;
      ++result->stream_chunk_count;
      if (on_delta) on_delta(text);
    }
    if (choice.contains("finish_reason") && !choice["finish_reason"].is_null()) {
      if (!choice["finish_reason"].is_string()) {
        return Fail("gateway finish_reason has invalid type", result, error);
      }
      result->finish_reason = choice["finish_reason"].get<std::string>();
      if (result->finish_reason != "stop") {
        return Fail("gateway answer did not finish successfully", result, error);
      }
      stopped_ = true;
    }
    return true;
  } catch (const nlohmann::json::exception&) {
    return Fail("invalid gateway SSE JSON or field type", result, error);
  }
}

}  // namespace sparkpush
