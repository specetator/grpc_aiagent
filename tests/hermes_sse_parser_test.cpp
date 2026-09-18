#include "sse_parser.h"

#include <iostream>
#include <string>
#include <vector>

namespace {
bool Check(bool value, const char* message) {
  if (!value) std::cerr << "FAIL: " << message << '\n';
  return value;
}
const std::string kDelta = "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n";
const std::string kFinal = "event: pi.final\ndata: {\"final_response\":\"最终答案🙂\",\"response_metadata\":{\"citations\":[{\"id\":\"ref\"}]}}\n\n";
const std::string kDone = "data: [DONE]\n\n";

bool Reject(const std::string& stream) {
  sparkpush::HermesSseParser parser;
  sparkpush::HermesChatResult result;
  std::string error;
  const bool fed = parser.Feed(stream, &result, {}, &error);
  const bool finished = parser.Finish(&result, {}, &error);
  if (!Check((!fed || !finished) && !error.empty() && result.text.empty(), "invalid stream accepted")) return false;
  // A later DONE must never turn an earlier error into success.
  return Check(!parser.Feed(kFinal + kDone, &result, {}, &error), "failure was not latched");
}
}  // namespace

int main() {
  {
    std::string status, error, delta;
    sparkpush::HermesSseParser parser([&](const std::string& s) {status = s;});
    sparkpush::HermesChatResult result;
    const std::string progress = "event: agent.event\ndata: {\"schema\":\"sparkpush.agent_event.v1\",\"type\":\"assistant_progress\",\"data\":{\"text\":\"waiting\"}}\n\n";
    if (!Check(parser.Feed(progress + kDelta + kFinal + kDone, &result,
                          [&](const std::string& s) {delta += s;}, &error) &&
               parser.Finish(&result, {}, &error) && status == "waiting" && delta == "partial" &&
               result.text == "最终答案🙂", "progress contaminated answer")) return 1;
    if (!Reject(kFinal + progress + kDone)) return 1;
  }
  {
    sparkpush::HermesSseParser parser;
    sparkpush::HermesChatResult result;
    std::string preview, error;
    std::vector<int64_t> sequences;
    parser = sparkpush::HermesSseParser({}, [&](const nlohmann::json& event) {
      if (event.contains("envelope"))
        sequences.push_back(event["envelope"]["sequence"].get<int64_t>());
    });
    const std::string stream =
      "event: agent.event\ndata: {\"schema\":\"sparkpush.agent_event.v1\",\"type\":\"assistant_delta\",\"data\":{\"text\":\"preview\"},\"envelope\":{\"schema\":\"sparkpush.agent_envelope.v1\",\"event_id\":\"evt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"request_id\":\"request-1\",\"route_key\":\"rt_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"session_key\":\"agent:pi:spark_pc:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"tenant_id\":\"local\",\"channel_id\":\"spark_pc\",\"conversation_id\":\"s_7_99\",\"thread_id\":\"_\",\"agent_id\":\"pi\",\"sequence\":0,\"created_at_ms\":1750000000000,\"replayable\":true,\"terminal\":false,\"replayed\":false}}\n\n"
      "event: agent.event\ndata: {\"schema\":\"sparkpush.agent_event.v1\",\"type\":\"assistant_final\",\"data\":{\"text\":\"final\",\"metadata\":{\"model_state\":{\"override\":false}}},\"envelope\":{\"schema\":\"sparkpush.agent_envelope.v1\",\"event_id\":\"evt_cccccccccccccccccccccccccccccccc\",\"request_id\":\"request-1\",\"route_key\":\"rt_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"session_key\":\"agent:pi:spark_pc:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"tenant_id\":\"local\",\"channel_id\":\"spark_pc\",\"conversation_id\":\"s_7_99\",\"thread_id\":\"_\",\"agent_id\":\"pi\",\"sequence\":1,\"created_at_ms\":1750000000001,\"replayable\":true,\"terminal\":true,\"replayed\":false}}\n\n"
      "data: [DONE]\n\n";
    if (!Check(parser.Feed(stream, &result, [&](const std::string& s) { preview += s; }, &error) &&
               parser.Finish(&result, {}, &error) && preview == "preview" && result.text == "final" &&
               result.metadata.contains("model_state") && sequences == std::vector<int64_t>({0, 1}) &&
               result.agent_event["envelope"]["terminal"] == true,
               "generic AgentEvent envelope stream failed")) return 1;
  }
  using sparkpush::HermesSseParser;
  using sparkpush::HermesChatResult;
  // Split at every byte, including the middle of UTF-8 and SSE field names.
  HermesSseParser parser;
  HermesChatResult result;
  std::string preview, error;
  std::string alias = kFinal;
  alias.replace(alias.find("pi.final"), 8, "hermes.final");
  for (const char byte : kDelta + kFinal + alias + kDone) {
    if (!Check(parser.Feed(std::string(1, byte), &result,
                           [&](const std::string& s) { preview += s; }, &error), "fragmented stream failed")) return 1;
  }
  if (!Check(parser.Finish(&result, {}, &error), "valid final rejected") ||
      !Check(result.text == "最终答案🙂", "final did not replace preview") ||
      !Check(preview == "partial", "preview callback changed") ||
      !Check(result.citations.size() == 1, "citations lost")) return 1;

  for (const auto& invalid : {
           kDelta,
           kDelta + kDone,
           kDelta + "data: {\"error\":{\"message\":\"failure\"}}\n\n",
           kDelta + "event: error\ndata: {}\n\n",
           kDelta + "data: {\"choices\":[{\"finish_reason\":\"length\"}]}\n\n" + kDone,
           kDelta + "data: []\n\n",
           kDelta + "data: {\"choices\":123}\n\n",
           kDelta + "event: pi.final\ndata: {\"final_response\":123}\n\n" + kDone,
           kDelta + kFinal,
           kFinal + kDelta + kDone,
           kFinal + "event: agent.final\ndata: {\"final_response\":\"other\"}\n\n" + kDone,
           kFinal + "event: hermes.final\ndata: {\"final_response\":\"最终答案🙂\",\"response_metadata\":{}}\n\n" + kDone,
           kFinal + kDone + "data: {\"error\":\"late error\"}\n\n"}) {
    if (!Reject(invalid)) return 1;
  }

  HermesSseParser standard;
  HermesChatResult standard_result;
  const std::string stream =
      ": heartbeat\r\n\r\ndata: {\"choices\":\r\ndata: [{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\r\n\r\n"
      "data: [DONE]";
  if (!Check(standard.Feed(stream, &standard_result, {}, &error) &&
             standard.Finish(&standard_result, {}, &error) && standard_result.text == "ok",
             "standard stop/DONE or multiline CRLF failed")) return 1;
  {
    // Regression guard for long UTF-8 answers: the stream and final event are
    // well above the old 64 KiB database/read-buffer boundary.
    std::string long_text;
    long_text.reserve(100000);
    for (int i = 0; i < 10000; ++i) long_text += u8"\u4E2D\u6587\U0001F642";
    std::string long_stream;
    std::string long_preview;
    constexpr std::size_t kChunkBytes = 1000;  // ends on a UTF-8 boundary
    for (std::size_t offset = 0; offset < long_text.size(); offset += kChunkBytes) {
      const std::string chunk = long_text.substr(offset, kChunkBytes);
      long_stream += "data: {\"choices\":[{\"delta\":{\"content\":\"" +
                     chunk + "\"}}]}\n\n";
    }
    long_stream += "event: pi.final\ndata: {\"final_response\":\"" +
                   long_text +
                   "\",\"response_metadata\":{\"length_audit\":{\"finish_reason\":\"stop\"}}}\n\n"
                   "data: [DONE]\n\n";
    HermesSseParser long_parser;
    HermesChatResult long_result;
    std::string long_error;
    for (std::size_t offset = 0; offset < long_stream.size(); offset += 113) {
      if (!Check(long_parser.Feed(long_stream.substr(offset, 113), &long_result,
                                  [&](const std::string& value) { long_preview += value; },
                                  &long_error),
                 "long UTF-8 stream rejected")) return 1;
    }
    if (!Check(long_parser.Finish(&long_result, {}, &long_error),
               "long UTF-8 stream did not finish") ||
        !Check(long_result.text == long_text, "long final text was shortened") ||
        !Check(long_preview == long_text, "long delta text was shortened") ||
        !Check(long_result.stream_chunk_count == long_text.size() / kChunkBytes,
               "long stream chunk count changed") ||
        !Check(long_result.finish_reason == "stop", "long finish reason lost")) return 1;
  }
  if (!Reject("data: " + std::string(16 * 1024 * 1024, 'x'))) return 1;
  std::cout << "hermes SSE parser tests passed\n";
}
