#include "sse_parser.h"

#include <iostream>
#include <string>

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
    const std::string stream =
      "event: agent.event\ndata: {\"schema\":\"sparkpush.agent_event.v1\",\"type\":\"assistant_delta\",\"data\":{\"text\":\"preview\"}}\n\n"
      "event: agent.event\ndata: {\"schema\":\"sparkpush.agent_event.v1\",\"type\":\"assistant_final\",\"data\":{\"text\":\"final\",\"metadata\":{\"model_state\":{\"override\":false}}}}\n\n"
      "data: [DONE]\n\n";
    if (!Check(parser.Feed(stream, &result, [&](const std::string& s) { preview += s; }, &error) &&
               parser.Finish(&result, {}, &error) && preview == "preview" && result.text == "final" &&
               result.metadata.contains("model_state"), "generic AgentEvent stream failed")) return 1;
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
  if (!Reject("data: " + std::string(16 * 1024 * 1024, 'x'))) return 1;
  std::cout << "hermes SSE parser tests passed\n";
}
