#pragma once

#include "hermes_client.h"

namespace sparkpush {

// Parses the gateway transport only. A non-empty preview is not a final answer.
class HermesSseParser {
 public:
  explicit HermesSseParser(
      std::function<void(const std::string&)> progress = {},
      std::function<void(const nlohmann::json&)> agent_event = {})
      : on_progress_(std::move(progress)),
        on_agent_event_(std::move(agent_event)) {}
  bool Feed(const std::string& chunk, HermesChatResult* result,
            const std::function<void(const std::string&)>& on_delta,
            std::string* error);
  bool Finish(HermesChatResult* result,
              const std::function<void(const std::string&)>& on_delta,
              std::string* error);

 private:
  bool ProcessEvent(HermesChatResult* result,
                    const std::function<void(const std::string&)>& on_delta,
                    std::string* error);
  bool Fail(const std::string& message, HermesChatResult* result,
            std::string* error);

  std::function<void(const std::string&)> on_progress_;
  std::function<void(const nlohmann::json&)> on_agent_event_;
  std::string pending_;
  std::string event_data_;
  std::string event_name_;
  std::string failure_;
  std::string final_text_;
  bool final_{false};
  bool stopped_{false};
  bool done_{false};
};

}  // namespace sparkpush
