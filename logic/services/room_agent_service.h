#pragma once
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>
#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
// A repository owns persistence, not permissions or use-case rules.
class RoomAgentRepository {
 public:
    virtual ~RoomAgentRepository() = default;
    virtual bool Access(int64_t room, int64_t user, int64_t* owner, bool* member,
                        std::string* error) = 0;
    // Missing rows return true + null; database failures return false.
    virtual bool Load(int64_t room, nlohmann::json* task, std::string* error) = 0;
    virtual bool Save(int64_t room, int64_t expected_revision,
                      const nlohmann::json& task, std::string* error) = 0;
    virtual bool Request(const std::string& id, nlohmann::json* request,
                         std::string* error) = 0;
    virtual bool PutRequest(const nlohmann::json& request, std::string* error) = 0;
    virtual bool Finish(const std::string& id, const nlohmann::json& result,
                        std::string* error) = 0;
    virtual bool Requests(int64_t room, nlohmann::json* requests,
                          std::string* error) = 0;
};

// Durable operator progress is separate from the Agent transcript.
class OperatorTaskService {
 public:
    explicit OperatorTaskService(RoomAgentRepository* repo) : repo_(repo) {}
    ServiceStatus Checkpoint(int64_t actor, int64_t room, int64_t revision,
                            const nlohmann::json& patch, nlohmann::json* result);
 private:
    RoomAgentRepository* repo_;
};

class RoomAgentService {
 public:
    RoomAgentService(RoomAgentRepository* repo, int64_t public_cann_bot,
                     bool enabled) : repo_(repo), bot_(public_cann_bot), enabled_(enabled) {}
    ServiceStatus Configure(int64_t actor, int64_t room, const nlohmann::json& input,
                            nlohmann::json* result);
    ServiceStatus View(int64_t actor, int64_t room, nlohmann::json* result);
    ServiceStatus CheckMember(int64_t actor, int64_t room);
    ServiceStatus Trigger(int64_t actor, int64_t room, const std::string& content,
                          bool* trigger);
    bool BuildRequest(const Message& current, const std::vector<Message>& history,
                      nlohmann::json* request, std::string* error);
    bool ValidateReply(const nlohmann::json& reply, nlohmann::json* request,
                       std::string* error);
    bool Complete(const nlohmann::json& reply, std::string* error);
    // Single Logic instance orders room append -> persistence -> AI enqueue.
    std::mutex& SubmissionMutex() { return submission_mu_; }
 private:
    RoomAgentRepository* repo_;
    int64_t bot_;
    bool enabled_;
    std::mutex submission_mu_;
};
}  // namespace sparkpush
