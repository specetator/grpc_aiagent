#pragma once
#include "services/media_processing_service.h"
#include "mysql_pool.h"

namespace sparkpush {
class MySqlMediaProcessingRepository final : public MediaProcessingRepository {
 public:
    explicit MySqlMediaProcessingRepository(MySqlConnectionPool* pool) : pool_(pool) {}
    bool CheckSchema();
    ServiceStatus Claim(const std::string&, const std::string&, bool, nlohmann::json*, const std::string& parser_sha="") override;
    ServiceStatus Check(const MediaLease&, nlohmann::json*,bool require_live=true) override;
    ServiceStatus Current(const std::string&,const std::string&) override;
    ServiceStatus Heartbeat(const MediaLease&) override;
    ServiceStatus Stage(const MediaLease&, uint64_t, const nlohmann::json&, nlohmann::json*) override;
    ServiceStatus Complete(const MediaLease&, uint64_t, const std::string&, nlohmann::json*) override;
    ServiceStatus Fail(const MediaLease&, const std::string&) override;
    ServiceStatus Info(const std::string&, nlohmann::json*) override;
    ServiceStatus Fragments(const std::string&, const std::string&, uint64_t, uint64_t, nlohmann::json*) override;
    ServiceStatus Search(const std::string&, const std::string&, uint64_t, nlohmann::json*) override;
 private:
    MySqlConnectionPool* pool_;
};
}  // namespace sparkpush
