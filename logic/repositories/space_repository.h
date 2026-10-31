#pragma once
#include "mysql_pool.h"
#include "services/space_repository.h"
namespace sparkpush {
class MySqlSpaceRepository final : public SpaceRepository {
 public:
    explicit MySqlSpaceRepository(MySqlConnectionPool* pool) : pool_(pool) {}
    bool CheckSchema(std::string* error);
    ServiceStatus Create(int64_t, const std::string&, const std::string&, const std::string&, int64_t*) override;
    ServiceStatus WithRoom(int64_t, const std::function<ServiceStatus(SpaceTransaction&)>&) override;
    ServiceStatus List(int64_t, int, int, std::vector<SpaceSummary>*) override;
    ServiceStatus Invitations(int64_t, std::vector<SpaceInvitation>*) override;
 private:
    MySqlConnectionPool* pool_;
};
}
