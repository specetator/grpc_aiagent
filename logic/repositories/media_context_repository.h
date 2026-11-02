#pragma once
#include "services/media_context_service.h"
#include "mysql_pool.h"
namespace sparkpush {
class MySqlMediaContextRepository final : public MediaContextRepository {
 public:
    explicit MySqlMediaContextRepository(MySqlConnectionPool* pool):pool_(pool) {}
    ServiceStatus Messages(const MediaContextScope&,const std::string&,int64_t,int64_t,
        uint64_t,uint64_t,uint64_t,nlohmann::json*) override;
    ServiceStatus Record(const MediaContextScope&,const nlohmann::json&,nlohmann::json*) override;
    ServiceStatus Media(const MediaContextScope&,const std::string&,uint64_t,uint64_t,nlohmann::json*) override;
 private:
    MySqlConnectionPool* pool_;
};
}
