#pragma once
#include <mysql/mysql.h>
#include <nlohmann/json.hpp>
#include "services/service_status.h"

namespace sparkpush::room_context_sql {
// Shared by member source lookup and task creation. Caller holds im_group
// FOR UPDATE in a current transaction. References come only from the receipt
// table, never from the client. Implementation reuses the context read checks.
ServiceStatus Resolve(MYSQL*,int64_t room,const std::string& request_id,
    const nlohmann::json& source_ids,const std::string& after,uint64_t limit,nlohmann::json*);
}
