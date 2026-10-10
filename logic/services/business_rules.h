#pragma once

#include "model.h"
#include "services/service_status.h"

namespace sparkpush {
// Actor IDs are resolved by trusted adapters, never copied from client JSON.
inline ServiceStatus RequireAdministrator(int64_t actor) {
    return actor == kAdminUserId ? ServiceStatus{}
                                 : ServiceStatus{403, "administrator required"};
}
}  // namespace sparkpush
