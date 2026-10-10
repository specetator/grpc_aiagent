#pragma once

#include <string>

namespace sparkpush {

// Business errors, not HTTP status codes or grpc::Status. Adapters map these
// to the existing wire contract. The reason preserves legacy login behaviour
// without coupling services to HTTP response classes.
enum class ServiceReason { kNone, kInactiveAccount, kMembershipLookupFailed };
struct ServiceStatus {
    int code{0};
    std::string message{"ok"};
    ServiceReason reason{ServiceReason::kNone};
    bool ok() const { return code == 0; }
};

}  // namespace sparkpush
