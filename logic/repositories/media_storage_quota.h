#pragma once
#include "repositories/room_sql.h"
#include "services/media_repository.h"

namespace sparkpush::media_storage {
// Logical output budget: 128 MiB images + 16 MiB text + bounded metadata and
// staging headroom. This is not a physical MySQL/filesystem capacity guarantee.
constexpr uint64_t kRevisionReservation = 176ULL * 1024 * 1024;
constexpr uint64_t kArtifactBytes = 128ULL * 1024 * 1024;
constexpr uint64_t kArtifactCount = 1024;
// Caller holds im_group FOR UPDATE and uses a current snapshot (READ COMMITTED
// for the next transaction). Never use a snapshot established before that lock.
inline uint64_t Charged(MYSQL* conn, int64_t room) {
    auto rows=room_sql::Query(conn,"SELECT COALESCE(SUM(charged_bytes),0) "
        "FROM room_media_revision_storage WHERE room_id=? AND state<>'reclaimed'",{std::to_string(room)});
    return std::stoull(rows[0][0]);
}
inline void NextTransactionCurrent(MYSQL* conn) {
    // One transaction only; do not mutate the pooled connection's session policy.
    room_sql::Query(conn,"SET TRANSACTION ISOLATION LEVEL READ COMMITTED");
}
}  // namespace sparkpush::media_storage
