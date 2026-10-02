#pragma once

#include <cstdint>
#include <string>
#include "mysql_pool.h"
#include "spark_push.pb.h"

namespace sparkpush {

// SQL owns retries after Kafka has durably handed a message to Job.
class DeliveryOutbox {
public:
    struct Task {
        std::string msg_id;
        std::string lease_token;
        PersistMessageRequest request;
        int attempts{0};
    };
    explicit DeliveryOutbox(MySqlConnectionPool* pool) : pool_(pool) {}
    bool EnsureSchema(std::string* error);
    bool InsertTask(const PersistMessageRequest& request, std::string* error);
    // One durable COMMIT owns history, session metadata and its delivery task.
    bool PersistAtomically(const PersistMessageRequest& request, std::string* error);
    // Claims one task, never an entire batch whose leases could expire in queue.
    bool ClaimReady(int lease_ms, Task* task, bool* found, std::string* error);
    bool Renew(const Task& task, int lease_ms, std::string* error);
    bool Complete(const Task& task, std::string* error);
    bool Retry(const Task& task, int delay_ms, std::string* error);
    bool Backlog(int64_t* count, std::string* error);
private:
    MySqlConnectionPool* pool_;
};
}  // namespace sparkpush
