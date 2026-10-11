#pragma once
#include "model.h"
#include "mysql_pool.h"
namespace sparkpush {
// The durable authority survives Redis loss. Only the Kafka delivery report is an accepted ACK.
class MessageReservationRepository {
 public:
    virtual ~MessageReservationRepository() = default;
    virtual bool Reserve(const Message& proposed, Message* canonical, bool* fresh, std::string* error) = 0;
    virtual bool PrepareOutbox(const Message&, const std::string& payload, std::string* error) = 0;
    virtual bool Published(const Message&, std::string* error) = 0;
};
class MySqlMessageReservations final : public MessageReservationRepository {
 public:
    explicit MySqlMessageReservations(MySqlConnectionPool* pool) : pool_(pool) {}
    bool Reserve(const Message&, Message*, bool*, std::string*) override;
    bool CheckSchema(std::string* error);
    bool PrepareOutbox(const Message&, const std::string& payload, std::string* error) override;
    bool Published(const Message&, std::string* error) override;
    bool Persisted(const Message&, std::string* error);
    bool Pending(std::vector<std::pair<Message, std::string>>*, int limit, std::string* error);
    static bool SafePrefix(MYSQL*, const std::string& session, int64_t* seq, std::string* error);
 private:
    MySqlConnectionPool* pool_;
};
}  // namespace sparkpush
