#include <stdexcept>
#include "conversation_store.h"
#include "services/message_service.h"
#include "services/user_service.h"
#include "spark_push.pb.h"
using namespace sparkpush;
void Require(bool v) { if (!v) throw std::runtime_error("message acceptance regression"); }
class Users final : public UserRepository {
 public:
 bool UsersAvailable() const override { return true; }
 bool TokensAvailable() const override { return true; }
 bool FindById(int64_t id, User* user, std::string*) override { user->id=id; return true; }
 bool FindByAccount(const std::string&,User*,std::string*) override { return false; }
 bool Create(const std::string&,const std::string&,const std::string&,int64_t*,std::string*) override { return false; }
 bool UpdatePassword(int64_t,const std::string&,std::string*) override { return false; }
 bool ResolveToken(const std::string& token,int64_t* id) override { *id=7; return token=="test-actor"; }
 bool SaveToken(const std::string&,int64_t,int) override { return false; }
 void Audit(int64_t,int64_t,const std::string&,const std::string&) override {}
};
class Reservations final : public MessageReservationRepository {
 public:
 Message first;
 int reserve_calls{0}, prepares{0}, publications{0};
 std::string outbox;
 bool Reserve(const Message& proposed, Message* message, bool* fresh, std::string*) override {
     *fresh = ++reserve_calls == 1;
     if (*fresh) { first = proposed; first.timestamp_ms=12345; first.msg_seq=42; first.msg_id="s_7_8-42"; }
     *message = first;
     return true;
 }
 bool PrepareOutbox(const Message&,const std::string& bytes,std::string*) override {
     ++prepares;
     if (!outbox.empty()) Require(outbox == bytes);
     outbox=bytes; return true;
 }
 bool Published(const Message&,std::string*) override { ++publications; return true; }
};
class Publisher final : public MessageEventPublisher {
 public:
 std::vector<std::string> attempts;
 bool Confirm(const std::string&,const std::string& bytes,int) override {
     attempts.push_back(bytes);
     return attempts.size() > 1; // First durable delivery is unconfirmed.
 }
};
int main() {
    Reservations reservations;
    ConversationStore store(nullptr,nullptr,nullptr,nullptr,&reservations);
    Users repository;
    UserService users(&repository,{});
    Publisher publisher;
    MessageService service(&store,nullptr,&users,nullptr,nullptr,nullptr,nullptr,nullptr,{},1000,
        nullptr,false,900000000001LL,{},nullptr,nullptr,&publisher);
    UpstreamCommand command{"single",7,8,0,R"({"type":"single_chat","content":{"text":"hello"}})","retry-id","","test-actor"};
    auto spoof = command; spoof.from_user_id=8;
    Require(service.Submit(spoof).status.code == 403 && reservations.reserve_calls == 0);
    auto failed = service.Submit(command);
    Require(!failed.accepted && failed.status.code == 503 && reservations.publications == 0);
    auto retry = service.Submit(command);
    Require(retry.status.ok() && retry.accepted && retry.message.timestamp_ms == 12345 && retry.message.msg_seq == 42);
    Require(publisher.attempts.size() == 2 && publisher.attempts[0] == publisher.attempts[1]);
    PersistMessageRequest event;
    Require(event.ParseFromString(publisher.attempts[1]) && event.message().timestamp_ms() == 12345);
    Require(reservations.publications == 1);
    auto malformed = command; malformed.content_json="[1,2]";
    Require(service.Submit(malformed).status.code == 400 && reservations.reserve_calls == 2);
}
