#include <cstdlib>
#include <iostream>
#include <thread>
#include <future>
#include <chrono>
#include <stdexcept>
#include <fstream>
#include "repositories/space_repository.h"
#include "repositories/room_agent_repository.h"
#include "repositories/room_sql.h"
#include "services/room_service.h"
#include "hermes_reply.h"
#include "message_reservation.h"
#include "agent_event.h"
#include "spark_push.pb.h"
#include <openssl/sha.h>
using namespace sparkpush;
void Require(bool ok,const std::string& why) { if(!ok) throw std::runtime_error(why); }
// Pause only after the real Service policy evaluated under the real Repository
// locks. The hook never changes the policy decision or substitutes SQL behavior.
class GatedPublicationRepository : public MySqlRoomAgentRepository {
 public:
    using MySqlRoomAgentRepository::MySqlRoomAgentRepository;
    std::promise<void> decided,release;
    bool Publication(const nlohmann::json& request,const std::string& digest,
        const PublicationMatch& match,const PublicationDecision& decide,nlohmann::json* out,
        std::string* error,bool* existing=nullptr)override {
        auto resume=release.get_future();
        return MySqlRoomAgentRepository::Publication(request,digest,match,[&](const nlohmann::json& state){
            auto result=decide(state);decided.set_value();resume.wait();return result;
        },out,error,existing);
    }
};
bool WaitForRoomLock(MySqlConnectionPool& pool,int64_t room) {
    const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    while(std::chrono::steady_clock::now()<deadline) {
        auto guard=pool.Acquire();
        const auto rows=room_sql::Query(guard.get(),
            "SELECT 1 FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l "
            "ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID AND l.ENGINE=w.ENGINE "
            "WHERE l.OBJECT_SCHEMA=DATABASE() AND l.OBJECT_NAME='im_group' AND l.LOCK_DATA=?",
            {std::to_string(room)});
        if(!rows.empty())return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}
int main() {
    if(!std::getenv("SPARK_PUSH_RUN_MYSQL_TESTS")) return 77;
    MySqlConfig config;config.host="127.0.0.1";config.user="root";config.db="spark_review_test";
    config.port=std::stoi(std::getenv("SPARK_TEST_MYSQL_PORT"));config.password=std::getenv("SPARK_TEST_MYSQL_PASSWORD");
    MySqlConnectionPool pool;Require(pool.Init(config),"pool init");
    int64_t a,b,c;
    {
        auto guard=pool.Acquire();
        for(auto* id:{&a,&b,&c}) {
            room_sql::Query(guard.get(),"INSERT INTO user(account,password_hash) VALUES(UUID(),'test')");*id=mysql_insert_id(guard.get());
        }
    }
    MySqlSpaceRepository repo(&pool);GroupDao groups(&pool);GroupMemberDao members(&pool);
    RoomService service(nullptr,&groups,&members,nullptr,&repo);
    std::string error;Require(repo.CheckSchema(&error),error);int64_t room=0;
    const std::string name="O'Reilly \\ '; DROP TABLE im_group; -- 空间";
    auto created=service.CreateSpace(a,name,"描述 ' \\ \"","private",&room);
    Require(created.ok(),"create private: "+created.message+" actor="+std::to_string(a));
    ImGroup g;Require(groups.GetGroup(room,&g,&error) && g.name==name && g.visibility=="private","bound name roundtrip");
    Require(service.CheckMember(a,room).ok() && service.CheckMember(b,room).code==403,"authoritative membership");
    Require(service.Invite(a,room,b).ok() && service.Join(b,b,room).code==403,"pending invite no access");
    Require(service.Accept(b,room).ok() && service.ChangeRole(a,room,b,1).ok(),"accept and promote");
    Require(service.Join(b,b,room).ok(),"idempotent join");int role=-1;
    Require(members.GetRole(room,b,&role,&error) && role==1,"join preserves admin");
    Require(members.AddOrUpdateMember(room,b,0,&error) && members.GetRole(room,b,&role,&error) && role==0,"DAO actually updates role");
    Require(service.Remove(a,room,b).ok() && service.CheckMember(b,room).code==403 && service.Accept(b,room).code==403,"removal invalidates old invite");
    Require(service.Invite(a,room,b).ok(),"reinvite");
    ServiceStatus accept,remove;
    std::thread t1([&]{accept=service.Accept(b,room);});
    std::thread t2([&]{remove=service.Remove(a,room,b);});t1.join();t2.join();
    Require(remove.ok() && (accept.ok() || accept.code==403) && service.CheckMember(b,room).code==403,"accept/remove serialized");
    int64_t public_room=0;Require(groups.CreateChatroom(name,a,&public_room,&error),error);
    Require(service.Join(c,c,public_room).ok(),"legacy public creation and join");
    std::vector<ImGroup> directory;
    Require(service.DiscoverSpaces(b,0,100,&directory).ok(),"public directory available to authenticated nonmember");
    bool public_found=false;
    for(const auto& found:directory) {
        Require(found.id!=room && found.visibility=="public","private space never disclosed by directory");
        public_found=public_found || found.id==public_room;
    }
    Require(public_found,"legacy public room discoverable");
    Require(service.DiscoverSpaces(b,0,1,&directory).ok() && directory.size()==1 && directory[0].id==public_room,"directory deterministic pagination");
    Require(service.DiscoverSpaces(0,0,100,&directory).code==403,"directory authentication required");
    // Prepared statements must work even when backslash escaping is disabled.
    {
        auto guard=pool.Acquire();room_sql::Query(guard.get(),"SET SESSION sql_mode='STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES'");
        auto rows=room_sql::Query(guard.get(),"SELECT ?",{name});Require(rows[0][0]==name,"SQL mode independent binding");
    }
    MySqlRoomAgentRepository agent_repo(&pool,&groups,&members);RoomAgentService agent(&agent_repo,900000000001LL,true);
    nlohmann::json task;
    auto status=agent.Configure(a,room,{{"title",name},{"spec","keep ' workspace \\"},{"developer_user_ids",nlohmann::json::array()}},&task);
    Require(status.ok(),"configure SQL room Agent: "+status.message);
    const auto id=task["task_id"];
    Require(service.Invite(a,room,b).ok() && service.Accept(b,room).ok() && service.ChangeRole(a,room,b,1).ok(),"admin setup");
    Require(agent.Configure(b,room,{{"revision",1},{"developer_user_ids",nlohmann::json::array()}},&task).ok() && task["task_id"]==id && task["title"]==name,"admin configure preserves workspace");
    nlohmann::json request={{"request_id","request-'\\"},{"user_id",a},{"message_seq",1},{"collaboration",{{"room_id",room}}},{"messages",nlohmann::json::array({name})}};
    Require(agent_repo.PutRequest(request,&error),error);nlohmann::json loaded;
    Require(agent_repo.Request(request["request_id"],&loaded,&error) && loaded==request,"Agent request parameter binding");
    Require(agent_repo.Finish(request["request_id"],{{"ok",true},{"text",name}},&error),error);
    Require(agent_repo.Finish(request["request_id"],{{"ok",true},{"text",name},{"completed_at_ms",123}},&error),"Bridge redelivery timestamp does not poison first result");
    Require(!agent_repo.Finish(request["request_id"],{{"ok",true},{"text","conflicting result"}},&error),"semantic result conflict rejected");
    Require(agent_repo.Requests(room,&loaded,&error) && loaded.size()==1,"Agent result query");
    Require(service.Remove(a,room,b).ok() && agent.View(b,room,&loaded).code==403,"removed admin loses task access");
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"ALTER TABLE room_agent_dispatch MODIFY session_id VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL");}
    Require(agent_repo.CheckCatalogSchema(&error),"generic schema check: "+error);
    RoomAgentService multi(&agent_repo,900000000001LL,true,{{"public-pi","Pi","pi_rpc",900000000401LL,true},{"second-pi","Pi","pi_rpc",900000000402LL,true}});
    Require(multi.Install(a,room,{{"agent_id","public-pi"}},true).ok(),"install public Pi");
    Require(multi.Install(a,room,{{"agent_id","second-pi"}},true).ok(),"install second Pi");
    multi.SetProjectCatalog({{"spark-push","Quotes ' and SQL ; --"}},std::string(48,'k'));
    Require(multi.SetWorkspace(a,room,{{"project_id","spark-push"},{"generation",0}},&loaded).ok(),"persist owner workspace");
    Require(loaded["workspace"]["generation"]==1,"workspace initial generation");
    Require(multi.SetWorkspace(a,room,{{"project_id",""},{"generation",1}},&loaded).ok(),"workspace revoke tombstone");
    Require(multi.SetWorkspace(a,room,{{"project_id","spark-push"},{"generation",2}},&loaded).ok(),"workspace regrant");
    Require(loaded["workspace"]["generation"]==3,"workspace generation never resets");
    Require(multi.SetWorkspace(a,room,{{"project_id",""},{"generation",1}},&loaded).code==409,"stale workspace update cannot revoke new grant");
    Message source;source.session_id="r_"+std::to_string(room);source.msg_seq=20;source.msg_id=source.session_id+"-20";source.sender_id=a;source.client_msg_id="multi-source";
    source.content_json=nlohmann::json({{"content",{{"text","summarize O'Reilly \\"}}},{"agent_mentions",nlohmann::json::array({"public-pi","second-pi"})}}).dump();
    nlohmann::json batch,pending;
    Require(multi.BuildRequests(source,{},&batch,&error)&&batch.size()==2,"persist all request intents");
    Require(agent_repo.Pending(&pending,&error)&&pending.empty(),"unaccepted source cannot dispatch");
    {
        auto guard=pool.Acquire();
        room_sql::Query(guard.get(),"INSERT INTO message_reservation(session_id,msg_seq,sender_id,client_msg_id,original_message,stage) VALUES(?,?,?,?,?,1)",{source.session_id,"20",std::to_string(a),source.client_msg_id,"{}"});
    }
    Require(agent_repo.Pending(&pending,&error)&&pending.empty(),"prepared source cannot dispatch");
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE message_reservation SET stage=2 WHERE session_id=? AND msg_seq=20",{source.session_id});}
    Require(agent_repo.Pending(&pending,&error)&&pending.size()==2,"broker-confirmed source recovers both requests");
    Require(agent_repo.MarkAttempt(batch[0]["request_id"],&error)&&agent_repo.Dispatched(batch[0]["request_id"],&error),"confirmed independent publish");
    Require(agent_repo.Pending(&pending,&error)&&pending.size()==1,"partial publish recovers remaining intent");
    Require(agent_repo.RecoverDispatch(batch[0]["request_id"],&error),"manual recovery requeues exact immutable request");
    Require(agent_repo.Pending(&pending,&error)&&pending.size()==2,"manual recovery is visible to existing dispatcher");
    Require(agent_repo.Dispatched(batch[0]["request_id"],&error)&&agent_repo.Pending(&pending,&error)&&pending.size()==1,"recover uses same request");
    Require(multi.Install(a,room,{{"agent_id","second-pi"}},false).ok(),"uninstall invalidates pending");
    Require(multi.CheckDispatch(pending[0]).code==403,"pending rechecks binding");
    Require(multi.RejectPending(pending[0],"binding revoked",&error),"visible cancellation");
    Require(agent_repo.Pending(&pending,&error)&&pending.empty(),"cancelled intent not retried");
    Require(multi.Install(a,room,{{"agent_id","second-pi"}},true).ok()&&multi.CheckDispatch(batch[1]).code==403,"reinstall cannot revive old generation");
    Require(multi.Agents(a,room,&loaded).ok(),"catalog and request query");
    Require(multi.AuthorizeWorkspace(std::string(48,'k'),{{"request_id",batch[0]["request_id"]}},&loaded).ok(),"real workspace authority reads immutable request");
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE `user` SET status=2 WHERE id=?",{std::to_string(a)});}
    Require(multi.AuthorizeWorkspace(std::string(48,'k'),{{"request_id",batch[0]["request_id"]}},&loaded).code==403,"disabled actor loses live file authorization");
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE `user` SET status=1 WHERE id=?",{std::to_string(a)});}

    using Json=nlohmann::json;
    auto finalReply=[](const Json& request){
        Json reply;
        for(const auto* key:{"request_id","session_id","user_id","bot_user_id","collaboration"})reply[key]=request.at(key);
        reply["ok"]=true;reply["text"]="private code ' result";reply["completed_at_ms"]=123456;
        reply["delta_count"]=2;
        reply["response_metadata"]["agent_event"]={{"schema","sparkpush.agent_event.v1"},{"type","assistant_final"},
            {"data",{{"text",reply["text"]}}},{"envelope",{{"schema","sparkpush.agent_envelope.v1"},
            {"request_id",request["request_id"]},{"event_id",ProjectedEventId(request["request_id"],2)},
            {"route_key","rt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"session_key","room-public"},
            {"tenant_id","local"},{"channel_id","spark_pc"},{"conversation_id",request["session_id"]},
            {"thread_id","_"},{"agent_id",request["agent_id"]},{"sequence",2},{"created_at_ms",100},
            {"replayable",true},{"terminal",true},{"replayed",false}}}};
        return reply;
    };
    auto final=finalReply(batch[0]);PreparedHermesReply original;
    Require(PrepareHermesReply(final.dump(),900000000401LL,"Pi",&original,&error,&batch[0]),"prepare workspace final");
    Json canonical,concurrent;bool publish1=false,publish2=false;std::string err1,err2;
    std::thread p1([&]{publish1=multi.PublishReply(batch[0],final,original.message.content_json,&canonical,&err1);});
    std::thread p2([&]{publish2=multi.PublishReply(batch[0],final,original.message.content_json,&concurrent,&err2);});
    p1.join();p2.join();Require(publish1 && publish2 && canonical==final && concurrent==canonical,"concurrent final decisions have one canonical reply");
    // Simulate a crash after admission and before outbox/Kafka: a fresh service
    // must recover the original decision even after the workspace is revoked.
    Require(multi.SetWorkspace(a,room,{{"project_id",""},{"generation",3}},&loaded).ok(),"revoke before redelivery");
    MySqlRoomAgentRepository restarted_repo(&pool,&groups,&members);
    RoomAgentService restarted(&restarted_repo,900000000001LL,true,{});
    final["completed_at_ms"]=999999;
    final["delta_count"]=7;
    auto& envelope=final["response_metadata"]["agent_event"]["envelope"];
    envelope["sequence"]=7;envelope["event_id"]=ProjectedEventId(final["request_id"],7);envelope["replayed"]=true;
    PreparedHermesReply replayPrepared;
    Require(PrepareHermesReply(final.dump(),900000000401LL,"Pi",&replayPrepared,&error,&batch[0]),"prepare retransmitted final");
    Require(restarted.PublishReply(batch[0],final,replayPrepared.message.content_json,&concurrent,&error) && concurrent==canonical,"restart and transport changes retain admitted bytes despite revocation");
    // Emulate an existing pre-v2 row in this disposable database only.
    const Json oldResult={{"ok",canonical.at("ok")},{"text",canonical.at("text")}};
    const auto oldInput=Json{{"message",original.message.content_json},{"result",oldResult}}.dump();
    unsigned char hash[SHA256_DIGEST_LENGTH];SHA256(reinterpret_cast<const unsigned char*>(oldInput.data()),oldInput.size(),hash);
    std::string oldDigest;const char* hex="0123456789abcdef";
    for(auto byte:hash){oldDigest+=hex[byte>>4];oldDigest+=hex[byte&15];}
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE room_agent_publication SET input_digest=? WHERE request_id=?",{oldDigest,batch[0]["request_id"]});}
    Require(restarted.PublishReply(batch[0],final,replayPrepared.message.content_json,&concurrent,&error) && concurrent==canonical,"real legacy row returns immutable original on transport replay");
    {auto guard=pool.Acquire();const auto rows=room_sql::Query(guard.get(),"SELECT input_digest FROM room_agent_publication WHERE request_id=?",{batch[0]["request_id"]});
     Require(rows.at(0).at(0)==oldDigest,"legacy digest is never rewritten");}
    Require(!restarted.PublishReply(batch[0],final,"different canonical content",&concurrent,&error),"semantic replay conflict rejected");
    auto revokedFinal=finalReply(batch[1]);Json denied;
    Require(multi.PublishReply(batch[1],revokedFinal,"original rejected body",&denied,&error) && denied["terminal_status"]=="cancelled" && denied["ok"]==false && denied.dump().find("private code")==std::string::npos,"revoked unpublished final never retains private result");
    {auto guard=pool.Acquire();const auto rows=room_sql::Query(guard.get(),"SELECT reply FROM room_agent_publication WHERE request_id=?",{batch[1]["request_id"]});
     Require(rows.size()==1 && rows[0][0].find("private code")==std::string::npos,"denied ledger stores safe error only");}
    Require(multi.SetWorkspace(a,room,{{"project_id","spark-push"},{"generation",4}},&loaded).ok(),"regrant after cancellation");
    Require(multi.PublishReply(batch[1],revokedFinal,"original rejected body",&concurrent,&error) && concurrent==denied,"regrant cannot rewrite cancellation");

    // Context epoch uses real SQL; no model/broker execution is claimed here.
    source.msg_seq=250;source.msg_id=source.session_id+"-250";
    Json epochRequests;
    Require(multi.BuildRequests(source,{},&epochRequests,&error),"new requests persist explicit context generation");
    Require(epochRequests[0]["collaboration"]["context_generation"]==0,"missing context row is original epoch");
    auto legacyEpoch=epochRequests[1];legacyEpoch["collaboration"].erase("context_generation");
    Require(multi.CheckDispatch(legacyEpoch).ok(),"persisted workspace request without epoch is zero");
    for(const auto* field:{"workspace_generation","project_id"})legacyEpoch["collaboration"].erase(field);
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE room_agent_request SET payload=? WHERE request_id=?",
        {legacyEpoch.dump(),legacyEpoch["request_id"]});}
    {
        auto guard=pool.Acquire();room_sql::Transaction transaction(guard.get());
        room_sql::Query(guard.get(),"SELECT id FROM im_group WHERE id=? FOR UPDATE",{std::to_string(room)});
        room_sql::Query(guard.get(),"INSERT INTO room_context(room_id,generation) VALUES(?,1)",{std::to_string(room)});
        transaction.Commit();
    }
    Require(multi.CheckDispatch(epochRequests[0]).code==403 && multi.CheckDispatch(legacyEpoch).code==403,"epoch advance fences explicit and legacy scopes");
    Require(multi.AuthorizeWorkspace(std::string(48,'k'),{{"request_id",epochRequests[0]["request_id"]}},&loaded).code==403,"canonical connector authorization rejects old epoch");
    Json epochReplay;Require(multi.BuildRequests(source,{},&epochReplay,&error) && epochReplay[0]==epochRequests[0] && epochReplay[1]==legacyEpoch,"stored request replay never upgrades epoch");
    auto epochFinal=finalReply(epochRequests[0]);
    Require(multi.PublishReply(epochRequests[0],epochFinal,"epoch protected body",&denied,&error) && denied["terminal_status"]=="cancelled" &&
        denied.dump().find("private code")==std::string::npos,"first stale publication persists only cancellation");
    auto earliestFinal=finalReply(legacyEpoch);
    Require(multi.PublishReply(legacyEpoch,earliestFinal,"pre-workspace body",&denied,&error) && denied["terminal_status"]=="cancelled",
        "pre-workspace generic first publication cannot bypass context fence");
    Require(restarted.PublishReply(batch[0],final,replayPrepared.message.content_json,&concurrent,&error) && concurrent==canonical,"previous committed publication survives context advance");
    source.msg_seq=251;source.msg_id=source.session_id+"-251";
    Json nextEpoch;Require(multi.BuildRequests(source,{},&nextEpoch,&error) && nextEpoch[0]["collaboration"]["context_generation"]==1,"new request captures new epoch");
    Require(multi.CheckDispatch(nextEpoch[0]).ok(),"current epoch dispatch remains available");
    // Missing migration/query failure cannot silently grant epoch zero. Existing
    // publication decisions do not need the context table to recover.
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"RENAME TABLE room_context TO room_context_unavailable");}
    Require(multi.CheckDispatch(nextEpoch[0]).code==503,"missing context schema is retryable unavailable");
    source.msg_seq=252;source.msg_id=source.session_id+"-252";
    Require(!multi.BuildRequests(source,{},&epochReplay,&error),"new request denied when context cannot be read");
    Require(restarted.PublishReply(batch[0],final,replayPrepared.message.content_json,&concurrent,&error) && concurrent==canonical,"committed publication independent of missing context schema");
    auto unavailableFinal=finalReply(nextEpoch[0]);
    Require(!multi.PublishReply(nextEpoch[0],unavailableFinal,"unavailable context",&concurrent,&error),"context outage does not commit false cancellation");
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"RENAME TABLE room_context_unavailable TO room_context");
     Require(room_sql::Query(guard.get(),"SELECT request_id FROM room_agent_publication WHERE request_id=?",{nextEpoch[0]["request_id"]}).empty(),"unavailable publication remains undecided");}
    Require(multi.CheckDispatch(nextEpoch[0]).ok(),"schema recovery restores current epoch authorization");

    // Simulate an actually persisted pre-workspace generic final: its old IM
    // identity, durable result and bytes must survive adopting the new ledger.
    source.msg_seq=253;source.msg_id=source.session_id+"-253";
    Json legacyBatch;Require(multi.BuildRequests(source,{},&legacyBatch,&error),"legacy generic fixture identity");
    auto legacyRequest=legacyBatch[0];
    for(const auto* field:{"context_generation","workspace_generation","project_id"})legacyRequest["collaboration"].erase(field);
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE room_agent_request SET payload=? WHERE request_id=?",{legacyRequest.dump(),legacyRequest["request_id"]});}
    auto oldFinal=finalReply(legacyRequest);PreparedHermesReply oldPrepared;
    Require(PrepareHermesReply(oldFinal.dump(),legacyRequest["bot_user_id"],legacyRequest["agent_name"],&oldPrepared,&error,&legacyRequest),"prepare old generic final");
    MySqlMessageReservations legacyReservations(&pool);Message oldStored;bool oldFresh=false;
    Require(legacyReservations.Reserve(oldPrepared.message,&oldStored,&oldFresh,&error)&&oldFresh,"reserve old generic message");
    auto oldContent=Json::parse(oldStored.content_json);
    oldContent["msg_id"]=oldStored.msg_id;oldContent["msg_seq"]=oldStored.msg_seq;oldContent["create_time"]=oldStored.timestamp_ms;
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"INSERT INTO message(session_id,msg_seq,sender_id,msg_type,content_json,timestamp_ms,client_msg_id) VALUES(?,?,?,?,?,?,?)",
        {oldStored.session_id,std::to_string(oldStored.msg_seq),std::to_string(oldStored.sender_id),oldStored.msg_type,oldContent.dump(),std::to_string(oldStored.timestamp_ms),oldStored.client_msg_id});
     room_sql::Query(guard.get(),"UPDATE message_reservation SET stage=3 WHERE session_id=? AND msg_seq=?",{oldStored.session_id,std::to_string(oldStored.msg_seq)});}
    Require(multi.Complete(oldFinal,&error),"persist old generic result without publication ledger");
    auto oldReplay=oldFinal;oldReplay["completed_at_ms"]=999999;
    PreparedHermesReply oldReplayPrepared;
    Require(PrepareHermesReply(oldReplay.dump(),legacyRequest["bot_user_id"],legacyRequest["agent_name"],&oldReplayPrepared,&error,&legacyRequest),"prepare old generic retransmission");
    bool requireOld=false;
    Require(restarted.PublishReply(legacyRequest,oldReplay,oldReplayPrepared.message.content_json,&concurrent,&error,&requireOld) &&
        requireOld && concurrent==oldFinal,"adopt proven old generic final after epoch revocation");
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"DELETE FROM message_reservation WHERE session_id=? AND msg_seq=?",{oldStored.session_id,std::to_string(oldStored.msg_seq)});}
    Require(restarted.PublishReply(legacyRequest,oldReplay,oldReplayPrepared.message.content_json,&concurrent,&error,&requireOld) &&
        requireOld && concurrent==oldFinal,"legacy generic replay survives reservation GC using original history");
    Message oldSame;oldFresh=true;
    Require(legacyReservations.ReservePublished(oldPrepared.message,&oldSame,&oldFresh,&error,requireOld) && !oldFresh && oldSame.msg_seq==oldStored.msg_seq,
        "legacy generic adoption cannot allocate a second message sequence");
    std::string originalLegacyResult;
    {auto guard=pool.Acquire();originalLegacyResult=room_sql::Query(guard.get(),"SELECT result FROM room_agent_request WHERE request_id=?",{legacyRequest["request_id"]})[0][0];
     room_sql::Query(guard.get(),"DELETE FROM room_agent_publication WHERE request_id=?",{legacyRequest["request_id"]});
     room_sql::Query(guard.get(),"UPDATE room_agent_request SET result=JSON_SET(result,'$.text','mismatched old result') WHERE request_id=?",{legacyRequest["request_id"]});}
    Require(!restarted.PublishReply(legacyRequest,oldReplay,oldReplayPrepared.message.content_json,&concurrent,&error),"old generic result/history mismatch cannot be adopted");
    {auto guard=pool.Acquire();Require(room_sql::Query(guard.get(),"SELECT request_id FROM room_agent_publication WHERE request_id=?",{legacyRequest["request_id"]}).empty(),"ambiguous legacy result does not commit a fabricated decision");
     room_sql::Query(guard.get(),"UPDATE room_agent_request SET result=? WHERE request_id=?",{originalLegacyResult,legacyRequest["request_id"]});}
    Require(restarted.PublishReply(legacyRequest,oldReplay,oldReplayPrepared.message.content_json,&concurrent,&error) && concurrent==oldFinal,"verified old generic result remains recoverable");

    // Revoke first: hold the room row until the actual publication SQL is
    // observed waiting in MySQL, then commit epoch 2 before it can decide.
    {
        auto guard=pool.Acquire();room_sql::Transaction transaction(guard.get());
        room_sql::Query(guard.get(),"SELECT id FROM im_group WHERE id=? FOR UPDATE",{std::to_string(room)});
        auto raceFinal=finalReply(nextEpoch[0]);Json raceResult;std::string raceError;bool published=false;
        std::thread publisher([&]{published=multi.PublishReply(nextEpoch[0],raceFinal,"racing first reply",&raceResult,&raceError);});
        bool waiting=false;std::exception_ptr probeError;
        try {waiting=WaitForRoomLock(pool,room);
            room_sql::Query(guard.get(),"UPDATE room_context SET generation=2 WHERE room_id=?",{std::to_string(room)});
            transaction.Commit();
        } catch(...) {probeError=std::current_exception();mysql_rollback(guard.get());}
        publisher.join();if(probeError)std::rethrow_exception(probeError);
        Require(waiting && published && raceResult["terminal_status"]=="cancelled","real room lock: epoch change wins before first publication");
    }
    // Publish first: the genuine Service decision holds the room lock. Observe
    // the epoch writer waiting, release publication to commit, then revoke.
    source.msg_seq=254;source.msg_id=source.session_id+"-254";
    Json raceRequests;Require(multi.BuildRequests(source,{},&raceRequests,&error),"current race epoch request");
    GatedPublicationRepository gated(&pool,&groups,&members);
    RoomAgentService gatedService(&gated,900000000001LL,true,{{"public-pi","Pi","pi_rpc",900000000401LL,true}});
    gatedService.SetProjectCatalog({{"spark-push","Spark Push"}},std::string(48,'k'));
    auto decisionReady=gated.decided.get_future();auto raceFinal=finalReply(raceRequests[0]);
    Json raceCanonical;std::string raceError;bool racePublished=false,epochCommitted=false;
    std::thread publisher([&]{racePublished=gatedService.PublishReply(raceRequests[0],raceFinal,"publish before revoke",&raceCanonical,&raceError);});
    const bool reached=decisionReady.wait_for(std::chrono::seconds(5))==std::future_status::ready;
    std::thread changer([&]{try {
        auto guard=pool.Acquire();room_sql::Transaction transaction(guard.get());
        room_sql::Query(guard.get(),"SELECT id FROM im_group WHERE id=? FOR UPDATE",{std::to_string(room)});
        room_sql::Query(guard.get(),"UPDATE room_context SET generation=3 WHERE room_id=?",{std::to_string(room)});
        transaction.Commit();epochCommitted=true;
    }catch(...) {}});
    bool waiting=false;std::exception_ptr probeError;
    try {waiting=WaitForRoomLock(pool,room);}catch(...) {probeError=std::current_exception();}
    gated.release.set_value();publisher.join();changer.join();if(probeError)std::rethrow_exception(probeError);
    Require(reached && waiting && racePublished && epochCommitted && raceCanonical==raceFinal,"real room lock: first publication commits before epoch writer");
    Require(restarted.PublishReply(raceRequests[0],raceFinal,"publish before revoke",&concurrent,&error) && concurrent==raceCanonical,
        "race winner canonical remains immutable after epoch advance");
    std::cout << "Real MySQL context: both room-lock publication/revocation orders observed and verified\n";

    // Real SQL compatibility fixtures, not broker or model execution evidence.
    Require(agent_repo.CheckPublicationSchema(&error),"CANN-only publication schema");
    source.msg_seq=300;source.msg_id=source.session_id+"-300";source.client_msg_id="legacy-cann-source";
    source.content_json=Json{{"content",{{"text","@CANN use the completed compiler receipt"}}}}.dump();
    Json cann;Require(agent.BuildRequest(source,{},&cann,&error),"build legacy CANN request");
    auto route=cann;route["agent_id"]="pi";
    auto cannFirst=finalReply(route);
    cannFirst["response_metadata"]["workspace_checkpoint"]={{"code_revision","original-source"},
        {"tests",Json::array({{{"run_id","compile-once"},{"passed",true}}})}};
    auto prepareCann=[&](const Json& value){PreparedHermesReply p;std::string local_error;
        Require(PrepareHermesReply(value.dump(),900000000001LL,"Kernel Agent",&p,&local_error,&cann),"prepare CANN SQL fixture");return p.message;};
    const auto firstMessage=prepareCann(cannFirst);
    Require(agent.PublishReply(cann,cannFirst,firstMessage.content_json,&canonical,&error) && canonical==cannFirst,"CANN-only first final decision");
    MySqlMessageReservations reservations(&pool);Message stored;bool fresh=false;
    Require(reservations.Reserve(firstMessage,&stored,&fresh,&error) && fresh,"CANN final sequence allocated once");
    auto storedContent=Json::parse(stored.content_json);
    storedContent["msg_id"]=stored.msg_id;storedContent["msg_seq"]=stored.msg_seq;storedContent["create_time"]=stored.timestamp_ms;
    const auto storedBytes=storedContent.dump();
    {
        auto guard=pool.Acquire();
        room_sql::Query(guard.get(),"INSERT INTO message(session_id,msg_seq,sender_id,msg_type,content_json,timestamp_ms,client_msg_id) VALUES(?,?,?,?,?,?,?)",
            {stored.session_id,std::to_string(stored.msg_seq),std::to_string(stored.sender_id),stored.msg_type,storedBytes,std::to_string(stored.timestamp_ms),stored.client_msg_id});
        room_sql::Query(guard.get(),"UPDATE message_reservation SET stage=3,persist_payload=? WHERE session_id=? AND msg_seq=?",{"original-outbox-fixture",stored.session_id,std::to_string(stored.msg_seq)});
    }
    Require(agent.Complete(cannFirst,&error),"old CANN original result persisted");
    auto cannReplay=cannFirst;cannReplay["completed_at_ms"]=999999;cannReplay["delta_count"]=23;
    auto& cannEvent=cannReplay["response_metadata"]["agent_event"]["envelope"];
    cannEvent["sequence"]=23;cannEvent["event_id"]=ProjectedEventId(cann["request_id"],23);cannEvent["replayed"]=true;
    auto replayMessage=prepareCann(cannReplay);
    Require(restarted.PublishReply(cann,cannReplay,replayMessage.content_json,&canonical,&error) && canonical==cannFirst,"new CANN replay retains original projection after service restart");
    auto snapshot=[&]{auto guard=pool.Acquire();return room_sql::Query(guard.get(),
        "SELECT m.msg_seq,m.content_json,m.timestamp_ms,r.result FROM message m JOIN room_agent_request r ON r.request_id=? WHERE m.session_id=? AND m.sender_id=? AND m.client_msg_id=?",
        {cann.at("request_id"),stored.session_id,std::to_string(stored.sender_id),stored.client_msg_id});};
    const auto originalRows=snapshot();Require(originalRows.size()==1,"one original CANN history row");
    for(int gc_case:{0,1,2}) {
        {
            auto guard=pool.Acquire();
            room_sql::Query(guard.get(),"DELETE FROM room_agent_publication WHERE request_id=?",{cann.at("request_id")});
            if(gc_case==1)room_sql::Query(guard.get(),"DELETE FROM message_reservation WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)});
            if(gc_case==2)room_sql::Query(guard.get(),"UPDATE message_reservation SET created_at=CURRENT_TIMESTAMP(3)-INTERVAL 2 DAY WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)});
        }
        bool require_existing=false;
        Require(restarted.PublishReply(cann,cannReplay,replayMessage.content_json,&canonical,&error,&require_existing) && require_existing,"old CANN adoption: "+error);
        if(gc_case==2) {
            std::vector<std::pair<Message,std::string>> pending_gc;
            Require(reservations.Pending(&pending_gc,10,&error),"actual reservation GC between publication and allocation");
            auto guard=pool.Acquire();Require(room_sql::Query(guard.get(),"SELECT msg_seq FROM message_reservation WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)}).empty(),"GC really removed the observed reservation");
        }
        auto recovered=prepareCann(canonical);Message same;fresh=true;
        Require(recovered.content_json==firstMessage.content_json && recovered.timestamp_ms==firstMessage.timestamp_ms,"old CANN exact original durable content");
        Require(reservations.ReservePublished(recovered,&same,&fresh,&error,require_existing) && !fresh && same.msg_seq==stored.msg_seq,"old CANN keeps sequence after reservation collection");
        Require(snapshot()==originalRows,"old CANN adoption never rewrites history/result");
        auto read=pool.Acquire();const auto outbox=room_sql::Query(read.get(),"SELECT stage,persist_payload FROM message_reservation WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)});
        Require(outbox.size()==1 && outbox[0][0]=="3" && (gc_case?outbox[0][1].empty():outbox[0][1]=="original-outbox-fixture"),"existing outbox unchanged; expired reservation restored without invented payload");
    }
    auto corrupt=cannReplay;corrupt["response_metadata"]["workspace_checkpoint"]["tests"][0]["passed"]=false;
    Require(!restarted.PublishReply(cann,corrupt,prepareCann(corrupt).content_json,&canonical,&error) && snapshot()==originalRows,"CANN conflicting tool receipt cannot overwrite completion");
    auto erasePublication=[&]{auto guard=pool.Acquire();room_sql::Query(guard.get(),"DELETE FROM room_agent_publication WHERE request_id=?",{cann.at("request_id")});};
    auto dbState=[&]{auto guard=pool.Acquire();return Json{
        {"message",snapshot()},
        {"publication",room_sql::Query(guard.get(),"SELECT input_digest,reply FROM room_agent_publication WHERE request_id=?",{cann.at("request_id")})},
        {"reservation",room_sql::Query(guard.get(),"SELECT original_message,stage,HEX(persist_payload) FROM message_reservation WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)})},
        {"sequence",room_sql::Query(guard.get(),"SELECT last_seq FROM message_sequence WHERE session_id=?",{stored.session_id})}};};
    erasePublication();
    for(const auto* field:{"user_id","bot_user_id","session_id"}) {
        auto wrong=cannReplay;wrong[field]=std::string(field)=="session_id"?Json("r_999999"):Json(999999);
        const auto before=dbState();
        Require(!restarted.PublishReply(cann,wrong,replayMessage.content_json,&canonical,&error) && dbState()==before,"legacy adoption cannot change room/actor/bot");
    }
    {const auto before=dbState();Require(!restarted.PublishReply(cann,corrupt,prepareCann(corrupt).content_json,&canonical,&error)&&dbState()==before,"legacy receipt conflict leaves no partial publication");}
    {auto guard=pool.Acquire();auto different=storedContent;different["content"]["text"]="inconsistent durable source";
     room_sql::Query(guard.get(),"UPDATE message SET content_json=? WHERE session_id=? AND msg_seq=?",{different.dump(),stored.session_id,std::to_string(stored.msg_seq)});}
    {const auto before=dbState();Require(!restarted.PublishReply(cann,cannReplay,replayMessage.content_json,&canonical,&error)&&dbState()==before,"two individually valid but different legacy sources are rejected");}
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE message SET content_json=? WHERE session_id=? AND msg_seq=?",{storedBytes,stored.session_id,std::to_string(stored.msg_seq)});}
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE message SET timestamp_ms=timestamp_ms+1 WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)});}
    {const auto before=dbState();Require(!restarted.PublishReply(cann,cannReplay,replayMessage.content_json,&canonical,&error)&&dbState()==before,"conflicting history/reservation is not repaired by overwriting either source");}
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"UPDATE message SET timestamp_ms=? WHERE session_id=? AND msg_seq=?",{std::to_string(stored.timestamp_ms),stored.session_id,std::to_string(stored.msg_seq)});
     room_sql::Query(guard.get(),"DELETE FROM message WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)});}
    bool require_existing=false;
    Require(restarted.PublishReply(cann,cannReplay,replayMessage.content_json,&canonical,&error,&require_existing)&&require_existing,"reservation-only completed legacy receipt can be verified");
    Message same;fresh=true;
    Require(reservations.ReservePublished(prepareCann(canonical),&same,&fresh,&error,require_existing)&&!fresh&&same.msg_seq==stored.msg_seq,"reservation-only keeps the reserved identity");
    erasePublication();
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"DELETE FROM message_reservation WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)});}
    {const auto before=dbState();Require(!restarted.PublishReply(cann,cannReplay,replayMessage.content_json,&canonical,&error)&&dbState()==before,"missing both legacy sources rejects without mutation");
     Require(!reservations.ReservePublished(firstMessage,&same,&fresh,&error,true)&&dbState()==before,"known old publication never falls back to fresh allocation when history vanishes");}
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"INSERT INTO message(session_id,msg_seq,sender_id,msg_type,content_json,timestamp_ms,client_msg_id) VALUES(?,?,?,?,?,?,?)",{stored.session_id,std::to_string(stored.msg_seq),std::to_string(stored.sender_id),stored.msg_type,storedBytes,std::to_string(stored.timestamp_ms),stored.client_msg_id});}
    bool adopted[2]={false,false};
    auto adopt=[&](int index){
        for(int attempt=0;attempt<3&&!adopted[index];++attempt){Json answer;std::string failure;bool old=false,is_new=true;Message message;
            adopted[index]=restarted.PublishReply(cann,cannReplay,replayMessage.content_json,&answer,&failure,&old)&&old&&
                reservations.ReservePublished(prepareCann(answer),&message,&is_new,&failure,old)&&!is_new&&message.msg_seq==stored.msg_seq&&message.content_json==firstMessage.content_json;}
    };
    std::thread adopt1([&]{adopt(0);}),adopt2([&]{adopt(1);});adopt1.join();adopt2.join();
    Require(adopted[0]&&adopted[1]&&snapshot()==originalRows,"concurrent old publication/GC recovery converges on one original message");
    {auto guard=pool.Acquire();room_sql::Query(guard.get(),"DELETE FROM message_reservation WHERE session_id=? AND msg_seq=?",{stored.session_id,std::to_string(stored.msg_seq)});}
    for(bool change_type:{false,true}) {
        auto changed=firstMessage;
        if(change_type)changed.msg_type="system";else changed.content_json="different result";
        const auto before=dbState();
        Require(!reservations.ReservePublished(changed,&same,&fresh,&error,true)&&dbState()==before,"strict published-history comparison rejects changed type/content without allocating");
    }

    if(const char* fixture_path=std::getenv("SPARK_TEST_CANN_LEGACY_FIXTURE")) {
        // Optional independent oracle captured from an OLD real image. Raw
        // message/outbox/result bytes come from that image, not this writer.
        std::ifstream input(fixture_path);Json fixture;input>>fixture;
        Require(fixture.at("schema")=="sparkpush.cann_legacy_fixture.v1","captured fixture schema");
        const auto captured=Json::parse(fixture.at("request_json").get<std::string>());
        const auto prior=Json::parse(fixture.at("original_message_json").get<std::string>());
        const auto captured_reply=Json::parse(fixture.at("replay_reply_json").get<std::string>());
        const std::string hex=fixture.at("persist_payload_hex");std::string binary;
        Require(hex.size()%2==0,"captured binary length");
        for(size_t i=0;i<hex.size();i+=2)binary.push_back(static_cast<char>(std::stoul(hex.substr(i,2),nullptr,16)));
        PersistMessageRequest payload;Require(payload.ParseFromString(binary),"parse OLD original outbox protobuf");
        const auto& message=payload.message();
        Require(message.session_id()==prior.at("session")&&message.msg_seq()==prior.at("seq")&&message.client_msg_id()==prior.at("client")&&message.sender_id()==prior.at("sender")&&message.timestamp_ms()==prior.at("time"),"captured SQL and outbox identities agree");
        const auto historic=Json::parse(message.content_json());const std::string bot_name=historic.at("sender").at("name");
        PreparedHermesReply candidate;
        Require(PrepareHermesReply(captured_reply.dump(),message.sender_id(),bot_name,&candidate,&error,&captured),"prepare captured replay");
        {
            auto guard=pool.Acquire();const auto actor=std::to_string(captured.at("user_id").get<int64_t>());
            if(room_sql::Query(guard.get(),"SELECT id FROM user WHERE id=?",{actor}).empty())
                room_sql::Query(guard.get(),"INSERT INTO user(id,account,password_hash) VALUES(?,?,'test')",{actor,"captured-"+actor});
            room_sql::Query(guard.get(),"INSERT INTO room_agent_request(request_id,room_id,message_seq,payload,result) VALUES(?,?,?,?,?)",{captured.at("request_id"),std::to_string(captured.at("collaboration").at("room_id").get<int64_t>()),std::to_string(captured.at("message_seq").get<int64_t>()),fixture.at("request_json"),fixture.at("result_json")});
            room_sql::Query(guard.get(),"INSERT INTO message(session_id,msg_seq,sender_id,msg_type,content_json,timestamp_ms,client_msg_id) VALUES(?,?,?,?,?,?,?)",{message.session_id(),std::to_string(message.msg_seq()),std::to_string(message.sender_id()),message.msg_type(),message.content_json(),std::to_string(message.timestamp_ms()),message.client_msg_id()});
            room_sql::Query(guard.get(),"INSERT INTO message_reservation(session_id,msg_seq,sender_id,client_msg_id,original_message,stage,persist_payload) VALUES(?,?,?,?,?,3,UNHEX(?))",{message.session_id(),std::to_string(message.msg_seq()),std::to_string(message.sender_id()),message.client_msg_id(),fixture.at("original_message_json"),hex});
        }
        for(bool gc:{false,true}) {
            if(gc){auto guard=pool.Acquire();room_sql::Query(guard.get(),"DELETE FROM room_agent_publication WHERE request_id=?",{captured.at("request_id")});room_sql::Query(guard.get(),"DELETE FROM message_reservation WHERE session_id=? AND msg_seq=?",{message.session_id(),std::to_string(message.msg_seq())});}
            Json recovered;bool known=false;
            Require(restarted.PublishReply(captured,captured_reply,candidate.message.content_json,&recovered,&error,&known)&&known,"adopt OLD captured completion: "+error);
            PreparedHermesReply final;
            Require(PrepareHermesReply(recovered.dump(),message.sender_id(),bot_name,&final,&error,&captured),"prepare recovered OLD final");
            bool was_new=true;
            Require(PersistHermesReply(&final.message,&was_new,
                [&](Message* m,bool* is_new){const auto proposal=*m;return reservations.ReservePublished(proposal,m,is_new,&error,known);},
                [&](const Message& m){return m.content_json==message.content_json()&&m.msg_seq==message.msg_seq()&&m.timestamp_ms==message.timestamp_ms();})&&!was_new,"OLD exact persisted bytes/time/sequence survive adoption and GC");
            auto guard=pool.Acquire();
            const auto rows=room_sql::Query(guard.get(),"SELECT m.content_json,r.result FROM message m JOIN room_agent_request r ON r.request_id=? WHERE m.session_id=? AND m.msg_seq=?",{captured.at("request_id"),message.session_id(),std::to_string(message.msg_seq())});
            Require(rows.size()==1&&rows[0][0]==message.content_json()&&Json::parse(rows[0][1])==Json::parse(fixture.at("result_json").get<std::string>()),"OLD history and result never rewritten");
            if(!gc)Require(room_sql::Query(guard.get(),"SELECT HEX(persist_payload) FROM message_reservation WHERE session_id=? AND msg_seq=?",{message.session_id(),std::to_string(message.msg_seq())}).at(0).at(0)==hex,"OLD binary outbox remains byte-identical");
        }
        std::cout<<"captured pre-fix CANN message/outbox/result exact recovery passed\n";
    }

    std::cout<<"real MySQL space transactions, concurrency, SQL binding and Agent persistence passed\n";
}
