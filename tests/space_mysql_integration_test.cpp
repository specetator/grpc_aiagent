#include <cstdlib>
#include <iostream>
#include <thread>
#include <stdexcept>
#include "repositories/space_repository.h"
#include "repositories/room_agent_repository.h"
#include "repositories/room_sql.h"
#include "services/room_service.h"
using namespace sparkpush;
void Require(bool ok,const std::string& why) { if(!ok) throw std::runtime_error(why); }
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
    std::cout<<"real MySQL space transactions, concurrency, SQL binding and Agent persistence passed\n";
}
