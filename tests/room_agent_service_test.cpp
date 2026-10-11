#include "services/room_agent_service.h"
#include "hermes_reply.h"
#include <iostream>
#include <stdexcept>
using namespace sparkpush;
using Json = nlohmann::json;
namespace {
int checks = 0;
void Check(bool value, const char* why) { ++checks; if (!value) throw std::runtime_error(why); }
class MemoryRepository : public RoomAgentRepository {
 public:
    Json task;
    std::map<std::string, Json> requests, results;
    bool fail{false};
    bool Access(int64_t room, int64_t user, int64_t* owner, bool* member, std::string* e) override {
        if (fail) { *e="database unavailable"; return false; }
        *owner=7; *member=room==1 && user>=7 && user<=9; return true;
    }
    bool Load(int64_t, Json* out, std::string* e) override { if(fail){*e="database unavailable";return false;} *out=task;return true; }
    bool Save(int64_t, int64_t version, const Json& value, std::string* e) override {
        if (fail || version!=(task.is_null()?0:task.value("revision",0))) {*e="conflict";return false;}
        task=value;return true;
    }
    bool Request(const std::string& id, Json* out, std::string* e) override {
        if(fail){*e="database unavailable";return false;}
        *out=requests.count(id)?requests[id]:Json(nullptr);return true;
    }
    bool PutRequest(const Json& value,std::string*) override { requests[value.at("request_id")]=value;return true; }
    bool Finish(const std::string& id,const Json& value,std::string*) override { results[id]=value;return true; }
    bool Requests(int64_t,Json* out,std::string*) override { *out=Json::array();return true; }
};
}
int main() {
 try {
    MemoryRepository repo;
    RoomAgentService service(&repo,99,true);
    OperatorTaskService tasks(&repo);
    Json out;
    const Json config={{"title","Add operator"},{"spec","fp32; CANN 8.5"},{"developer_user_ids",Json::array({8})}};
    Check(service.Configure(10,1,config,&out).code==403,"outsider configure");
    Check(service.Configure(8,1,config,&out).code==403,"non-owner configure");
    auto bad=config;bad["developer_user_ids"]=Json::array({10});
    Check(service.Configure(7,1,bad,&out).code==403,"grant outsider");
    Check(service.Configure(7,1,config,&out).ok(),"owner configure");
    Check(out["task_id"].get<std::string>().size()==32,"stable random task ID");
    const auto task_id=out["task_id"];
    Check(service.View(8,1,&out).ok() && out["can_develop"]==true,"developer view");
    Check(service.View(9,1,&out).ok() && out["can_develop"]==false,"observer view");
    Check(service.View(10,1,&out).code==403,"outsider view");
    Check(tasks.Checkpoint(9,1,1,{{"summary","hack"}},&out).code==403,"observer cannot edit progress");
    Check(tasks.Checkpoint(8,1,1,{{"summary","tail block pending"},{"next_steps","fix tail"}},&out).ok(),"developer checkpoint");
    Check(tasks.Checkpoint(7,1,1,{{"summary","stale"}},&out).code==409,"stale checkpoint");
    Check(tasks.Checkpoint(8,1,2,{{"passed",true}},&out).code==400,"cannot invent test receipt");
    Check(tasks.Checkpoint(8,1,2,{{"task_id","rebind"}},&out).code==400,"cannot rebind task");
    auto text=[](const std::string& t){return Json({{"content",{{"text",t}}}}).dump();};
    bool trigger=false;
    Check(service.Trigger(8,1,text("discussion"),&trigger).ok() && !trigger,"ordinary discussion");
    Check(service.Trigger(9,1,text("@CANN explain"),&trigger).ok() && trigger,"observer can ask");
    Check(service.Trigger(10,1,text("@CANN explain"),&trigger).code==403,"outsider cannot submit");
    Check(service.Trigger(8,1,text("@CANN /new"),&trigger).code==403,"shared reset denied");
    Check(service.Trigger(8,1,text("@CANN"),&trigger).code==400,"empty question denied");
    Message current;current.msg_id="r_1-4";current.session_id="r_1";current.sender_id=8;current.msg_seq=4;current.content_json=text("@CANN fix tail");
    Message earlier=current;earlier.msg_seq=3;earlier.sender_id=7;earlier.content_json=text("earlier context");
    Message future=current;future.msg_seq=5;future.content_json=text("FUTURE_MARKER");
    Json request;std::string error;
    Check(service.BuildRequest(current,{future,earlier},&request,&error),"build room request");
    const auto prompt=request["messages"][0]["content"].get<std::string>();
    Check(prompt.find("user_id=7")!=std::string::npos,"participant attribution");
    Check(prompt.find("FUTURE_MARKER")==std::string::npos,"future context excluded");
    Check(prompt.find("tail block pending")!=std::string::npos,"durable handoff context");
    Check(request["collaboration"]["can_develop"]==true,"trusted developer scope");
    repo.task["summary"]="later edit";
    Json retry;Check(service.BuildRequest(current,{},&retry,&error) && retry==request,"retries use immutable accepted input");
    Json reply={{"request_id",current.msg_id},{"session_id","r_1"},{"user_id",8},{"bot_user_id",99},
                {"collaboration",request["collaboration"]},{"ok",true},{"text","fixed"},{"completed_at_ms",12345}};
    Json authorized;Check(service.ValidateReply(reply,&authorized,&error),"ledger reply authorization");
    PreparedHermesReply prepared;
    Check(!PrepareHermesReply(reply.dump(),99,"CANN",&prepared,&error),"unvalidated room reply denied");
    Check(PrepareHermesReply(reply.dump(),99,"CANN",&prepared,&error,&authorized),"validated room reply accepted");
    auto content=Json::parse(prepared.message.content_json);
    Check(content["type"]=="chatroom" && content["group_id"]==1 && !content.contains("to_user_id"),"room final envelope");
    auto forged=reply;forged["session_id"]="r_2";
    Check(!service.ValidateReply(forged,&authorized,&error),"cross-room reply denied");
    forged=reply;forged["user_id"]=7;
    Check(!service.ValidateReply(forged,&authorized,&error),"actor spoof denied");
    forged=reply;forged["collaboration"]["task_id"]="another";
    Check(!service.ValidateReply(forged,&authorized,&error),"task spoof denied");
    Check(service.Complete(reply,&error),"durable final checkpoint");
    Check(service.Complete(reply,&error),"idempotent completion");
    Check(repo.results.size()==1,"one result after replay");
    Check(repo.task["task_id"]==task_id,"handoff keeps task");
    repo.fail=true;
    Check(service.Trigger(8,1,text("@CANN question"),&trigger).code==500,"membership error fails closed");
    std::cout << "room Agent service: " << checks << " checks passed\n";
    return 0;
 } catch(const std::exception& e) { std::cerr<<e.what()<<'\n';return 1; }
}
