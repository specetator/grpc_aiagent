#include "services/room_agent_service.h"
#include "hermes_reply.h"
#include "agent_event.h"
#include <openssl/sha.h>
#include <iostream>
#include <stdexcept>
using namespace sparkpush;
using Json = nlohmann::json;
namespace {
int checks = 0;
void Check(bool value, const char* why) { ++checks; if (!value) throw std::runtime_error(why); }
// Produce the pre-v2 persisted fixture without calling the new digest code.
std::string LegacyPublicationDigest(const Json& reply,const std::string& content) {
    const auto input=Json{{"message",content},{"result",{{"ok",reply.at("ok")},{"text",reply.at("text")}}}}.dump();
    unsigned char bytes[SHA256_DIGEST_LENGTH];SHA256(reinterpret_cast<const unsigned char*>(input.data()),input.size(),bytes);
    std::string value;const char* hex="0123456789abcdef";
    for(auto byte:bytes){value+=hex[byte>>4];value+=hex[byte&15];}return value;
}
class MemoryRepository : public RoomAgentRepository {
 public:
    Json task;
    std::map<std::string, Json> requests, results;
    Json bindings=Json::array();
    Json workspace={{"project_id",""},{"generation",0}};
    bool Workspace(int64_t,Json* out,std::string*)override{*out=workspace;return !fail;}
    bool SetWorkspace(int64_t,int64_t,int64_t generation,const std::string& id,std::string*)override{
        if(workspace["generation"]!=generation)return false;
        if(workspace["project_id"]!=id || generation==0)workspace={{"project_id",id},{"generation",generation+1}};
        return true;
    }
    std::map<std::string,Json> publications;
    bool Publication(const Json& request,const std::string& digest,const PublicationMatch& matches,const PublicationDecision& decide,Json* out,std::string*)override {
        if(fail)return false;
        const std::string id=request.at("request_id");
        if(publications.count(id)) {
            if(publications[id]["digest"]!=digest && !matches(publications[id]["digest"],publications[id]["reply"]))return false;
            *out=publications[id]["reply"];return true;
        }
        *out=decide({{"member",!removed},{"bindings",bindings},{"workspace",workspace}});
        publications[id]={{"digest",digest},{"reply",*out}};return true;
    }
    std::map<std::string,Json> tasks;
    bool fail{false};
    bool removed{false};
    bool Access(int64_t room, int64_t user, int64_t* owner, bool* member, std::string* e) override {
        if (fail) { *e="database unavailable"; return false; }
        *owner=7; *member=room==1 && user>=7 && user<=9 && !(removed && user==8); return true;
    }
    bool Task(int64_t,const std::string& id,Json* out,std::string*)override{*out=tasks.count(id)?tasks[id]:Json();return true;}
    bool Tasks(int64_t,Json* out,std::string*)override{*out=tasks;return true;}
    bool SaveTask(int64_t,int64_t revision,const Json& task,std::string*)override{
        const std::string id=task.at("task_id");if(revision && tasks[id].at("revision")!=revision)return false;tasks[id]=task;return true;
    }
    bool Bindings(int64_t,Json* out,std::string*) override { *out=bindings;return !fail; }
    bool SetBinding(int64_t,int64_t,const std::string& id,bool installed,std::string*) override {
        for(auto& b:bindings)if(b["agent_id"]==id){if(b["installed"]!=installed)b["generation"]=b["generation"].get<int>()+1;b["installed"]=installed;return true;}
        bindings.push_back({{"agent_id",id},{"installed",installed},{"generation",1}});return true;
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
    bool RecoverDispatch(const std::string&,std::string*) override {return true;}
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
    Json registry={{"schema",1},{"agents",Json::array({
        {{"id","public-pi"},{"name","Pi"},{"runtime","pi_rpc"},{"owner_user_ids",Json::array()},{"bot_user_id",101},{"room_enabled",true},{"room_policy","read_only"}},
        {{"id","second-pi"},{"name","Pi"},{"runtime","pi_rpc"},{"owner_user_ids",Json::array()},{"bot_user_id",102},{"room_enabled",true},{"room_policy","read_only"}}
    })}};
    auto catalog=ParseRoomAgentCatalog(registry);
    auto publicHermes=registry["agents"][0];
    publicHermes["id"]="public-hermes";publicHermes["runtime"]="hermes_native";
    publicHermes["profile"]="spark-public-hermes";publicHermes["room_public_profile"]=true;
    publicHermes["room_workspace_contract"]=1;publicHermes["native_interactive_approvals"]=false;
    Json nativeRegistry={{"schema",1},{"agents",Json::array({publicHermes})}};
    Check(!ParseRoomAgentCatalog(nativeRegistry)[0].enabled,"public Native requires separate operator opt in");
    Check(!ParseRoomAgentCatalog(nativeRegistry,true)[0].enabled,"test switch cannot enable public Native");
    Check(ParseRoomAgentCatalog(nativeRegistry,false,true)[0].enabled,"public Native explicit isolated contract accepted");
    for(const auto& change:std::vector<Json>{{{"profile","technical"}},{{"room_workspace_contract",0}},
                {{"room_public_profile",false}},{{"native_interactive_approvals",true}}}) {
        auto bad=nativeRegistry;bad["agents"][0].update(change);bool refused=false;
        try {ParseRoomAgentCatalog(bad,false,true);}catch(const std::exception&){refused=true;}
        Check(refused,"public Native incomplete isolation contract refused");
    }
    RoomAgentService generic(&repo,99,true,catalog);
    Check(generic.Install(8,1,{{"agent_id","public-pi"}},true).code==403,"member cannot install");
    Check(generic.Install(7,1,{{"agent_id","public-pi"},{"runtime_url","bad"}},true).code==400,"cannot inject runtime");
    Check(generic.Install(7,1,{{"agent_id","public-pi"}},true).ok(),"owner install Pi");
    Check(generic.Install(7,1,{{"agent_id","second-pi"}},true).ok(),"owner install second Agent");
    Check(generic.Trigger(8,1,text("@Pi plain text"),&trigger).ok()&&!trigger,"display name alone cannot route");
    auto mention=Json::parse(text("@Pi summarize"));mention["agent_mentions"]=Json::array({"second-pi","public-pi"});
    current.msg_id="r_1-7";current.msg_seq=7;current.content_json=mention.dump();
    Check(generic.Trigger(8,1,current.content_json,&trigger).ok()&&trigger,"structured mentions route");
    Json many;Check(generic.BuildRequests(current,{},&many,&error)&&many.size()==2,"two requests for one source");
    Check(many[0]["request_id"]!=many[1]["request_id"],"Agent request IDs distinct");
    Check(many[0]["request_id"].get<std::string>().size()==67,"bounded deterministic request ID");
    Json replay;Check(generic.BuildRequests(current,{earlier},&replay,&error)&&replay==many,"multi request immutable replay");
    Check(many[0]["collaboration"]["actor_user_id"]==8 && many[0]["source_msg_id"]=="r_1-7","server request attribution");
    auto unknown=mention;unknown["agent_mentions"]=Json::array({"unknown"});
    Check(generic.Trigger(8,1,unknown.dump(),&trigger).code==403,"unknown Agent denied");
    unknown["agent_mentions"]=Json::array({"public-pi","public-pi"});
    Check(generic.Trigger(8,1,unknown.dump(),&trigger).code==400,"duplicate IDs denied");
    Check(generic.CheckDispatch(many[0]).ok(),"installed dispatch admitted");
    Check(generic.Control(8,1,{{"request_id",many[0]["request_id"]},{"operation","recover"}},&out).ok()&&out["status"]=="recovery_queued","recover original request without new ID");
    Check(generic.Control(9,1,{{"request_id",many[0]["request_id"]},{"operation","recover"}},&out).code==403,"unrelated member cannot recover another request");
    Check(generic.Install(7,1,{{"agent_id","public-pi"}},false).ok(),"owner uninstall");
    Check(generic.CheckDispatch(many[0]).code==403,"uninstalled dispatch denied");
    Check(generic.Install(7,1,{{"agent_id","public-pi"}},true).ok(),"owner reinstall");
    Check(generic.CheckDispatch(many[0]).code==403,"old generation stays revoked");
    auto genericReply=reply;for(const auto* k:{"request_id","session_id","user_id","bot_user_id","collaboration"})genericReply[k]=many[0][k];
    Check(generic.ValidateReply(genericReply,&authorized,&error),"generic stored reply scope");
    Check(PrepareHermesReply(genericReply.dump(),101,"Pi",&prepared,&error,&authorized),"generic final uses same IM pipeline");
    auto final=Json::parse(prepared.message.content_json);
    Check(final["agent_id"]=="public-pi"&&final["source_msg_id"]==current.msg_id&&!final.contains("agent_mentions"),"final attribution without recursive trigger");
    genericReply["ok"]=false;genericReply["terminal_status"]="cancelled";
    Check(generic.Complete(genericReply,&error),"cancelled result persisted");
    Json createdTask;
    Check(generic.CreateTask(8,1,{{"request_id",many[0]["request_id"]},{"title","Kafka delivery"},{"steps",Json::array({"trace outbox","test retry"})}},&createdTask).ok(),"member creates durable task from request");
    const auto taskId=createdTask.at("task_id");
    Check(generic.UpdateTask(9,1,{{"task_id",taskId},{"revision",1},{"summary","unclaimed"}},&out).code==403,"must claim task before progress");
    Check(generic.UpdateTask(9,1,{{"task_id",taskId},{"revision",1},{"claim",true},{"status","running"}},&out).ok(),"other member takes task");
    Check(generic.UpdateTask(8,1,{{"task_id",taskId},{"revision",2},{"claim",true}},&out).code==403,"cannot steal assignment");
    Check(generic.UpdateTask(9,1,{{"task_id",taskId},{"revision",1},{"summary","stale"}},&out).code==409,"task optimistic concurrency");
    Check(generic.UpdateTask(9,1,{{"task_id",taskId},{"revision",2},{"summary","handoff"},{"status","completed"}},&out).ok(),"assignee persists progress");
    generic.SetControlTransport([](const Json& command){return Json{{"code",0},{"data",{{"status","unknown"},{"request_id",command.at("request_id")}}}};});
    Check(generic.Control(9,1,{{"request_id",many[0]["request_id"]},{"operation","stop"}},&out).code==403,"unrelated member cannot stop task");
    Check(generic.Control(8,1,{{"request_id",many[0]["request_id"]},{"operation","status"}},&out).ok()&&out["status"]=="unknown","unknown runtime remains unknown");
    const std::string workspaceKey(48,'w');
    const auto projects=ParseRoomProjectCatalog({{"schema",1},{"projects",Json::array({{{"id","spark-push"},{"name","Spark Push"},{"root","/private/operator/path"}}})}});
    generic.SetProjectCatalog(projects,workspaceKey);
    Check(generic.Workspace(8,1,&out).ok() && !out["can_manage"].get<bool>() && out.dump().find("/private/")==std::string::npos,"member sees project IDs without roots");
    Check(generic.Workspace(10,1,&out).code==403,"outsider cannot enumerate workspace");
    const Json selection={{"project_id","spark-push"},{"generation",0}};
    Check(generic.SetWorkspace(8,1,selection,&out).code==403,"member cannot authorize project");
    auto injected=selection;injected["root"]="/etc";
    Check(generic.SetWorkspace(7,1,injected,&out).code==400,"path injection rejected");
    injected=selection;injected["project_id"]="unknown";
    Check(generic.SetWorkspace(7,1,injected,&out).code==403,"unlisted project rejected");
    Check(generic.SetWorkspace(7,1,selection,&out).ok() && out["workspace"]["generation"]==1,"owner authorizes project");
    Check(generic.SetWorkspace(7,1,selection,&out).code==409,"stale workspace update rejected");
    current.msg_id="r_1-9";current.msg_seq=9;
    Json scoped;
    Check(generic.BuildRequests(current,{},&scoped,&error),"request binds current workspace");
    const auto scopedId=scoped[0]["request_id"];
    Check(scoped[0]["collaboration"]["project_id"]=="spark-push" && scoped[0]["collaboration"]["workspace_generation"]==1,"server-owned project scope");
    Check(generic.AuthorizeWorkspace("wrong",{{"request_id",scopedId}},&out).code==401,"connector credential required");
    Check(generic.AuthorizeWorkspace(workspaceKey,{{"request_id",scopedId},{"actor_user_id",7}},&out).code==400,"connector cannot submit actor identity");
    Check(generic.AuthorizeWorkspace(workspaceKey,{{"request_id",scopedId}},&out).ok() && out["collaboration"]==scoped[0]["collaboration"],"canonical stored scope returned");
    auto workspaceReply=scoped[0];workspaceReply["ok"]=true;workspaceReply["text"]="authorized code result";workspaceReply["completed_at_ms"]=100;
    Json published;
    Check(generic.PublishReply(scoped[0],workspaceReply,"canonical original",&published,&error) && published==workspaceReply,"first allowed publication decision committed");
    auto checkpointReply=workspaceReply;checkpointReply["response_metadata"]={{"workspace_checkpoint",{{"passed",true}}}};
    Check(generic.Complete(checkpointReply,&error) && !repo.results[scopedId].contains("workspace_checkpoint"),"read-only generic result cannot persist CANN write/test checkpoint");
    auto provenanceReply=workspaceReply;
    provenanceReply["response_metadata"]={{"project_snapshot",{{"project_id","spark-push"},{"revision",std::string(64,'a')},{"file_count",3},{"root","/private"}}}};
    Check(generic.Complete(provenanceReply,&error) && repo.results[scopedId]["project_snapshot"].size()==3,"result provenance allowlist removes paths");
    Check(!generic.PublishReply(scoped[0],provenanceReply,"canonical original",&published,&error),"durable provenance changes conflict even with same supplied message content");
    provenanceReply["response_metadata"]["project_snapshot"]["revision"]="invalid";
    Check(generic.Complete(provenanceReply,&error) && !repo.results[scopedId].contains("project_snapshot"),"invalid provenance rejected");
    {
        auto request=scoped[0];request["request_id"]="publication-transport-test";
        auto first=workspaceReply;first["request_id"]=request["request_id"];first["delta_count"]=2;
        first["response_metadata"]["agent_event"]={{"schema","sparkpush.agent_event.v1"},{"type","assistant_final"},
            {"data",{{"text",first["text"]}}},{"envelope",{{"schema","sparkpush.agent_envelope.v1"},
            {"request_id",request["request_id"]},{"event_id",ProjectedEventId(request["request_id"],2)},
            {"route_key","rt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"session_key","room-public-pi"},
            {"tenant_id","local"},{"channel_id","spark_pc"},{"conversation_id",request["session_id"]},
            {"thread_id","_"},{"agent_id",request["agent_id"]},{"run_id","native-run-1"},
            {"sequence",2},{"created_at_ms",100},{"replayable",true},{"terminal",true},{"replayed",false},
            {"receipt",{{"sequence",4},{"created_at_ms",123}}}}}};
        auto prepare=[&](const Json& reply){PreparedHermesReply prepared;
            Check(PrepareHermesReply(reply.dump(),request["bot_user_id"],request["agent_name"],&prepared,&error,&request),"prepare transport fixture");
            return prepared.message.content_json;};
        const auto firstContent=prepare(first);
        Check(generic.PublishReply(request,first,firstContent,&published,&error) && published==first,"v2 first projection admitted");
        auto replay=first;replay["completed_at_ms"]=999;replay["delta_count"]=9;
        auto& envelope=replay["response_metadata"]["agent_event"]["envelope"];
        envelope["sequence"]=9;envelope["event_id"]=ProjectedEventId(request["request_id"],9);
        envelope["replayed"]=true;envelope["created_at_ms"]=999;
        const auto replayContent=prepare(replay);
        Check(firstContent!=replayContent,"fixture really varies transport bytes");
        Check(generic.PublishReply(request,replay,replayContent,&published,&error) && published==first && prepare(published)==firstContent,"v2 returns original bytes and timestamp across projection changes");
        const std::string id=request["request_id"];
        repo.publications[id]["digest"]=LegacyPublicationDigest(first,firstContent);
        const auto legacyRow=repo.publications[id];
        Check(generic.PublishReply(request,replay,replayContent,&published,&error) && published==first && repo.publications[id]==legacyRow,"old digest is verified before compatible replay; row unchanged");
        for(const auto* field:{"run_id","session_key","agent_id","route_key"}) {
            auto changed=replay;changed["response_metadata"]["agent_event"]["envelope"][field]=
                std::string(field)=="route_key"?"rt_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb":"changed-identity";
            Check(!generic.PublishReply(request,changed,prepare(changed),&published,&error),"runtime identity cannot change on replay");
        }
        auto changed=replay;changed["text"]="different result";changed["response_metadata"]["agent_event"]["data"]["text"]=changed["text"];
        Check(!generic.PublishReply(request,changed,prepare(changed),&published,&error),"text mutation conflicts across transport changes");
        changed=replay;changed["response_metadata"]["agent_event"]["envelope"]["receipt"]["sequence"]=5;
        Check(!generic.PublishReply(request,changed,prepare(changed),&published,&error),"same field name at a different path remains immutable");
        changed=replay;changed["response_metadata"]["project_snapshot"]={{"project_id","spark-push"},{"revision",std::string(64,'a')},{"file_count",2}};
        Check(!generic.PublishReply(request,changed,prepare(changed),&published,&error),"provenance mutation conflicts across transport changes");
        changed=replay;changed["ok"]=false;
        Check(!generic.PublishReply(request,changed,prepare(changed),&published,&error),"status mutation conflicts");
        changed=replay;changed["user_id"]=999;
        Check(!generic.PublishReply(request,changed,replayContent,&published,&error),"actor mismatch rejected before digest");
        changed=replay;changed["collaboration"]["generation"]=999;
        Check(!generic.PublishReply(request,changed,replayContent,&published,&error),"scope mismatch rejected before digest");
        changed=replay;changed["response_metadata"]["agent_event"]["envelope"]["event_id"]="evt_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        Check(!generic.PublishReply(request,changed,prepare(changed),&published,&error),"arbitrary event ID cannot be erased for comparison");
        repo.publications[id]["digest"]=std::string(64,'0');
        Check(!generic.PublishReply(request,replay,replayContent,&published,&error),"legacy reconstruction requires saved hash verification");
        repo.publications.erase(id);repo.removed=true;
        Check(generic.PublishReply(request,first,firstContent,&published,&error) && published["terminal_status"]=="cancelled","v2 denied decision");
        const auto denied=published;repo.removed=false;
        Check(generic.PublishReply(request,replay,replayContent,&published,&error) && published==denied,"v2 cancellation survives transport changes and regrant");
        repo.publications[id]["digest"]=LegacyPublicationDigest(first,firstContent);
        Check(generic.PublishReply(request,first,firstContent,&published,&error) && published==denied,"legacy cancellation exact replay retained");
        Check(!generic.PublishReply(request,replay,replayContent,&published,&error),"unverifiable legacy cancellation variant fails closed");
    }
    repo.removed=true;
    Check(generic.PublishReply(scoped[0],workspaceReply,"canonical original",&published,&error) && published==workspaceReply,"revocation cannot change an already committed final");
    Check(!generic.PublishReply(scoped[0],workspaceReply,"conflicting original",&published,&error),"conflicting replay cannot replace publication");
    auto deniedRequest=scoped[0];deniedRequest["request_id"]="unpublished-final";
    auto deniedReply=workspaceReply;deniedReply["request_id"]="unpublished-final";
    Check(generic.PublishReply(deniedRequest,deniedReply,"unpublished original",&published,&error) && published["ok"]==false && published["terminal_status"]=="cancelled" && published.dump().find("authorized code result")==std::string::npos,"revocation before publication strips code result");
    Check(generic.AuthorizeWorkspace(workspaceKey,{{"request_id",scopedId}},&out).code==403,"each file authorization rechecks actor membership");
    repo.removed=false;
    Json replayDenied;
    Check(generic.PublishReply(deniedRequest,deniedReply,"unpublished original",&replayDenied,&error) && replayDenied==published,"regrant cannot change committed cancellation");
    Check(generic.SetWorkspace(7,1,{{"project_id",""},{"generation",1}},&out).ok() && out["workspace"]["generation"]==2,"clear keeps revocation generation");
    Check(generic.CheckDispatch(scoped[0]).code==403,"cleared workspace blocks queued request");
    Check(generic.SetWorkspace(7,1,{{"project_id","spark-push"},{"generation",2}},&out).ok() && out["workspace"]["generation"]==3,"rebind does not reuse old generation");
    Check(generic.AuthorizeWorkspace(workspaceKey,{{"request_id",scopedId}},&out).code==403,"old request cannot inherit new grant");
    Check(generic.Control(8,1,{{"request_id",scopedId},{"operation","recover"}},&out).code==403,"revoked request recovery cannot execute tools");
    const auto savedRequests=repo.requests, savedPublications=repo.publications;
    generic.DisableConfiguration();
    Check(generic.Agents(8,1,&out).code==503 && generic.Workspace(8,1,&out).code==503,"invalid optional configuration is visible as unavailable");
    Check(generic.Agents(10,1,&out).code==403,"configuration failure does not bypass membership");
    Check(generic.Trigger(8,1,text("ordinary discussion"),&trigger).ok() && !trigger,"configuration failure does not block ordinary chat");
    Check(generic.Trigger(8,1,current.content_json,&trigger).code==503,"configuration failure blocks structured Agent invocation");
    Check(generic.Trigger(8,1,text("@CANN question"),&trigger).code==503,"configuration failure cannot fall back to CANN");
    Check(generic.CheckDispatch(scoped[0]).code==503,"configuration error remains retryable for queued work");
    Check(!generic.PublishReply(scoped[0],workspaceReply,"canonical original",&published,&error),"configuration error defers publication instead of committing cancellation");
    Check(generic.AuthorizeWorkspace(workspaceKey,{{"request_id",scopedId}},&out).code==401,"invalid configuration disables connector authority");
    Check(generic.Control(8,1,{{"request_id",scopedId},{"operation","stop"}},&out).code==503,"invalid configuration does not call the runtime");
    Check(generic.Install(7,1,{{"agent_id","pi"}},true).code==503,"invalid configuration cannot partially install Agents");
    Check(generic.SetWorkspace(7,1,{{"project_id",""},{"generation",3}},&out).code==503,"invalid configuration cannot change workspace");
    Check(repo.requests==savedRequests && repo.publications==savedPublications,"configuration failure preserves requests and publication decisions");
    repo.removed=true;
    Check(generic.Trigger(8,1,current.content_json,&trigger).code==403,"removed member cannot invoke generic Agent");
    Check(!service.BuildRequest(current,{},&retry,&error),"removed actor cannot replay accepted request");
    Check(service.View(8,1,&out).code==403,"removed developer cannot view task");
    repo.fail=true;
    Check(service.Trigger(8,1,text("@CANN question"),&trigger).code==500,"membership error fails closed");
    std::cout << "room Agent service: " << checks << " checks passed\n";
    return 0;
 } catch(const std::exception& e) { std::cerr<<e.what()<<'\n';return 1; }
}
