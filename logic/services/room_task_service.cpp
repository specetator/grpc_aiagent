#include "services/room_agent_service.h"
#include "security.h"
#include <set>
namespace sparkpush {
namespace {
using Json=nlohmann::json;
bool TextField(const Json& j,const char* field,size_t limit,bool required=false) {
    if(!j.contains(field))return !required;
    return j[field].is_string() && j[field].get_ref<const std::string&>().size()<=limit &&
        (!required || !j[field].get_ref<const std::string&>().empty());
}
bool SourceId(const Json& value) {
    if(!value.is_string())return false;const auto& id=value.get_ref<const std::string&>();
    return id.size()==68 && id.rfind("src_",0)==0 && id.find_first_not_of("0123456789abcdef",4)==std::string::npos;
}
bool SourceIds(const Json& ids) {
    if(!ids.is_array() || ids.size()>32)return false;std::set<std::string> unique;
    for(const auto& id:ids)if(!SourceId(id) || !unique.insert(id.get<std::string>()).second)return false;
    return true;
}
}
ServiceStatus RoomAgentService::Sources(int64_t actor,int64_t room,const Json& input,Json* output) {
    *output=Json::object();auto access=CheckMember(actor,room);if(!access.ok())return access;
    if(!input.is_object())return {400,"invalid source query"};
    for(auto it=input.begin();it!=input.end();++it)
        if(it.key()!="room_id" && it.key()!="request_id" && it.key()!="source_ids" && it.key()!="after" && it.key()!="limit")return {400,"invalid source field"};
    if(!TextField(input,"request_id",121,true))return {400,"request_id required"};
    const auto ids=input.value("source_ids",Json::array());
    if(!SourceIds(ids) || (input.contains("after") && !SourceId(input.at("after"))) ||
       (input.contains("limit") && (!input.at("limit").is_number_integer() || input.at("limit")<1 || input.at("limit")>32)))
        return {400,"invalid bounded source query"};
    if(!ids.empty() && (input.contains("after") || input.contains("limit")))return {400,"select IDs or use pagination"};
    Json result;const auto status=repo_->Sources(room,input.at("request_id"),ids,input.value("after",""),input.value("limit",20ULL),&result);
    if(!status.ok())return status;
    access=CheckMember(actor,room);if(!access.ok())return access;
    int64_t generation=0;std::string error;
    if(!repo_->ContextGeneration(room,&generation,&error))return {503,"room context unavailable"};
    if(result.at("context_generation")!=generation)return {409,"room context changed during source lookup"};
    *output=std::move(result);return {};
}
ServiceStatus RoomAgentService::Tasks(int64_t actor,int64_t room,Json* output) {
    *output=Json::object();
    auto access=CheckMember(actor,room);if(!access.ok())return access;
    std::string error;Json result;if(!repo_->Tasks(room,&result,&error))return {500,error};
    access=CheckMember(actor,room);if(!access.ok())return access;*output=std::move(result);return {};
}
ServiceStatus RoomAgentService::CreateTask(int64_t actor,int64_t room,const Json& input,Json* output) {
    auto access=CheckMember(actor,room);if(!access.ok())return access;
    for(auto it=input.begin();it!=input.end();++it)
        if(it.key()!="room_id" && it.key()!="request_id" && it.key()!="title" && it.key()!="description" && it.key()!="steps" && it.key()!="source_ids")return {400,"invalid task field"};
    if(!TextField(input,"title",240,true)||!TextField(input,"description",16000)||!TextField(input,"request_id",121,true))return {400,"invalid task text"};
    const auto steps=input.value("steps",Json::array());
    if(!steps.is_array()||steps.size()>30)return {400,"invalid task steps"};
    for(const auto& step:steps)if(!step.is_string()||step.get_ref<const std::string&>().size()>2000)return {400,"invalid task step"};
    std::string error;Json request;
    if(!repo_->Request(input.at("request_id"),&request,&error))return {500,error};
    if(!request.is_object()||request.at("collaboration").at("room_id")!=room)return {404,"source request not found in room"};
    const auto ids=input.value("source_ids",Json::array());if(!SourceIds(ids))return {400,"invalid task source IDs"};
    if(!ids.empty()) {
        Json sources;auto status=Sources(actor,room,{{"request_id",request.at("request_id")},{"source_ids",ids}},&sources);
        if(!status.ok())return status;
        for(const auto& source:sources.at("items"))if(!source.at("available").get<bool>())return {409,"task source is no longer available"};
        if(sources.at("items").size()!=ids.size())return {404,"task source not found in original request"};
    }
    std::string id;if(!GenerateSecureToken(&id,&error))return {500,error};
    Json task={{"task_id",id.substr(0,32)},{"room_id",room},{"request_id",request.at("request_id")},
        {"source_msg_id",request.value("source_msg_id",request.at("request_id").get<std::string>())},
        {"created_by",actor},{"assignee_user_id",0},{"title",input.at("title")},{"description",input.value("description","")},
        {"steps",steps},{"source_ids",ids},{"summary",""},{"status","queued"},{"revision",1},{"last_actor_user_id",actor}};
    if(!repo_->SaveTask(room,0,task,&error))return {409,"task conflict"};*output=task;return {};
}
ServiceStatus RoomAgentService::UpdateTask(int64_t actor,int64_t room,const Json& input,Json* output) {
    auto access=CheckMember(actor,room);if(!access.ok())return access;
    for(auto it=input.begin();it!=input.end();++it)
        if(it.key()!="room_id"&&it.key()!="task_id"&&it.key()!="revision"&&it.key()!="status"&&it.key()!="summary"&&it.key()!="claim")return {400,"invalid task update"};
    if(!TextField(input,"task_id",32,true)||!TextField(input,"summary",16000)||!TextField(input,"status",20))return {400,"invalid task update"};
    std::string error;Json task;
    if(!repo_->Task(room,input.at("task_id"),&task,&error))return {500,error};
    if(!task.is_object())return {404,"task not found"};
    int role=-1;if(!repo_->Role(room,actor,&role,&error))return {500,error};
    const auto assignee=task.value("assignee_user_id",0LL);
    const bool claim=input.value("claim",false);
    if(assignee!=actor && role<1 && !(claim&&assignee==0))return {403,"claim an unassigned task before updating"};
    if(claim) {
        if(assignee!=0 && assignee!=actor && role<1)return {403,"task already assigned"};
        task["assignee_user_id"]=actor;
    }
    const int64_t revision=input.at("revision").get<int64_t>();
    if(task.at("revision")!=revision)return {409,"task revision conflict"};
    if(input.contains("status")) {
        const std::set<std::string> states{"queued","running","completed","failed","cancelled","unknown"};
        if(!states.count(input.at("status").get<std::string>()))return {400,"invalid task status"};
        task["status"]=input.at("status");
    }
    if(input.contains("summary"))task["summary"]=input.at("summary");
    task["revision"]=revision+1;task["last_actor_user_id"]=actor;
    if(!repo_->SaveTask(room,revision,task,&error))return {409,"task revision conflict"};*output=task;return {};
}
}
