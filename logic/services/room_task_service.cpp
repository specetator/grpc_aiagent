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
}
ServiceStatus RoomAgentService::Tasks(int64_t actor,int64_t room,Json* output) {
    auto access=CheckMember(actor,room);if(!access.ok())return access;
    std::string error;if(!repo_->Tasks(room,output,&error))return {500,error};return {};
}
ServiceStatus RoomAgentService::CreateTask(int64_t actor,int64_t room,const Json& input,Json* output) {
    auto access=CheckMember(actor,room);if(!access.ok())return access;
    for(auto it=input.begin();it!=input.end();++it)
        if(it.key()!="room_id" && it.key()!="request_id" && it.key()!="title" && it.key()!="description" && it.key()!="steps")return {400,"invalid task field"};
    if(!TextField(input,"title",240,true)||!TextField(input,"description",16000)||!TextField(input,"request_id",121,true))return {400,"invalid task text"};
    const auto steps=input.value("steps",Json::array());
    if(!steps.is_array()||steps.size()>30)return {400,"invalid task steps"};
    for(const auto& step:steps)if(!step.is_string()||step.get_ref<const std::string&>().size()>2000)return {400,"invalid task step"};
    std::string error;Json request;
    if(!repo_->Request(input.at("request_id"),&request,&error))return {500,error};
    if(!request.is_object()||request.at("collaboration").at("room_id")!=room)return {404,"source request not found in room"};
    std::string id;if(!GenerateSecureToken(&id,&error))return {500,error};
    Json task={{"task_id",id.substr(0,32)},{"room_id",room},{"request_id",request.at("request_id")},
        {"source_msg_id",request.value("source_msg_id",request.at("request_id").get<std::string>())},
        {"created_by",actor},{"assignee_user_id",0},{"title",input.at("title")},{"description",input.value("description","")},
        {"steps",steps},{"summary",""},{"status","queued"},{"revision",1},{"last_actor_user_id",actor}};
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
