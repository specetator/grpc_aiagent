#include "services/room_agent_service.h"
#include "agent_event.h"
#include "security.h"
#include <algorithm>
#include <set>
#include <stdexcept>
#include <openssl/sha.h>

namespace sparkpush {
namespace { using Json=nlohmann::json; }
std::vector<ProjectDefinition> ParseRoomProjectCatalog(const Json& catalog) {
    if(!catalog.is_object() || catalog.value("schema",0)!=1 ||
       !catalog.at("projects").is_array() || catalog.at("projects").size()>32)
        throw std::invalid_argument("invalid room project catalog");
    std::vector<ProjectDefinition> result; std::set<std::string> ids;
    for(const auto& entry:catalog.at("projects")) {
        ProjectDefinition p{entry.at("id"),entry.at("name")};
        if(!AgentSlugValid(p.project_id) || !ids.insert(p.project_id).second ||
           p.display_name.empty() || p.display_name.size()>120 ||
           std::any_of(p.display_name.begin(),p.display_name.end(),[](unsigned char c){return c<32 || c==127;}))
            throw std::invalid_argument("invalid room project definition");
        // Runtime roots never enter a client response or an Agent prompt.
        result.push_back(std::move(p));
    }
    return result;
}
void RoomAgentService::SetProjectCatalog(std::vector<ProjectDefinition> projects,std::string key) {
    if((!projects.empty() || !key.empty()) && (key.size()<32 || key.size()>256 || key.find_first_of("\r\n")!=std::string::npos))
        throw std::invalid_argument("workspace authorization key must contain 32..256 bytes");
    projects_=std::move(projects); workspace_key_=std::move(key);
}
ServiceStatus RoomAgentService::Workspace(int64_t actor,int64_t room,Json* result) {
    auto access=CheckMember(actor,room); if(!access.ok()) return access;
    int64_t owner=0; bool member=false; std::string error; Json selected;
    if(!repo_->Access(room,actor,&owner,&member,&error) || !repo_->Workspace(room,&selected,&error))return {500,error};
    Json projects=Json::array();
    for(const auto& p:projects_) projects.push_back({{"project_id",p.project_id},{"name",p.display_name}});
    *result={{"projects",projects},{"workspace",selected},{"can_manage",member && owner==actor}};
    return {};
}
ServiceStatus RoomAgentService::SetWorkspace(int64_t actor,int64_t room,const Json& input,Json* result) {
    auto access=CheckMember(actor,room); if(!access.ok())return access;
    for(auto it=input.begin();it!=input.end();++it)
        if(it.key()!="room_id" && it.key()!="project_id" && it.key()!="generation") return {400,"only a project ID and generation may be selected"};
    if(!input.contains("project_id") || !input["project_id"].is_string() ||
       !input.contains("generation") || !input["generation"].is_number_integer())return {400,"project_id and generation required"};
    const auto id=input["project_id"].get<std::string>(); const auto generation=input["generation"].get<int64_t>();
    if(generation<0 || generation>=9007199254740991LL)return {400,"invalid workspace generation"};
    if(!id.empty() && std::none_of(projects_.begin(),projects_.end(),[&](const auto& p){return p.project_id==id;}))return {403,"project is not allowed"};
    int64_t owner=0;bool member=false;std::string error;
    if(!repo_->Access(room,actor,&owner,&member,&error))return {500,error};
    if(!member || owner!=actor)return {403,"room owner required"};
    if(!repo_->SetWorkspace(room,actor,generation,id,&error))return {409,"workspace changed; reload before editing"};
    return Workspace(actor,room,result);
}
ServiceStatus RoomAgentService::CheckAuthorization(const Json& scope,const Json& state) const {
    if(!state.at("member").get<bool>())return {403,"active room membership required"};
    const auto* d=Definition(scope.at("agent_id"));
    if(!enabled_ || !d || !d->enabled || d->bot_user_id!=scope.at("bot_user_id") || d->runtime!=scope.at("runtime"))
        return {403,"Agent disabled or definition changed"};
    bool installed=false;
    for(const auto& b:state.at("bindings"))
        if(b.at("agent_id")==scope.at("agent_id") && b.value("installed",false) && b.at("generation")==scope.at("generation"))installed=true;
    if(!installed)return {403,"Agent binding revoked"};
    const auto& current=state.at("workspace");const auto id=scope.value("project_id","");
    if(current.at("project_id")!=id || current.at("generation")!=scope.value("workspace_generation",0LL))return {403,"workspace authorization changed"};
    if(!id.empty() && std::none_of(projects_.begin(),projects_.end(),[&](const auto& p){return p.project_id==id;}))return {403,"project removed from catalog"};
    return {};
}
bool RoomAgentService::PublishReply(const Json& request,const Json& reply,const std::string& content,Json* output,std::string* error) {
    const auto& scope=request.at("collaboration");
    // Existing CANN and pre-workspace requests keep their original reservations.
    if(scope.value("kind","")!="room_agent_v1" || !scope.contains("workspace_generation")) {*output=reply;return true;}
    auto result=ResultFromReply(request,reply);result.erase("completed_at_ms");
    const auto material=Json{{"message",content},{"result",result}}.dump();
    unsigned char bytes[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(material.data()),material.size(),bytes);
    std::string digest;const char* hex="0123456789abcdef";
    for(auto b:bytes){digest+=hex[b>>4];digest+=hex[b&15];}
    return repo_->Publication(request,digest,[&](const Json& state){
        if(CheckAuthorization(scope,state).ok())return reply;
        // No original text, tool output or metadata survives a denied decision.
        Json denied;
        for(const auto* field:{"request_id","session_id","user_id","bot_user_id","collaboration","completed_at_ms"})denied[field]=reply.at(field);
        denied["ok"]=false;denied["terminal_status"]="cancelled";
        denied["text"]="工作区或成员授权已撤销，本次结果未发布。";
        return denied;
    },output,error);
}
ServiceStatus RoomAgentService::AuthorizeWorkspace(const std::string& key,const Json& input,Json* output) {
    if(workspace_key_.empty() || !ConstantTimeEquals(key,workspace_key_))return {401,"workspace connector authentication required"};
    if(!input.is_object() || input.size()!=1 || !input.contains("request_id") || !input["request_id"].is_string())return {400,"only request_id is accepted"};
    const auto id=input["request_id"].get<std::string>();
    if(id.size()!=67 || id.rfind("ra_",0)!=0 || id.find_first_not_of("0123456789abcdef",3)!=std::string::npos)return {400,"invalid request_id"};
    Json request;std::string error;
    if(!repo_->Request(id,&request,&error))return {500,error};
    if(!request.is_object() || !request.contains("agent_id"))return {404,"room Agent request not found"};
    auto access=CheckDispatch(request);if(!access.ok())return access;
    *output={{"request_id",id},{"collaboration",request.at("collaboration")}};
    return {};
}
}
