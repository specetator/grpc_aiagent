#include "services/room_agent_service.h"
#include "agent_event.h"
#include "hermes_reply.h"
#include "security.h"
#include <algorithm>
#include <set>
#include <stdexcept>
#include <openssl/sha.h>

namespace sparkpush {
namespace {
using Json=nlohmann::json;
std::string PublicationDigest(const std::string& content,Json result,bool stable) {
    result.erase("completed_at_ms");
    auto message=content;
    if(stable) {
        auto parsed=Json::parse(content,nullptr,false);
        const Json::json_pointer path("/content/agent_event/envelope");
        if(parsed.is_object() && parsed.contains(path)) {
            auto& envelope=parsed[path];
            // Only Bridge-derived transport fields may vary. Validate the ID
            // before omitting it; route, run, actor and every other field stay.
            if(!AgentEnvelopeValid(envelope) || envelope.at("event_id")!=ProjectedEventId(
                envelope.at("request_id"),envelope.at("sequence").get<int64_t>()))
                throw std::invalid_argument("invalid projected final event");
            for(const auto* key:{"event_id","sequence","replayed","created_at_ms"})envelope.erase(key);
            message=parsed.dump();
        }
    }
    Json input={{"message",message},{"result",result}};
    if(stable)input["schema"]="sparkpush.room_publication_digest.v2";
    const auto material=input.dump();
    unsigned char bytes[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(material.data()),material.size(),bytes);
    std::string digest;const char* hex="0123456789abcdef";
    for(auto b:bytes){digest+=hex[b>>4];digest+=hex[b&15];}
    return digest;
}
}
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
    if(configuration_error_)return {503,"room Agent configuration unavailable"};
    int64_t owner=0; bool member=false; std::string error; Json selected;
    if(!repo_->Access(room,actor,&owner,&member,&error) || !repo_->Workspace(room,&selected,&error))return {500,error};
    Json projects=Json::array();
    for(const auto& p:projects_) projects.push_back({{"project_id",p.project_id},{"name",p.display_name}});
    *result={{"projects",projects},{"workspace",selected},{"can_manage",member && owner==actor}};
    return {};
}
ServiceStatus RoomAgentService::SetWorkspace(int64_t actor,int64_t room,const Json& input,Json* result) {
    auto access=CheckMember(actor,room); if(!access.ok())return access;
    if(configuration_error_)return {503,"room Agent configuration unavailable"};
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
    if(configuration_error_) {if(error)*error="room Agent configuration unavailable";return false;}
    for(const auto* key:{"request_id","session_id","user_id","bot_user_id","collaboration"})
        if(!reply.contains(key) || reply.at(key)!=request.at(key))return false;
    const auto result=ResultFromReply(request,reply);
    std::string digest;
    try {digest=PublicationDigest(content,result,true);}
    catch(const std::exception& e){if(error)*error=e.what();return false;}
    const auto legacy=PublicationDigest(content,result,false);
    return repo_->Publication(request,digest,[&](const std::string& saved_digest,const Json& saved){
        if(saved_digest==legacy)return true;
        PreparedHermesReply prepared;std::string prepare_error;
        if(!PrepareHermesReply(saved.dump(),request.at("bot_user_id"),request.at("agent_name"),
                              &prepared,&prepare_error,&request))return false;
        const auto saved_result=ResultFromReply(request,saved);
        // A cancelled decision has already discarded its private input. Never
        // infer that input from the cancellation text or overwrite old hashes.
        if(PublicationDigest(prepared.message.content_json,saved_result,false)!=saved_digest)return false;
        try {return PublicationDigest(prepared.message.content_json,saved_result,true)==digest;}
        catch(const std::exception&){return false;}
    },[&](const Json& state){
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
