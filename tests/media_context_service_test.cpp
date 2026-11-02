#include "services/media_context_service.h"
#include <iostream>
#include <stdexcept>
using namespace sparkpush;
using J=nlohmann::json;
namespace {
int checks=0;
void Check(bool ok,const char* what){++checks;if(!ok)throw std::runtime_error(what);}
class RequestStore final:public RoomAgentRepository {
 public:
    bool removed=false,unavailable=false,installed=true;
    int64_t epoch=0;
    J scope={{"kind","room_agent_v1"},{"room_id",1},{"actor_user_id",7},{"agent_id","pi"},
        {"bot_user_id",100},{"runtime","pi"},{"generation",1},{"project_id",""},{"workspace_generation",0},{"context_generation",0}};
    J request;
    RequestStore(){request={{"request_id","ra_"+std::string(64,'a')},{"agent_id","pi"},{"user_id",7},{"message_seq",10},{"collaboration",scope}};}
    bool Access(int64_t room,int64_t user,int64_t* owner,bool* member,std::string*)override{*owner=7;*member=!removed&&room==1&&user==7;return true;}
    bool Request(const std::string& id,J* out,std::string*)override{*out=id==request.at("request_id")?request:J();return true;}
    bool Bindings(int64_t,J* out,std::string*)override{*out=J::array({{{"agent_id","pi"},{"generation",1},{"installed",installed}}});return true;}
    bool ContextGeneration(int64_t,int64_t* out,std::string*)override{*out=epoch;return !unavailable;}
    bool Load(int64_t,J*,std::string*)override{return false;}
    bool Save(int64_t,int64_t,const J&,std::string*)override{return false;}
    bool PutRequest(const J&,std::string*)override{return false;}
    bool Finish(const std::string&,const J&,std::string*)override{return false;}
    bool Requests(int64_t,J*,std::string*)override{return false;}
};
class Files final:public MediaRepository {
 public:
    std::string session="r_2";
    ServiceStatus Find(const std::string& id,RoomMedia* out)override{out->file.id=id;out->file.session_id=session;return {};}
    ServiceStatus Create(const MediaUpload&,RoomMedia*)override{return {503,"unused"};}
    ServiceStatus PutChunk(const std::string&,uint64_t,const std::string&,RoomMedia*)override{return {503,"unused"};}
    ServiceStatus Finalize(const std::string&,RoomMedia*)override{return {503,"unused"};}
    ServiceStatus Abort(const std::string&,RoomMedia*)override{return {503,"unused"};}
    ServiceStatus List(const std::string&,uint64_t,int,std::vector<RoomMedia>*)override{return {503,"unused"};}
    ServiceStatus Uploads(const std::string&,int64_t,std::vector<RoomMedia>*)override{return {503,"unused"};}
    ServiceStatus Read(const RoomMedia&,uint64_t,uint64_t,std::string*)override{return {503,"unused"};}
};
class Context final:public MediaContextRepository {
 public:
    int reads=0,records=0;std::function<void()> during_read,during_record;
    bool oversized=false;
    ServiceStatus record_status;
    ServiceStatus Messages(const MediaContextScope& scope,const std::string&,int64_t,int64_t,
        uint64_t,uint64_t,uint64_t,J* out)override{
        ++reads;Check(scope.collaboration.at("room_id")==1&&scope.message_ceiling==10,"canonical context");
        *out={{"items",J::array({{{"reference",{{"kind","message"},{"msg_seq",1}}},{"text","protected"},{"total_characters",9}}})},
            {"has_more",false},{"before_seq",1}};
        if(oversized)(*out)["items"][0]["text"]=std::string(2*1024*1024,'x');
        if(during_read)during_read();return {};
    }
    ServiceStatus Media(const MediaContextScope&,const std::string&,uint64_t,uint64_t,J*)override{return {503,"unused"};}
    ServiceStatus Record(const MediaContextScope&,const J& refs,J* out)override{
        ++records;*out=J::array();for(const auto& ref:refs){(void)ref;out->push_back("src_"+std::string(64,'a'));}
        if(during_record)during_record();return record_status;
    }
};
}
int main(){
    try {
        RequestStore requests;Files files;Context repository;
        RoomAgentService agents(&requests,99,true,{{"pi","Pi","pi",100,true}},false);
        const std::string key(64,'b');agents.SetProjectCatalog({},key);
        MediaProcessingService processing(nullptr,&files,nullptr,"","");
        MediaContextService service(&agents,&processing,&files,&repository);
        const J search={{"request_id",requests.request.at("request_id")},{"type","messages"}};J out;
        auto execute=[&](const J& input,int code,const std::string& auth){
            out={{"sentinel","must clear"}};const auto status=service.Execute(auth,"search",input,&out);
            Check(status.code==code,"expected context status");if(code)Check(out.empty(),"failure returned source data");
        };
        execute(search,401,"wrong");Check(repository.reads==0,"invalid key performed read");
        execute(search,0,key);Check(out.at("items")[0].at("source_id")=="src_"+std::string(64,'a'),"receipt absent");
        const auto before_large=repository.records;repository.oversized=true;execute(search,413,key);
        Check(repository.records==before_large,"oversized response created receipts");repository.oversized=false;
        for(const auto* field:{"room_id","actor_user_id","runtime_url","workspace","source_id"}) {
            auto input=search;input[field]="forged";execute(input,400,key);
        }
        for(const auto& value:J::array({-1,true,nullptr,1.0,21,"10",9007199254740992ULL})) {
            auto input=search;input["limit"]=value;execute(input,400,key);
        }
        requests.removed=true;execute(search,403,key);requests.removed=false;
        requests.installed=false;execute(search,403,key);requests.installed=true;
        requests.epoch=1;execute(search,403,key);requests.epoch=0;
        requests.unavailable=true;execute(search,503,key);requests.unavailable=false;
        const auto count=repository.records;
        repository.during_read=[&]{requests.removed=true;};execute(search,403,key);
        Check(repository.records==count,"unauthorized read was recorded");repository.during_read={};requests.removed=false;
        repository.during_record=[&]{requests.removed=true;};execute(search,403,key);
        repository.during_record={};requests.removed=false;
        repository.during_read=[&]{++requests.epoch;};execute(search,403,key);
        repository.during_read={};requests.epoch=0;
        repository.record_status={503,"storage failed"};execute(search,503,key);repository.record_status={};
        auto legacy=requests.request;requests.request["collaboration"].erase("context_generation");execute(search,409,key);requests.request=legacy;
        J cross={{"request_id",search.at("request_id")},{"kind","fragment"},{"asset_id","att_"+std::string(24,'a')},{"revision",std::string(64,'c')}};
        Check(service.Execute(key,"read",cross,&out).code==404&&out.empty(),"cross-room source read");
        cross={{"request_id",search.at("request_id")},{"kind","message"},{"msg_seq",11}};
        Check(service.Execute(key,"read",cross,&out).code==400&&out.empty(),"future message read");
        std::cout<<checks<<" room context service contract checks passed (controlled repositories; no native model)\n";
    }catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
