
#include "http_server.h"
#include <muduo/base/Logging.h>
#include <iostream>
class Users final:public sparkpush::UserRepository {
 bool UsersAvailable()const override{return true;}bool TokensAvailable()const override{return true;}
 bool FindById(int64_t id,sparkpush::User* u,std::string*)override{u->id=id;u->status=sparkpush::kUserStatusActive;return true;}
 bool FindByAccount(const std::string&,sparkpush::User*,std::string*)override{return false;}
 bool Create(const std::string&,const std::string&,const std::string&,int64_t*,std::string*)override{return false;}
 bool UpdatePassword(int64_t,const std::string&,std::string*)override{return false;}
 bool ResolveToken(const std::string& s,int64_t* id)override{if(s!="probe")return false;*id=7;return true;}
 bool SaveToken(const std::string&,int64_t,int)override{return false;}
 void Audit(int64_t,int64_t,const std::string&,const std::string&)override{}
};
class Attachments final : public sparkpush::AttachmentRepository {
 public:
 bool Available() const override { return true; }
 bool Create(int64_t,const std::string&,const std::string&,const std::string&,sparkpush::StoredAttachment*,std::string*) override { return false; }
 bool Find(const std::string&,sparkpush::StoredAttachment*,std::string*) override { return false; }
 bool Bind(const std::string&,const std::string&,std::string*) override { return false; }
 bool Read(const sparkpush::StoredAttachment&,std::string*,std::string*) override { return false; }
};
int main(int argc,char** argv){
 muduo::Logger::setLogLevel(muduo::Logger::WARN);Users repo;sparkpush::UserService users(&repo,{});sparkpush::ConversationService conversations(nullptr);
 Attachments attachment_repo; sparkpush::AttachmentService attachments(&attachment_repo,&conversations);sparkpush::MessageService messages(nullptr,nullptr,&users,nullptr,nullptr,nullptr,nullptr,nullptr,{},1000,nullptr,false,0,{},&attachments);
 sparkpush::RoomService rooms(nullptr,nullptr,nullptr,nullptr);sparkpush::AdminService admin(nullptr,nullptr,nullptr);sparkpush::DanmakuService danmaku(nullptr,nullptr,nullptr,nullptr,nullptr);sparkpush::KnowledgeService knowledge("");
 sparkpush::AgentConnectionService::Transport connection_transport=[](const nlohmann::json& input) {
   return nlohmann::json{{"code",0},{"data",{{"actor_user_id",input.at("actor_user_id")}}}};
 };
 if (argc > 2) connection_transport=sparkpush::AgentConnectionService::LocalGateway(std::stoi(argv[2]),"test-gateway-key");
 sparkpush::AgentConnectionService connections(std::move(connection_transport));
 muduo::net::EventLoop loop;sparkpush::HttpApiServer server(&loop,muduo::net::InetAddress("127.0.0.1",std::stoi(argv[1])),{users,conversations,attachments,messages,rooms,admin,danmaku,knowledge,nullptr,nullptr,&connections});server.start();loop.loop();
}
