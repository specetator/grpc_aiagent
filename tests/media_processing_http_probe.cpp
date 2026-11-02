// Real HTTP, MySQL repositories, Redis bearer sessions and membership services.
// Other adapters are composed but Kafka/WS/model calls are outside this fixture.
#include "http_server.h"
#include "repositories/dao_repositories.h"
#include "repositories/media_repository.h"
#include "repositories/media_processing_repository.h"
#include "repositories/media_artifact_repository.h"
#include "repositories/media_context_repository.h"
#include "repositories/room_agent_repository.h"
#include "repositories/space_repository.h"
#include "user_dao.h"
#include "audit_log_dao.h"
#include "redis_store.h"
#include "conversation_store.h"
#include "attachment_dao.h"
#include "group_dao.h"
#include <muduo/base/Logging.h>
#include <cstdlib>
#include <stdexcept>
using namespace sparkpush;
int main(int argc,char** argv) {
    if(argc!=3 || !std::getenv("SPARK_PUSH_RUN_MYSQL_TESTS") || !std::getenv("SPARK_TEST_REDIS_PORT")) return 77;
    muduo::Logger::setLogLevel(muduo::Logger::ERROR);
    MySqlConfig sql; sql.host="127.0.0.1"; sql.user="root"; sql.db="spark_review_test";
    sql.port=std::stoi(std::getenv("SPARK_TEST_MYSQL_PORT")); sql.password=std::getenv("SPARK_TEST_MYSQL_PASSWORD");
    MySqlConnectionPool pool; if(!pool.Init(sql)) return 1;
    RedisConfig rc; rc.host="127.0.0.1"; rc.port=std::stoi(std::getenv("SPARK_TEST_REDIS_PORT"));
    RedisConnectionPool redis_pool; if(!redis_pool.Init(rc)) return 1;
    RedisStore redis(&redis_pool); UserDao user_dao(&pool); AuditLogDao audit(&pool);
    DaoUserRepository user_repo(&user_dao,&redis,&audit); UserService users(&user_repo,{});
    GroupDao groups(&pool); GroupMemberDao members(&pool); MySqlSpaceRepository space_repo(&pool);
    SessionDao sessions(&pool); MessageDao message_dao(&pool); UserSessionStateDao states(&pool);
    ConversationStore store(&sessions,&message_dao,&states,&redis);
    DaoConversationRepository conversation_repo(&store,&members);
    ConversationService conversations(&conversation_repo,&users);
    AttachmentDao attachment_dao(&pool,argv[2]); DaoAttachmentRepository attachment_repo(&attachment_dao);
    AttachmentService attachments(&attachment_repo,&conversations);
    MySqlMediaRepository media_repo(&pool,argv[2]); MySqlMediaProcessingRepository processing_repo(&pool);
    if(!media_repo.CheckSchema() || !processing_repo.CheckSchema()) return 1;
    RoomMediaService media(&media_repo,&conversations);
    const auto key=std::getenv("SPARK_PUSH_MEDIA_WORKER_KEY"), pipeline=std::getenv("SPARK_PUSH_MEDIA_PIPELINE_SHA256");
    MySqlMediaArtifactRepository artifacts(&pool,std::string(argv[2])+"/media_derived");
    const auto media_pipeline=std::getenv("SPARK_PUSH_MEDIA_PARSER_PIPELINE_SHA256"), parser_sha=std::getenv("SPARK_PUSH_MEDIA_PARSER_SHA256");
    MediaProcessingService processing(&processing_repo,&media_repo,&conversations,key?key:"",pipeline?pipeline:"",
        artifacts.CheckSchema()?&artifacts:nullptr,media_pipeline?media_pipeline:"",parser_sha?parser_sha:"");
    RoomService rooms(&store,&groups,&members,&redis,&space_repo);
    MySqlRoomAgentRepository agent_repo(&pool,&groups,&members);
    RoomAgentService agents(&agent_repo,90000,true,{{"public-pi","Pi","pi",90001,true}},false);
    OperatorTaskService operator_tasks(&agent_repo);
    const auto context_key=std::getenv("SPARK_PUSH_WORKSPACE_AUTH_KEY");
    agents.SetProjectCatalog({},context_key?context_key:"");
    MySqlMediaContextRepository context_repo(&pool);
    MediaContextService context(&agents,&processing,&media_repo,&context_repo);
    MessageService messages(nullptr,nullptr,&users,nullptr,nullptr,nullptr,nullptr,nullptr,{},1000,nullptr,false,0,{},&attachments);
    AdminService admin(nullptr,nullptr,nullptr); DanmakuService danmaku(nullptr,nullptr,nullptr,nullptr,nullptr); KnowledgeService knowledge("");
    muduo::net::EventLoop loop;
    HttpApiServer server(&loop,muduo::net::InetAddress("127.0.0.1",std::stoi(argv[1])),
        {users,conversations,attachments,messages,rooms,admin,danmaku,knowledge,&agents,&operator_tasks,nullptr,&media,&processing,&context},4);
    server.start(); loop.loop();
}
