#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <fstream>
#include <thread>
#include <unistd.h>
#include "attachment_dao.h"
#include "conversation_store.h"
#include "group_dao.h"
#include "repositories/dao_repositories.h"
#include "repositories/media_repository.h"
#include "repositories/room_sql.h"
#include "repositories/space_repository.h"
#include "services/attachment_service.h"
#include "services/media_service.h"
#include "services/room_service.h"
using namespace sparkpush;
using Json=nlohmann::json;
void Require(bool ok,const std::string& why) { if(!ok) throw std::runtime_error(why); }
std::string Encoded(const std::string& value) { std::string out; EncodeBase64(value,&out); return out; }
int main() {
    if(!std::getenv("SPARK_PUSH_RUN_MYSQL_TESTS")) return 77;
    MySqlConfig config; config.host="127.0.0.1"; config.user="root"; config.db="spark_review_test";
    config.port=std::stoi(std::getenv("SPARK_TEST_MYSQL_PORT")); config.password=std::getenv("SPARK_TEST_MYSQL_PASSWORD");
    MySqlConnectionPool pool; Require(pool.Init(config),"pool init");
    int64_t a,b,c;
    {
        auto guard=pool.Acquire();
        for(auto* user:{&a,&b,&c}) {
            room_sql::Query(guard.get(),"INSERT INTO user(account,password_hash) VALUES(UUID(),'test')");
            *user=mysql_insert_id(guard.get());
        }
    }
    MySqlSpaceRepository spaces(&pool); GroupDao groups(&pool); GroupMemberDao members(&pool);
    RoomService rooms(nullptr,&groups,&members,nullptr,&spaces);
    int64_t room=0,other=0;
    Require(rooms.CreateSpace(a,"media","","private",&room).ok(),"create room");
    Require(rooms.CreateSpace(c,"other","","private",&other).ok(),"create other room");
    Require(rooms.Invite(a,room,b).ok() && rooms.Accept(b,room).ok(),"accept member");
    SessionDao sessions(&pool); MessageDao messages(&pool); UserSessionStateDao states(&pool);
    ConversationStore store(&sessions,&messages,&states,nullptr);
    DaoConversationRepository conversation_repo(&store,&members);
    ConversationService conversations(&conversation_repo);
    char pattern[]="/tmp/spark-media-mysql-XXXXXX";
    const char* created=mkdtemp(pattern); Require(created,"temporary directory");
    const std::filesystem::path root(created);
    try {
        MySqlMediaRepository repo(&pool,root.string()); Require(repo.CheckSchema(),"media migration");
        RoomMediaService media(&repo,&conversations);
        Json out;
        auto call=[&](int64_t user,const std::string& op,const Json& in,int expected=0) {
            auto status=media.Execute(user,op,in,&out);
            Require(status.code==expected,op+": "+status.message+" code="+std::to_string(status.code));
            return out;
        };
        const std::string first(kMediaChunkBytes,'x'), last="会议末尾：保留原始请求 ID。";
        const std::string full=first+last;
        Json init={{"room_id",room},{"client_upload_id","test-upload-0001"},
            {"name","O'Reilly '; DROP TABLE attachment; -- 设计.md"},{"mime","text/markdown"},
            {"bytes",full.size()},{"sha256",Sha256Hex(full)}};
        call(c,"init",init,403);
        auto result=call(a,"init",init); const std::string id=result.at("id");
        Require(result.at("state")=="uploading","premature ready");
        Require(call(a,"uploads",{{"room_id",room}}).at("items").size()==1,"cannot rediscover own upload");
        Require(call(b,"uploads",{{"room_id",room}}).at("items").empty(),"another user's draft disclosed");
        Require(call(a,"init",init).at("id")==id,"init replay changed ID");
        auto conflict=init; conflict["name"]="different"; call(a,"init",conflict,409);
        auto injected=init; injected["runtime_url"]="http://untrusted"; call(a,"init",injected,400);
        call(b,"status",{{"id",id}},403);
        Require(call(b,"list",{{"room_id",room}}).at("items").empty(),"draft leaked to room library");
        call(a,"finish",{{"id",id}},409);
        call(a,"chunk",{{"id",id},{"index",1},{"data",Encoded(last)}});
        call(a,"chunk",{{"id",id},{"index",1},{"data",Encoded(last)}});
        call(a,"chunk",{{"id",id},{"index",1},{"data",Encoded(std::string(last.size(),'q'))}},409);
        call(a,"chunk",{{"id",id},{"index",0},{"data","eA==junk"}},400);
        call(a,"chunk",{{"id",id},{"index",0},{"data",Encoded(first)}});
        call(b,"finish",{{"id",id}},403);
        // Concurrent finish requests must publish one original and enqueue one job.
        ServiceStatus left,right; Json left_out,right_out;
        std::thread t1([&]{left=media.Execute(a,"finish",{{"id",id}},&left_out);});
        std::thread t2([&]{right=media.Execute(a,"finish",{{"id",id}},&right_out);});
        t1.join();t2.join(); Require(left.ok() && right.ok(),"concurrent finalize");
        Require(call(a,"finish",{{"id",id}}).at("state")=="queued","upload incorrectly marked parsed");
        {
            auto guard=pool.Acquire();
            auto jobs=room_sql::Query(guard.get(),"SELECT COUNT(*) FROM room_media_job WHERE asset_id=?",{id});
            Require(jobs[0][0]=="1","duplicate processing job");
        }
        Require(call(b,"list",{{"room_id",room}}).at("items").size()==1,"shared library");
        Require(call(b,"read",{{"id",id},{"offset",kMediaChunkBytes},{"length",kMediaChunkBytes}}).at("data")==Encoded(last),"late content range");
        call(c,"read",{{"id",id}},403);
        call(b,"read",{{"id",id},{"offset",full.size()}},416);
        call(b,"read",{{"id",id},{"length",kMediaChunkBytes+1}},400);
        MySqlMediaRepository resumed_repo(&pool,root.string()); RoomMediaService resumed(&resumed_repo,&conversations);
        Require(resumed.Execute(b,"read",{{"id",id},{"offset",kMediaChunkBytes}},&out).ok() && out.at("data")==Encoded(last),"restart lost original");
        AttachmentDao old_dao(&pool,root.string()); DaoAttachmentRepository old_repo(&old_dao);
        AttachmentService old_service(&old_repo,&conversations); AttachmentContents legacy;
        Require(!old_service.Read(b,id,&legacy).ok(),"legacy image API exposed non-ready media");
        Require(rooms.Remove(a,room,b).ok(),"revoke member");
        for(const auto& op:{"status","read","finish","chunk"}) call(b,op,{{"id",id}},403);
        call(b,"list",{{"room_id",room}},403);
        call(b,"init",init,403);
        // SQL binding, oversized MIME column (DOCX), hash failure and cancellation.
        Json bad=init; bad["client_upload_id"]="test-upload-0002"; bad["bytes"]=4; bad["sha256"]=Sha256Hex("fail");
        const std::string bad_id=call(a,"init",bad).at("id");
        call(a,"chunk",{{"id",bad_id},{"index",0},{"data",Encoded("pass")}});
        call(a,"finish",{{"id",bad_id}},422);
        Require(!std::filesystem::exists(root/bad_id),"digest failure published file");
        call(a,"abort",{{"id",bad_id}}); call(a,"chunk",{{"id",bad_id},{"index",0},{"data",Encoded("fail")}},409);
        Json docx=init; docx["client_upload_id"]="test-upload-0003";
        docx["mime"]="application/vnd.openxmlformats-officedocument.wordprocessingml.document";
        const std::string docx_id=call(a,"init",docx).at("id");
        Require(call(a,"status",{{"id",docx_id}}).at("mime")==docx.at("mime"),"DOCX MIME truncated");
        call(a,"abort",{{"id",docx_id}});
        // Simulate publication before a failed SQL transaction: files exist but
        // the database still describes an empty draft. Abort must reclaim them.
        Json orphan=bad; orphan["client_upload_id"]="test-upload-0004"; orphan["sha256"]=Sha256Hex("pass");
        const std::string orphan_id=call(a,"init",orphan).at("id");
        RoomMedia draft; Require(repo.Find(orphan_id,&draft).ok(),"load draft");
        draft.chunks={{0,4,Sha256Hex("pass")}};
        MediaBlobStore blobs(root.string());
        blobs.WriteChunk(orphan_id,0,"pass"); blobs.Assemble(draft);
        std::ofstream(root/(orphan_id+".partial.crashed")) << "partial";
        std::ofstream(root/"media_chunks"/(orphan_id+".0.partial.crashed")) << "partial";
        {
            // A stale file writer can outlive its SQL transaction. A second
            // repository must not delete files until that writer releases flock.
            auto stale_writer=blobs.Lock(orphan_id);
            auto disconnected=pool.Acquire();
            room_sql::Transaction stale_transaction(disconnected.get());
            room_sql::Query(disconnected.get(),"SELECT id FROM attachment WHERE id=? FOR UPDATE",{orphan_id});
            {
                auto killer=pool.Acquire();
                Require(mysql_kill(killer.get(),mysql_thread_id(disconnected.get()))==0,"kill owned MySQL connection");
                Require(!room_sql::Query(killer.get(),"SELECT id FROM attachment WHERE id=? FOR UPDATE NOWAIT",{orphan_id}).empty(),"SQL lock remained after disconnect");
            }
            MySqlMediaRepository second(&pool,root.string()); RoomMedia attempted;
            Require(second.Abort(orphan_id,&attempted).code==503,"concurrent repository bypassed file lock");
            Require(std::filesystem::exists(root/orphan_id),"abort deleted active writer output");
        }
        {
            auto guard=pool.Acquire();
            room_sql::Query(guard.get(),"UPDATE attachment SET status='cancelling' WHERE id=?",{orphan_id});
        }
        call(a,"finish",{{"id",orphan_id}},409);
        call(a,"chunk",{{"id",orphan_id},{"index",0},{"data",Encoded("pass")}},409);
        call(a,"read",{{"id",orphan_id}},409);
        call(c,"status",{{"id",orphan_id}},403);
        Require(call(a,"uploads",{{"room_id",room}}).at("items").size()==1,"cancelling upload not discoverable");
        call(a,"abort",{{"id",orphan_id}});
        Require(!std::filesystem::exists(root/orphan_id) &&
                !std::filesystem::exists(root/"media_chunks"/(orphan_id+".0")) &&
                !std::filesystem::exists(root/(orphan_id+".partial.crashed")) &&
                !std::filesystem::exists(root/"media_chunks"/(orphan_id+".0.partial.crashed")),"abort leaked uncommitted files");
        Require(call(a,"uploads",{{"room_id",room}}).at("items").empty(),"cancelled uploads occupy slots");
        // Real filesystem cleanup failure must persist cancelling, keep its
        // quota slot and remain private, until the uploader retries successfully.
        Json blocked=orphan; blocked["client_upload_id"]="test-upload-blocked";
        const std::string blocked_id=call(a,"init",blocked).at("id");
        const auto obstruction=root/"media_chunks"/(blocked_id+".0");
        std::filesystem::create_directory(obstruction);
        call(a,"abort",{{"id",blocked_id}},503);
        Require(call(a,"status",{{"id",blocked_id}}).at("state")=="cancelling","cleanup failure lost durable state");
        Require(rooms.Invite(a,room,b).ok() && rooms.Accept(b,room).ok(),"restore member for draft privacy check");
        call(b,"status",{{"id",blocked_id}},403);
        Require(call(b,"list",{{"room_id",room}}).at("items").size()==1,"cancelling draft leaked into shared library");
        std::vector<std::string> extra_ids;
        for(int i=0;i<3;++i) {
            blocked["client_upload_id"]="test-quota-upload-"+std::to_string(i);
            extra_ids.push_back(call(a,"init",blocked).at("id"));
        }
        blocked["client_upload_id"]="test-quota-fifth-upload";
        call(a,"init",blocked,429);
        std::filesystem::remove(obstruction);
        call(a,"abort",{{"id",blocked_id}});
        extra_ids.push_back(call(a,"init",blocked).at("id"));
        for(const auto& extra:extra_ids) call(a,"abort",{{"id",extra}});
        Require(call(a,"uploads",{{"room_id",room}}).at("items").empty(),"resumed cleanup did not release slots");
        Require(call(a,"status",{{"id",id}}).at("name")==init.at("name"),"SQL special characters changed");
        std::cout << "real MySQL room media: resume, hashes, concurrent finalize, permissions, restart and SQL binding passed\n";
    } catch(...) { std::filesystem::remove_all(root); throw; }
    std::filesystem::remove_all(root);
}
