#include "repositories/media_artifact_repository.h"
#include "repositories/media_repository.h"
#include "repositories/media_storage_quota.h"
#include "security.h"
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <thread>
#include <set>
#include <unistd.h>
#include <sys/stat.h>
using namespace sparkpush;
using room_sql::Query;
using Json=nlohmann::json;
namespace fs=std::filesystem;
void Require(bool ok,const std::string& why) { if(!ok) throw std::runtime_error(why); }
void Status(ServiceStatus s,int code,const std::string& why) {
    Require(s.code==code,why+" code="+std::to_string(s.code)+" "+s.message);
}
std::string Token() { std::string s; Require(GenerateSecureToken(&s),"secure token"); return s; }
template<class F> bool Until(F f) {
    const auto end=std::chrono::steady_clock::now()+std::chrono::seconds(5);
    do { if(f()) return true; std::this_thread::sleep_for(std::chrono::milliseconds(10)); }
    while(std::chrono::steady_clock::now()<end);
    return false;
}
int main() {
    if(!std::getenv("SPARK_PUSH_RUN_MYSQL_TESTS")) return 77;
    MySqlConfig cfg; cfg.host="127.0.0.1"; cfg.user="root"; cfg.db="spark_review_test";
    cfg.port=std::stoi(std::getenv("SPARK_TEST_MYSQL_PORT")); cfg.password=std::getenv("SPARK_TEST_MYSQL_PASSWORD");
    cfg.pool_size=6; cfg.max_pool_size=12;
    MySqlConnectionPool pool; Require(pool.Init(cfg),"pool");
    auto sql=[&](const char* command,const std::vector<std::string>& args=std::vector<std::string>()) {
        auto g=pool.Acquire(); return Query(g.get(),command,args);
    };
    auto ddl=[&](const char* command) {
        // MySQL's prepared-statement protocol does not support CREATE TRIGGER.
        // All statements passed here are fixed test-owned DDL, with no values.
        auto g=pool.Acquire(); Require(mysql_query(g.get(),command)==0,"test trigger DDL");
    };
    auto room=[&] {
        auto g=pool.Acquire(); Query(g.get(),"INSERT INTO im_group(name,group_type,owner_id) VALUES('artifact fixture',1,1)");
        return static_cast<int64_t>(mysql_insert_id(g.get()));
    };
    auto lease=[&](int64_t r) {
        MediaLease l; Require(GenerateAttachmentId(&l.asset),"asset id"); l.revision=Token();l.token=Token();l.attempt=1;
        sql("INSERT INTO attachment(id,owner_user_id,session_id,display_name,mime,bytes,sha256,storage_key,status) "
            "VALUES(?,1,?,'artifact source','application/pdf',1,?,?,'running')",{l.asset,"r_"+std::to_string(r),Token(),l.asset});
        sql("INSERT INTO room_media_asset(asset_id,upload_key,kind) VALUES(?,?,'document')",{l.asset,Token()});
        sql("INSERT INTO room_media_job(asset_id,pipeline,state,attempt,lease_token,lease_until) "
            "VALUES(?,'media-parser-v1','running',1,?,TIMESTAMPADD(SECOND,120,NOW(3)))",{l.asset,l.token});
        sql("INSERT INTO room_media_revision(revision_id,asset_id,attempt,pipeline_sha,lease_token,state) "
            "VALUES(?,?,1,?,?,'running')",{l.revision,l.asset,Token(),l.token});
        return l;
    };
    auto expire=[&](const MediaLease& l) {
        sql("UPDATE room_media_job SET lease_until=TIMESTAMPADD(SECOND,-1,NOW(3)) WHERE asset_id=?",{l.asset});
    };
    // Real 3x2 baseline JPEG generated with the same Pillow encoder, not a magic
    // prefix. Decoder integration and pixel assertions live in parser E2E.
    const std::string encoded="/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjL/wAARCAACAAMDASIAAhEBAxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwDraKKK/KD7Q//Z";
    std::string jpeg; Require(DecodeBase64(encoded,&jpeg),"fixture JPEG");
    Json m={{"mime","image/jpeg"},{"bytes",jpeg.size()},{"sha256",Sha256Hex(jpeg)},
        {"width",3},{"height",2},{"anchor",{{"page",1}}}};
    char pattern[]="/tmp/spark-media-artifact-sql-XXXXXX"; auto* tmp=mkdtemp(pattern);Require(tmp,"temp");
    const fs::path root(tmp);
    try {
        MySqlMediaArtifactRepository derived(&pool,(root/"derived").string()); Require(derived.CheckSchema(),"migration");
        MySqlMediaRepository originals(&pool,(root/"originals").string()); Require(originals.CheckSchema(),"original migration");
        const auto r=room(); auto l=lease(r); Json out;
        Status(derived.Stage(l,0,m,jpeg,&out),409,"unreserved output rejected");
        Status(derived.Reserve(l),0,"reserve"); Status(derived.Reserve(l),0,"reserve replay");
        Require(sql("SELECT COUNT(*) FROM room_media_revision_storage WHERE revision_id=?",{l.revision})[0][0]=="1","double reserve");
        auto wrong=m; wrong["width"]=4; Status(derived.Stage(l,0,wrong,jpeg,&out),400,"forged dimensions");
        Status(derived.Stage(l,1024,m,jpeg,&out),400,"artifact count");
        Status(derived.Stage(l,0,m,jpeg,&out),0,"real JPEG install"); const std::string id=out.at("artifact_id");
        Status(derived.Stage(l,0,m,jpeg,&out),0,"stage replay"); Require(out.at("artifact_id")==id,"unstable file ID");
        wrong=m;wrong["anchor"]["page"]=2; Status(derived.Stage(l,0,wrong,jpeg,&out),409,"metadata conflict");
        Status(derived.Reclaim(l.revision),409,"live GC denied");
        auto stale=l;stale.token=Token(); Status(derived.Stage(stale,1,m,jpeg,&out),409,"old worker token");
        // Connection loss after durable file installation: hold installed UPDATE
        // in an actual trigger and kill that precise MySQL connection.
        auto kill_install=[&](uint64_t index) {
            ddl("CREATE TRIGGER media_artifact_test_delay BEFORE UPDATE ON room_media_artifact FOR EACH ROW "
                "SET @media_artifact_test_delay = IF(NEW.installed=1 AND OLD.installed=0,SLEEP(10),0)");
            ServiceStatus lost;
            std::thread writer([&]{Json result; lost=derived.Stage(l,index,m,jpeg,&result);});
            bool killed=false;
            try {
                killed=Until([&] {
                    // In a trigger MySQL may report the nested SET, not the outer
                    // UPDATE. This isolated database has exactly one sleeping writer.
                    const auto rows=sql("SELECT ID FROM information_schema.PROCESSLIST WHERE STATE='User sleep' "
                        "AND DB=DATABASE() AND ID<>CONNECTION_ID()");
                    if(rows.empty()) return false;
                    Require(rows.size()==1,"ambiguous sleeping writer");
                    const auto connection=std::stoull(rows[0][0]); auto g=pool.Acquire();
                    Require(mysql_query(g.get(),("KILL CONNECTION "+std::to_string(connection)).c_str())==0,"kill writer"); return true;
                });
            } catch(...) { writer.join(); ddl("DROP TRIGGER media_artifact_test_delay"); throw; }
            writer.join(); ddl("DROP TRIGGER media_artifact_test_delay");
            Require(killed,"did not observe blocked installed UPDATE"); Status(lost,503,"SQL connection loss");
        };
        kill_install(1); // First installation loses its SQL acknowledgement.
        kill_install(0); // A previously installed index is replayed and interrupted.
        Require(sql("SELECT installed FROM room_media_artifact WHERE revision_id=? AND artifact_index=0",{l.revision})[0][0]=="0","installed replay hid unfinished IO");
        const auto orphan_id=MySqlMediaArtifactRepository::ArtifactId(l.revision,1);
        Require(fs::exists(root/"derived"/orphan_id),"file was not installed before SQL loss");
        Require(sql("SELECT installed FROM room_media_artifact WHERE revision_id=? AND artifact_index=1",{l.revision})[0][0]=="0","false install receipt");
        // Also model a crash between Assemble and cleanup. Staging a DIFFERENT
        // index must first remove all old chunks/partials, retaining originals.
        MediaBlobStore disk((root/"derived").string()); disk.WriteChunk(orphan_id,0,jpeg); disk.WriteChunk(id,0,jpeg);
        const auto partial=root/"derived"/(orphan_id+".partial.att_"+Token());
        {std::ofstream f(partial);f<<jpeg;}
        Status(derived.Stage(l,2,m,jpeg,&out),0,"next index after crashed assembly");
        Require(!fs::exists(partial) && !fs::exists(root/"derived/media_chunks"/(orphan_id+".0")) &&
            !fs::exists(root/"derived/media_chunks"/(id+".0")) &&
            fs::exists(root/"derived"/orphan_id),"unfinished indexes retained duplicate storage");
        MySqlMediaArtifactRepository restarted(&pool,(root/"derived").string());
        Status(restarted.Stage(l,1,m,jpeg,&out),0,"restart replay"); Require(out.at("artifact_id")==orphan_id,"orphan identity changed");
        Require(sql("SELECT COUNT(*) FROM room_media_artifact WHERE revision_id=?",{l.revision})[0][0]=="3","orphan metadata leaked");
        std::cout<<"real JPEG validation, immutable replay, connection kill and restarted install passed\n";
        // GC must retain its charge if an unlink fails, then converge on retry.
        const auto lock=root/"derived/media_locks"/("att_"+l.revision); struct stat before{},after{};
        Require(stat(lock.c_str(),&before)==0,"stable lock");
        expire(l); fs::remove(root/"derived"/id); fs::create_directory(root/"derived"/id);
        Status(restarted.Reclaim(l.revision),503,"failed cleanup");
        auto charge=sql("SELECT state,charged_bytes FROM room_media_revision_storage WHERE revision_id=?",{l.revision});
        Require(charge[0][0]=="gc_pending" && std::stoull(charge[0][1])==media_storage::kRevisionReservation,"released before cleanup");
        Status(restarted.Stage(l,2,m,jpeg,&out),409,"GC fenced old worker");
        fs::remove(root/"derived"/id);
        ddl("CREATE TRIGGER media_artifact_test_gc BEFORE UPDATE ON room_media_revision_storage FOR EACH ROW "
            "BEGIN IF NEW.state='reclaimed' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected GC receipt failure'; END IF; END");
        Status(restarted.Reclaim(l.revision),503,"GC final SQL failure");
        Require(!fs::exists(root/"derived"/orphan_id),"GC did not remove file before receipt failure");
        charge=sql("SELECT state,charged_bytes FROM room_media_revision_storage WHERE revision_id=?",{l.revision});
        Require(charge[0][0]=="gc_pending" && std::stoull(charge[0][1])==media_storage::kRevisionReservation,"SQL failure released charge");
        Require(sql("SELECT COUNT(*) FROM room_media_artifact WHERE revision_id=?",{l.revision})[0][0]=="3","SQL failure committed partial cleanup receipt");
        ddl("DROP TRIGGER media_artifact_test_gc");
        Status(restarted.Reclaim(l.revision),0,"GC retry");
        Status(restarted.Reclaim(l.revision),0,"GC replay");
        Require(!fs::exists(root/"derived"/orphan_id),"GC left installed file");
        Require(stat(lock.c_str(),&after)==0 && before.st_ino==after.st_ino,"GC unlinked lock inode");
        Require(sql("SELECT charged_bytes FROM room_media_revision_storage WHERE revision_id=?",{l.revision})[0][0]=="0","GC did not release charge");
        // Published/ready data is never reclaimed by this abandoned-attempt GC.
        auto published=lease(r);Status(derived.Reserve(published),0,"reserve published fixture");
        sql("UPDATE room_media_revision SET state='ready' WHERE revision_id=?",{published.revision});expire(published);
        Status(derived.Reclaim(published.revision),409,"ready revision GC denied");
        std::cout<<"GC failure retained quota, retry removed bytes, old worker fenced, lock inode retained\n";
        // Near-limit metadata fixtures exercise real SQL arithmetic/locking; no
        // claim is made that this test writes 10 GiB of file contents.
        const auto qr=room();auto one=lease(qr),two=lease(qr);
        sql("UPDATE attachment SET bytes=? WHERE id=?",{std::to_string(kMediaRoomBytes-media_storage::kRevisionReservation-1),one.asset});
        ServiceStatus left,right;
        std::thread a([&]{left=derived.Reserve(one);}),b([&]{right=derived.Reserve(two);});a.join();b.join();
        Require((left.ok() && right.code==413)||(right.ok() && left.code==413),"concurrent quota over-allocation");
        MediaUpload upload; upload.actor=1;upload.room=qr;upload.session="r_"+std::to_string(qr);
        upload.client_id=Token();upload.name="after derived reservation";upload.mime="text/plain";upload.kind="text";upload.bytes=1;upload.sha256=Sha256Hex("x");RoomMedia original;
        Status(originals.Create(upload,&original),413,"original ignored derived charge");
        const auto winner=left.ok()?one:two;expire(winner);Status(derived.Reclaim(winner.revision),0,"empty revision GC");
        Status(originals.Create(upload,&original),0,"released capacity reusable");
        // Two DIFFERENT writers compete for the same final reservation-sized
        // capacity: new original vs derived output. Exactly one must commit.
        const auto cr=room(); auto competing=lease(cr);
        sql("UPDATE attachment SET bytes=? WHERE id=?",{std::to_string(kMediaRoomBytes-media_storage::kRevisionReservation),competing.asset});
        auto raw=upload;raw.room=cr;raw.session="r_"+std::to_string(cr);raw.client_id=Token();raw.bytes=media_storage::kRevisionReservation;
        ServiceStatus raw_status,derived_status;
        std::thread raw_writer([&]{RoomMedia v;raw_status=originals.Create(raw,&v);});
        std::thread derived_writer([&]{derived_status=derived.Reserve(competing);});
        raw_writer.join();derived_writer.join();
        Require((raw_status.ok() && derived_status.code==413)||(derived_status.ok() && raw_status.code==413),
            "original and derived writers over-allocated quota concurrently");
        const auto total=sql("SELECT (SELECT COALESCE(SUM(a.bytes),0) FROM attachment a JOIN room_media_asset m ON m.asset_id=a.id "
            "WHERE a.session_id=? AND a.status NOT IN ('deleted','expired')) + "
            "(SELECT COALESCE(SUM(charged_bytes),0) FROM room_media_revision_storage WHERE room_id=? AND state<>'reclaimed')",
            {raw.session,std::to_string(cr)});
        Require(std::stoull(total[0][0])==kMediaRoomBytes,"combined quota accounting");
        // Wait until Reserve has performed its initial ordinary lookup and is
        // actually blocked on the room lock. A charge committed now must count.
        const auto sr=room();auto waiting=lease(sr),charge_owner=lease(sr);
        sql("UPDATE attachment SET bytes=? WHERE id=?",{std::to_string(kMediaRoomBytes-media_storage::kRevisionReservation-1),waiting.asset});
        auto held=pool.Acquire();room_sql::Transaction tx(held.get());
        Query(held.get(),"SELECT id FROM im_group WHERE id=? FOR UPDATE",{std::to_string(sr)});
        ServiceStatus waited;std::thread waiter([&]{waited=derived.Reserve(waiting);});
        const bool observed=Until([&]{return sql("SELECT COUNT(*) FROM performance_schema.data_lock_waits w "
            "JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID "
            "WHERE l.OBJECT_NAME='im_group'")[0][0]!="0";});
        Query(held.get(),"INSERT INTO room_media_revision_storage(revision_id,asset_id,room_id,charged_bytes,state) VALUES(?,?,?,?,'reserved')",
            {charge_owner.revision,charge_owner.asset,std::to_string(sr),std::to_string(media_storage::kRevisionReservation)});
        tx.Commit();waiter.join();Require(observed,"room lock wait was not observed");Status(waited,413,"stale quota snapshot");
        std::cout<<"concurrent reservations, simultaneous original/derived writes and actual room-lock wait snapshot test passed\n";
        // Candidate pages are hints; current live/ready checks still fence GC.
        const auto gr=room();auto abandoned=lease(gr),live=lease(gr),ready=lease(gr);
        for(const auto& item:{abandoned,live,ready}) Status(derived.Reserve(item),0,"GC fixture reserve");
        Status(derived.Stage(abandoned,0,m,jpeg,&out),0,"GC fixture actual file");
        expire(abandoned);expire(ready);
        sql("UPDATE room_media_revision SET state='ready' WHERE revision_id=?",{ready.revision});
        std::set<std::string> candidates;std::string gc_cursor;
        for(int page=0;page<100;++page) {
            Json found;Status(derived.ReclaimCandidates(gc_cursor,2,&found),0,"GC candidate page");
            Require(found["items"].size()<=2,"GC unbounded page");
            for(const auto& item:found["items"]) { const auto id=item.get<std::string>();Require(id>gc_cursor,"GC cursor order");candidates.insert(id); }
            if(found["items"].empty())break;
            gc_cursor=found["next_after"].get<std::string>();
        }
        Require(candidates.count(abandoned.revision) && !candidates.count(live.revision) && !candidates.count(ready.revision),"GC candidate authority filter");
        Status(derived.Reclaim(live.revision),409,"candidate caller cannot delete live");
        Status(derived.Reclaim(ready.revision),409,"missing pointer does not authorize ready GC");
        ServiceStatus gc1,gc2;
        std::thread g1([&]{gc1=derived.Reclaim(abandoned.revision);});
        std::thread g2([&]{gc2=derived.Reclaim(abandoned.revision);});
        g1.join();g2.join();Status(gc1,0,"first concurrent GC");Status(gc2,0,"second concurrent GC");
        Require(sql("SELECT charged_bytes,state FROM room_media_revision_storage WHERE revision_id=?",{abandoned.revision})[0]==std::vector<std::string>({"0","reclaimed"}),"GC charge convergence");
        std::cout<<"bounded GC cursor, live/ready exclusion and concurrent actual-file reclamation passed\n";
    } catch(...) { fs::remove_all(root); throw; }
    fs::remove_all(root);
}
