#include "repositories/media_artifact_repository.h"
#include "repositories/media_storage_quota.h"
#include "security.h"
#include "services/media_manifest.h"
#include <regex>
#include <set>

namespace sparkpush {
namespace {
using Json=nlohmann::json;
using room_sql::Query;
bool Hex(const std::string& s) { return s.size()==64 && s.find_first_not_of("0123456789abcdef")==std::string::npos; }
bool Valid(const MediaLease& l) { return IsValidAttachmentId(l.asset) && Hex(l.revision) && Hex(l.token) && l.attempt>0; }
ServiceStatus Stale() { return {409,"media lease or storage reservation is no longer writable"}; }
ServiceStatus Unavailable() { return {503,"derived media storage unavailable; retry the same identity"}; }
void Require(bool b) { if(!b) throw std::invalid_argument("invalid derived image"); }
uint64_t Number(const Json& j,uint64_t min,uint64_t max) {
    Require(j.is_number_unsigned() || (j.is_number_integer() && j.get<int64_t>()>=0));
    const auto n=j.get<uint64_t>(); Require(n>=min && n<=max); return n;
}
bool Lock(MYSQL* c,const MediaLease& l) {
    if(Query(c,"SELECT id FROM attachment WHERE id=? FOR UPDATE",{l.asset}).empty()) return false;
    if(Query(c,"SELECT asset_id FROM room_media_job WHERE asset_id=? FOR UPDATE",{l.asset}).empty()) return false;
    return !Query(c,"SELECT revision_id FROM room_media_revision WHERE revision_id=? AND asset_id=? "
        "AND lease_token=? AND attempt=? FOR UPDATE",{l.revision,l.asset,l.token,std::to_string(l.attempt)}).empty();
}
bool Live(MYSQL* c,const MediaLease& l) {
    return !Query(c,"SELECT r.revision_id FROM room_media_revision r JOIN attachment a ON a.id=r.asset_id "
        "JOIN room_media_job j ON j.asset_id=r.asset_id WHERE r.revision_id=? AND r.state='running' "
        "AND a.status='running' AND j.state='running' AND j.attempt=r.attempt AND j.lease_token=r.lease_token "
        "AND j.lease_until>NOW(3) AND TIMESTAMPADD(SECOND,3600,r.created_at)>NOW(3)",{l.revision}).empty();
}
bool Reserved(MYSQL* c,const std::string& revision) {
    auto rows=Query(c,"SELECT state,charged_bytes FROM room_media_revision_storage WHERE revision_id=? FOR UPDATE",{revision});
    return !rows.empty() && rows[0][0]=="reserved" && std::stoull(rows[0][1])==media_storage::kRevisionReservation;
}
// Independently inspect our single-frame RGB baseline JPEG contract. Actual
// decoding stays in the isolated parser, not in the privileged Logic process.
void Image(const std::string& bytes,uint64_t width,uint64_t height) {
    auto at=[&](size_t p) { Require(p<bytes.size()); return static_cast<unsigned char>(bytes[p]); };
    Require(bytes.size()>=4 && bytes.size()<=kMediaChunkBytes && at(0)==255 && at(1)==216 &&
            at(bytes.size()-2)==255 && at(bytes.size()-1)==217);
    size_t p=2; bool frame=false;
    while(p<bytes.size()) {
        Require(at(p)==255); while(p<bytes.size() && at(p)==255) ++p;
        const auto marker=at(p++);
        Require(marker==0xc0 || marker==0xc4 || marker==0xdb || marker==0xdd || marker==0xfe || marker==0xda ||
                (marker>=0xe0 && marker<=0xef));
        const size_t length=at(p)*256+at(p+1); Require(length>=2 && length<=bytes.size()-p);
        const size_t start=p+2, count=length-2; p+=length;
        if(marker==0xc0) {
            Require(!frame && count==15 && at(start)==8 && at(start+5)==3 &&
                at(start+6)==1 && at(start+9)==2 && at(start+12)==3);
            Require(static_cast<uint64_t>(at(start+1)*256+at(start+2))==height &&
                static_cast<uint64_t>(at(start+3)*256+at(start+4))==width); frame=true;
        } else if(marker==0xda) {
            Require(frame && count==10 && at(start)==3 && at(start+1)==1 && at(start+3)==2 &&
                at(start+5)==3 && at(start+7)==0 && at(start+8)==63 && at(start+9)==0);
            while(p<bytes.size()) {
                const auto next=bytes.find(static_cast<char>(255),p); Require(next!=std::string::npos); p=next;
                while(p<bytes.size() && at(p)==255) ++p;
                const auto m=at(p++);
                if(m==0xd9) { Require(p==bytes.size()); return; }
                Require(m==0 || (m>=0xd0 && m<=0xd7));
            }
        }
    }
    Require(false);
}
void Metadata(const Json& m,const std::string& bytes) {
    Require(m.is_object() && m.size()==6 && m.contains("mime") && m.contains("bytes") && m.contains("sha256") &&
            m.contains("width") && m.contains("height") && m.contains("anchor"));
    Require(m.at("mime")=="image/jpeg" && Number(m.at("bytes"),1,kMediaChunkBytes)==bytes.size());
    const auto sha=m.at("sha256").get<std::string>(); Require(Hex(sha) && Sha256Hex(bytes)==sha);
    Image(bytes,Number(m.at("width"),1,1600),Number(m.at("height"),1,1600));
    const auto& a=m.at("anchor"); Require(a.is_object() && !a.empty() && a.size()<=2);
    if(a.contains("page")) { Require(a.size()==1); Number(a.at("page"),1,200); }
    else if(a.contains("image")) { Require(a.size()==1); Number(a.at("image"),1,1); }
    else if(a.contains("timestamp_ms")) { Require(a.size()==1); Number(a.at("timestamp_ms"),0,3600000); }
    else {
        Require(a.size()==2 && a.contains("section") && a.contains("paragraph"));
        const auto section=a.at("section").get<std::string>();
        static const std::regex section_name("document|header[0-9]{1,6}|footer[0-9]{1,6}|footnotes|endnotes");
        Require(std::regex_match(section,section_name)); Number(a.at("paragraph"),1,3601000);
    }
}
RoomMedia Blob(const std::string& id,const Json& m) {
    RoomMedia b; b.file.id=id; b.file.mime="image/jpeg";
    b.file.bytes=m.at("bytes"); b.file.sha256=m.at("sha256");
    b.chunks={{0,b.file.bytes,b.file.sha256}}; return b;
}
Json Published(MYSQL* c,const std::string& asset,const std::string& revision) {
    auto rows=Query(c,"SELECT p.revision_id,v.artifact_count FROM room_media_publication p JOIN attachment a ON a.id=p.asset_id "
        "JOIN room_media_revision r ON r.revision_id=p.revision_id AND r.asset_id=a.id JOIN room_media_revision_profile v ON v.revision_id=r.revision_id "
        "JOIN room_media_revision_storage s ON s.revision_id=r.revision_id "
        "WHERE p.asset_id=? AND a.status='ready' AND r.state='ready' AND s.state='sealed'",{asset});
    if(rows.empty() || (!revision.empty() && rows[0][0]!=revision)) return Json();
    return {{"revision",rows[0][0]},{"artifact_count",std::stoull(rows[0][1])}};
}
}  // namespace

std::string MySqlMediaArtifactRepository::ArtifactId(const std::string& revision,uint64_t index) {
    Require(Hex(revision) && index<media_storage::kArtifactCount);
    return "att_"+Sha256Hex(Json::array({"room-media-artifact-v1",revision,index}).dump());
}
bool MySqlMediaArtifactRepository::CheckSchema() {
    try {
        auto g=pool_->Acquire();
        Query(g.get(),"SELECT revision_id,asset_id,room_id,charged_bytes,state,reclaimed_at FROM room_media_revision_storage LIMIT 0");
        Query(g.get(),"SELECT revision_id,artifact_index,artifact_id,sha256,bytes,metadata,installed FROM room_media_artifact LIMIT 0");
        return true;
    } catch(const std::exception&) { return false; }
}
ServiceStatus MySqlMediaArtifactRepository::Reserve(const MediaLease& l) {
    if(!Valid(l)) return {400,"invalid media lease identity"};
    try {
        auto g=pool_->Acquire(); auto* c=g.get(); media_storage::NextTransactionCurrent(c); room_sql::Transaction tx(c);
        // Resolve the immutable room before locks; READ COMMITTED also prevents
        // this lookup from creating an obsolete quota snapshot while we wait.
        auto source=Query(c,"SELECT session_id FROM attachment WHERE id=?",{l.asset});
        if(source.empty() || source[0][0].rfind("r_",0)!=0) return Stale();
        const auto room=std::stoll(source[0][0].substr(2));
        if(Query(c,"SELECT id FROM im_group WHERE id=? FOR UPDATE",{std::to_string(room)}).empty()) return Stale();
        if(!Lock(c,l) || !Live(c,l)) return Stale();
        auto old=Query(c,"SELECT state,charged_bytes FROM room_media_revision_storage WHERE revision_id=? FOR UPDATE",{l.revision});
        if(!old.empty()) { if(!Reserved(c,l.revision)) return Stale(); tx.Commit(); return {}; }
        auto originals=Query(c,"SELECT COALESCE(SUM(a.bytes),0) FROM attachment a JOIN room_media_asset m ON m.asset_id=a.id "
            "WHERE a.session_id=? AND a.status NOT IN ('deleted','expired')",{source[0][0]});
        const auto used=std::stoull(originals[0][0]);
        const auto derived=media_storage::Charged(c,room);
        if(used>kMediaRoomBytes || derived>kMediaRoomBytes-used ||
           media_storage::kRevisionReservation>kMediaRoomBytes-used-derived) return {413,"room media quota reached"};
        Query(c,"INSERT INTO room_media_revision_storage(revision_id,asset_id,room_id,charged_bytes,state) VALUES(?,?,?,?,'reserved')",
            {l.revision,l.asset,std::to_string(room),std::to_string(media_storage::kRevisionReservation)});
        tx.Commit(); return {};
    } catch(const std::exception&) { return Unavailable(); }
}
ServiceStatus MySqlMediaArtifactRepository::Stage(const MediaLease& l,uint64_t index,const Json& m,
                                                const std::string& jpeg,Json* out) {
    if(!out || !Valid(l) || index>=media_storage::kArtifactCount) return {400,"invalid artifact identity"};
    *out=Json::object();
    try { Metadata(m,jpeg); }
    catch(const std::exception&) { return {400,"invalid derived JPEG or metadata"}; }
    try {
        auto file_lock=blobs_.Lock("att_"+l.revision);
        const auto id=ArtifactId(l.revision,index); const auto encoded=m.dump();
        {
            auto g=pool_->Acquire(); auto* c=g.get(); media_storage::NextTransactionCurrent(c); room_sql::Transaction tx(c);
            if(!Lock(c,l) || !Live(c,l) || !Reserved(c,l.revision)) return Stale();
            auto old=Query(c,"SELECT metadata FROM room_media_artifact WHERE revision_id=? AND artifact_index=?",
                {l.revision,std::to_string(index)});
            if(!old.empty()) {
                if(Json::parse(old[0][0])!=m) return {409,"artifact index already contains different content"};
                // Replaying an installed index will write staging files again.
                // Persist FALSE first, so a crash during that replay cannot
                // hide its leftover chunks from the next index's cleanup.
                Query(c,"UPDATE room_media_artifact SET installed=FALSE WHERE revision_id=? AND artifact_index=?",
                    {l.revision,std::to_string(index)});
            } else {
                auto sum=Query(c,"SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM room_media_artifact WHERE revision_id=?",{l.revision});
                const auto used=std::stoull(sum[0][1]);
                if(std::stoull(sum[0][0])>=media_storage::kArtifactCount || used>media_storage::kArtifactBytes ||
                    jpeg.size()>media_storage::kArtifactBytes-used) return {413,"derived image budget exceeded"};
                Query(c,"INSERT INTO room_media_artifact(revision_id,artifact_index,artifact_id,sha256,bytes,metadata) VALUES(?,?,?,?,?,?)",
                    {l.revision,std::to_string(index),id,m.at("sha256"),std::to_string(jpeg.size()),encoded});
            }
            // A failed/ambiguous commit MUST prevent file writes. On retry the
            // same immutable row is found; a new identity is never allocated.
            if(!Live(c,l)) return Stale();
            tx.Commit();
        }
        // A previous process may have died after assembly but before removing
        // its chunk/temporary. Clean EVERY unfinished index before writing a new
        // one, not merely the retried index. Otherwise 128 MiB of artifacts could
        // retain another 128 MiB of chunks across repeated crashes.
        {
            auto g=pool_->Acquire();
            for(const auto& r:Query(g.get(),"SELECT artifact_id,metadata FROM room_media_artifact "
                    "WHERE revision_id=? AND installed=FALSE",{l.revision}))
                blobs_.RemoveStaging(Blob(r[0],Json::parse(r[1])));
        }
        const auto b=Blob(id,m);
        blobs_.WriteChunk(id,0,jpeg); blobs_.Assemble(b); blobs_.RemoveChunks(b);
        {
            auto g=pool_->Acquire(); auto* c=g.get(); media_storage::NextTransactionCurrent(c); room_sql::Transaction tx(c);
            if(!Lock(c,l) || !Live(c,l) || !Reserved(c,l.revision)) return Stale();
            Query(c,"UPDATE room_media_artifact SET installed=TRUE WHERE revision_id=? AND artifact_index=?",
                {l.revision,std::to_string(index)});
            if(!Live(c,l)) return Stale();
            tx.Commit();
        }
        *out={{"artifact_id",id},{"index",index},{"sha256",m.at("sha256")},{"bytes",jpeg.size()}}; return {};
    } catch(const std::exception&) { return Unavailable(); }
}
ServiceStatus MySqlMediaArtifactRepository::ReclaimCandidates(const std::string& after,uint64_t limit,Json* out) {
    if(!out || (!after.empty() && !Hex(after)) || !limit || limit>20) return {400,"invalid reclamation page"};
    try {
        auto g=pool_->Acquire();Json items=Json::array();
        for(const auto& row:Query(g.get(),
            "SELECT r.revision_id FROM room_media_revision_storage s JOIN room_media_revision r ON r.revision_id=s.revision_id "
            "JOIN room_media_job j ON j.asset_id=r.asset_id JOIN attachment a ON a.id=r.asset_id "
            "WHERE s.state<>'reclaimed' AND r.state<>'ready' AND r.revision_id>? "
            "AND NOT EXISTS(SELECT 1 FROM room_media_publication p WHERE p.revision_id=r.revision_id) "
            "AND COALESCE(r.state='running' AND j.state='running' AND j.attempt=r.attempt AND j.lease_token=r.lease_token "
            "AND j.lease_until>NOW(3) AND TIMESTAMPADD(SECOND,3600,r.created_at)>NOW(3) AND a.status='running',0)=0 "
            "ORDER BY r.revision_id LIMIT ?",{after,std::to_string(limit)})) items.push_back(row[0]);
        // Hints only. Reclaim rechecks live/publication under its existing locks.
        *out={{"items",items},{"next_after",items.empty()?std::string():items.back().get<std::string>()}};
        return {};
    } catch(const std::exception&) { return Unavailable(); }
}
ServiceStatus MySqlMediaArtifactRepository::Reclaim(const std::string& revision) {
    if(!Hex(revision)) return {400,"invalid revision"};
    try {
        {
            auto g=pool_->Acquire(); auto* c=g.get(); media_storage::NextTransactionCurrent(c); room_sql::Transaction tx(c);
            auto rows=Query(c,"SELECT asset_id,lease_token,attempt FROM room_media_revision WHERE revision_id=?",{revision});
            if(rows.empty()) return {404,"revision not found"};
            MediaLease l{rows[0][0],revision,rows[0][1],std::stoull(rows[0][2])};
            if(!Lock(c,l)) return Stale();
            auto s=Query(c,"SELECT state FROM room_media_revision_storage WHERE revision_id=? FOR UPDATE",{revision});
            if(s.empty()) return {404,"revision storage not found"};
            if(s[0][0]=="reclaimed") { tx.Commit(); return {}; }
            // Ready revisions remain immutable and charged, even if a broken
            // publication pointer is missing. Revocation needs its own protocol.
            auto r=Query(c,"SELECT state FROM room_media_revision WHERE revision_id=?",{revision});
            if(r[0][0]=="ready" || Live(c,l) || !Query(c,"SELECT asset_id FROM room_media_publication WHERE revision_id=?",{revision}).empty())
                return {409,"live or published revision cannot be reclaimed"};
            Query(c,"UPDATE room_media_revision SET state='expired' WHERE revision_id=? AND state='running'",{revision});
            Query(c,"UPDATE room_media_revision_storage SET state='gc_pending' WHERE revision_id=?",{revision});
            tx.Commit();
        }
        // No SQL lock is held while waiting for a writer. Once pending is
        // committed, that writer may finish IO but cannot mark/install/publish.
        auto file_lock=blobs_.Lock("att_"+revision);
        {
            auto g=pool_->Acquire();
            for(const auto& r:Query(g.get(),"SELECT artifact_id,metadata FROM room_media_artifact WHERE revision_id=?",{revision}))
                blobs_.DiscardUpload(Blob(r[0],Json::parse(r[1])));
        }
        {
            auto g=pool_->Acquire(); auto* c=g.get(); media_storage::NextTransactionCurrent(c); room_sql::Transaction tx(c);
            auto s=Query(c,"SELECT state FROM room_media_revision_storage WHERE revision_id=? FOR UPDATE",{revision});
            if(s.empty() || (s[0][0]!="gc_pending" && s[0][0]!="reclaimed")) return Stale();
            Query(c,"DELETE FROM room_media_artifact WHERE revision_id=?",{revision});
            Query(c,"DELETE FROM room_media_fragment WHERE revision_id=?",{revision});
            Query(c,"UPDATE room_media_revision_storage SET state='reclaimed',charged_bytes=0,reclaimed_at=NOW(3) WHERE revision_id=?",{revision});
            tx.Commit();
        }
        return {};
    } catch(const std::exception&) { return Unavailable(); }
}
ServiceStatus MySqlMediaArtifactRepository::Complete(const MediaLease& l,const Json& summary,Json* out) {
    if(!out || !Valid(l)) return {400,"invalid media completion identity"};
    try {
        Require(summary.is_object() && summary.size()==4 && summary.contains("fragment_count") &&
            summary.contains("artifact_count") && summary.contains("coverage") && summary.contains("manifest_sha256"));
        const auto count=Number(summary.at("fragment_count"),0,4096), images=Number(summary.at("artifact_count"),0,1024);
        const auto manifest=summary.at("manifest_sha256").get<std::string>();Require(Hex(manifest));
        const auto& coverage=summary.at("coverage");Require(coverage.dump().size()<=65536);
        auto file_lock=blobs_.Lock("att_"+l.revision);
        auto g=pool_->Acquire();auto* c=g.get();media_storage::NextTransactionCurrent(c);room_sql::Transaction tx(c);
        auto source=Query(c,"SELECT session_id,mime,bytes,sha256 FROM attachment WHERE id=?",{l.asset});
        if(source.empty() || source[0][0].rfind("r_",0)!=0) return Stale();
        if(Query(c,"SELECT id FROM im_group WHERE id=? FOR UPDATE",{source[0][0].substr(2)}).empty() || !Lock(c,l)) return Stale();
        auto profile=Query(c,"SELECT v.pipeline_kind,v.parser_sha,v.coverage,v.artifact_count,v.artifact_bytes,"
            "r.state,r.manifest_sha,r.fragment_count,r.text_bytes,r.pipeline_sha FROM room_media_revision_profile v "
            "JOIN room_media_revision r ON r.revision_id=v.revision_id WHERE v.revision_id=?",{l.revision});
        if(profile.empty() || profile[0][0]!="media-parser-v1") return {409,"media profile required"};
        const auto& p=profile[0];
        auto receipt=[&](uint64_t text,uint64_t bytes) {
            return Json({{"id",l.asset},{"revision",l.revision},{"state","ready"},{"pipeline_kind",p[0]},
                {"pipeline_sha",p[9]},{"manifest_sha256",manifest},{"fragment_count",count},
                {"artifact_count",images},{"text_bytes",text},{"artifact_bytes",bytes},{"coverage",coverage}});
        };
        if(p[5]=="ready") {
            if(p[6]!=manifest || std::stoull(p[7])!=count || std::stoull(p[3])!=images || Json::parse(p[2])!=coverage)
                return {409,"media receipt conflicts with original completion"};
            *out=receipt(std::stoull(p[8]),std::stoull(p[4]));tx.Commit();return {};
        }
        if(!Live(c,l) || !Reserved(c,l.revision)) return Stale();
        auto fs=Query(c,"SELECT fragment_index,sha256,payload,text_bytes FROM room_media_fragment WHERE revision_id=? ORDER BY fragment_index",{l.revision});
        auto as=Query(c,"SELECT artifact_index,artifact_id,metadata,installed FROM room_media_artifact WHERE revision_id=? ORDER BY artifact_index",{l.revision});
        if(fs.size()!=count || as.size()!=images) return {409,"media manifest is incomplete or has extra content"};
        Json fragments=Json::array(),artifacts=Json::array(),fh=Json::array(),ah=Json::array();uint64_t text_bytes=0,image_bytes=0;
        for(size_t i=0;i<fs.size();++i) {
            if(std::stoull(fs[i][0])!=i) return {409,"fragment indexes are not contiguous"};
            const auto f=Json::parse(fs[i][2]);const auto bytes=f.at("text").get<std::string>().size();
            if(!ValidateMediaFragment(f) || Sha256Hex(f.dump())!=fs[i][1] || bytes!=std::stoull(fs[i][3]))
                return {422,"fragment integrity failure"};
            fragments.push_back(f);fh.push_back(fs[i][1]);text_bytes+=bytes;
        }
        for(size_t i=0;i<as.size();++i) {
            if(std::stoull(as[i][0])!=i || as[i][3]!="1" || as[i][1]!=ArtifactId(l.revision,i))
                return {409,"artifact indexes or installed receipts are incomplete"};
            const auto m=Json::parse(as[i][2]);const auto b=Blob(as[i][1],m);
            // Keep the fixed revision flock throughout verification and commit.
            // No unregistered ID can exist: every writer commits metadata first.
            Metadata(m,blobs_.Read(b,0,b.file.bytes));blobs_.RemoveStaging(b);
            artifacts.push_back(m);ah.push_back(Sha256Hex(m.dump()));image_bytes+=b.file.bytes;
        }
        if(text_bytes>kMediaTextBytes || image_bytes>media_storage::kArtifactBytes ||
            !ValidateMediaCoverage(source[0][1],coverage,fragments,artifacts)) return {422,"media coverage or output budget is invalid"};
        const Json identity={{"mime",source[0][1]},{"bytes",std::stoull(source[0][2])},{"sha256",source[0][3]}};
        if(MediaManifestDigest(identity,p[9],p[1],coverage,fh,ah)!=manifest) return {409,"media manifest digest mismatch"};
        if(!Live(c,l)) return Stale();
        uint64_t changed=0;
        Query(c,"UPDATE room_media_job j JOIN room_media_revision r ON r.asset_id=j.asset_id AND r.attempt=j.attempt "
            "SET j.state='completed',j.lease_until=NULL WHERE r.revision_id=? AND r.state='running' AND j.state='running' "
            "AND j.lease_token=r.lease_token AND j.lease_until>NOW(3) AND TIMESTAMPADD(SECOND,3600,r.created_at)>NOW(3)",{l.revision},&changed);
        if(!changed) return Stale();
        Query(c,"UPDATE room_media_revision SET state='ready',manifest_sha=?,fragment_count=?,text_bytes=? WHERE revision_id=?",
            {manifest,std::to_string(count),std::to_string(text_bytes),l.revision});
        Query(c,"UPDATE room_media_revision_profile SET coverage=?,artifact_count=?,artifact_bytes=? WHERE revision_id=?",
            {coverage.dump(),std::to_string(images),std::to_string(image_bytes),l.revision});
        Query(c,"INSERT INTO room_media_publication(asset_id,revision_id) VALUES(?,?)",{l.asset,l.revision});
        Query(c,"UPDATE attachment SET status='ready' WHERE id=?",{l.asset});
        Query(c,"UPDATE room_media_revision_storage SET state='sealed',charged_bytes=? WHERE revision_id=?",
            {std::to_string(text_bytes+image_bytes),l.revision});
        tx.Commit();*out=receipt(text_bytes,image_bytes);return {};
    } catch(const Json::exception&) {return {400,"invalid media manifest types"};}
      catch(const std::invalid_argument&) {return {422,"media manifest or file integrity failure"};}
      catch(const std::exception&) {return Unavailable();}
}
ServiceStatus MySqlMediaArtifactRepository::Artifacts(const std::string& asset,const std::string& revision,
                                                      uint64_t offset,uint64_t limit,Json* out) {
    try {
        auto g=pool_->Acquire();auto current=Published(g.get(),asset,revision);
        if(current.is_null()) return {409,"media images are not currently published"};
        Json items=Json::array();const std::string rev=current.at("revision");
        for(const auto& r:Query(g.get(),"SELECT artifact_index,artifact_id,metadata FROM room_media_artifact "
            "WHERE revision_id=? AND installed=TRUE AND artifact_index>=? ORDER BY artifact_index LIMIT ?",
            {rev,std::to_string(offset),std::to_string(limit)})) {
            auto item=Json::parse(r[2]);item["artifact_id"]=r[1];item["index"]=std::stoull(r[0]);items.push_back(item);
        }
        if(Published(g.get(),asset,rev).is_null()) return Stale();
        const auto next=offset+items.size();
        *out={{"id",asset},{"revision",rev},{"items",items},{"next_offset",next},{"has_more",next<current.at("artifact_count").get<uint64_t>()}};return {};
    } catch(const std::exception&) {return Unavailable();}
}
ServiceStatus MySqlMediaArtifactRepository::Read(const std::string& asset,const std::string& revision,uint64_t index,Json* out) {
    try {
        auto g=pool_->Acquire();if(Published(g.get(),asset,revision).is_null()) return {409,"media image revision is not current"};
        auto rows=Query(g.get(),"SELECT artifact_id,metadata FROM room_media_artifact WHERE revision_id=? AND artifact_index=? AND installed=TRUE",
            {revision,std::to_string(index)});
        if(rows.empty()) return {404,"media image not found"};
        const auto m=Json::parse(rows[0][1]);const auto b=Blob(rows[0][0],m);const auto bytes=blobs_.Read(b,0,b.file.bytes);
        Metadata(m,bytes);std::string data;EncodeBase64(bytes,&data);
        if(Published(g.get(),asset,revision).is_null()) return {409,"media publication changed during read"};
        *out={{"id",asset},{"revision",revision},{"index",index},{"artifact_id",b.file.id},{"metadata",m},{"data",data}};return {};
    } catch(const std::exception&) {return Unavailable();}
}
}  // namespace sparkpush
