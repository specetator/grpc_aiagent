#include "repositories/media_processing_repository.h"
#include "repositories/room_sql.h"
#include "repositories/media_storage_quota.h"
#include "services/media_manifest.h"
#include "security.h"
#include <algorithm>

namespace sparkpush {
namespace {
using Json = nlohmann::json;
using room_sql::Query;
ServiceStatus Unavailable() { return {503,"media processing storage unavailable"}; }
ServiceStatus Stale() { return {409,"media lease expired or superseded"}; }
// All mutators lock asset -> job/revision -> fragments. Claim alone takes its
// immutable assignment row first. No completion path locks that row backwards.
Json Assignment(MYSQL* conn, const std::string& revision) {
    auto rows=Query(conn,"SELECT r.asset_id,r.attempt,r.lease_token,r.state,r.pipeline_sha,"
        "r.state='running' AND j.state='running' AND j.attempt=r.attempt AND j.lease_token=r.lease_token "
        "AND j.lease_until>NOW(3) AND TIMESTAMPADD(SECOND,3600,r.created_at)>NOW(3) "
        "AND a.status='running',a.mime,a.bytes,a.sha256,COALESCE(p.pipeline_kind,CASE WHEN a.mime IN ('text/plain','text/markdown') THEN 'media-text-v1' ELSE 'invalid' END),COALESCE(p.parser_sha,''), "
        "GREATEST(0,TIMESTAMPDIFF(SECOND,NOW(3),TIMESTAMPADD(SECOND,3600,r.created_at))) "
        "FROM room_media_revision r JOIN room_media_job j ON j.asset_id=r.asset_id "
        "JOIN attachment a ON a.id=r.asset_id LEFT JOIN room_media_revision_profile p ON p.revision_id=r.revision_id WHERE r.revision_id=?",{revision});
    if(rows.empty()) return Json();
    const auto& r=rows[0];
    return {{"id",r[0]},{"attempt",std::stoull(r[1])},{"lease_token",r[2]},
        {"revision",revision},{"state",r[3]},{"pipeline_sha",r[4]},{"live",r[5]=="1"},
        {"mime",r[6]},{"bytes",std::stoull(r[7])},{"sha256",r[8]},
        {"lease_seconds",120},{"attempt_seconds",3600},{"pipeline_kind",r[9]},{"parser_sha256",r[10]},{"remaining_seconds",std::stoull(r[11])}};
}
bool Lock(MYSQL* conn,const MediaLease& lease) {
    if(Query(conn,"SELECT id FROM attachment WHERE id=? FOR UPDATE",{lease.asset}).empty()) return false;
    if(Query(conn,"SELECT job_id FROM room_media_job WHERE asset_id=? FOR UPDATE",{lease.asset}).empty()) return false;
    return !Query(conn,"SELECT revision_id FROM room_media_revision WHERE revision_id=? AND asset_id=? "
        "AND attempt=? AND lease_token=? FOR UPDATE",
        {lease.revision,lease.asset,std::to_string(lease.attempt),lease.token}).empty();
}
bool Live(MYSQL* conn,const MediaLease& lease) {
    const auto value=Assignment(conn,lease.revision);
    return !value.is_null() && value.at("id")==lease.asset && value.at("attempt")==lease.attempt &&
        ConstantTimeEquals(value.at("lease_token"),lease.token) && value.at("live").get<bool>();
}
Json Receipt(MYSQL* conn,const std::string& revision) {
    auto r=Query(conn,"SELECT asset_id,manifest_sha,fragment_count,text_bytes,line_count,pipeline_sha "
        "FROM room_media_revision WHERE revision_id=? AND state='ready'",{revision});
    if(r.empty()) return Json();
    return {{"id",r[0][0]},{"revision",revision},{"manifest_sha256",r[0][1]},
        {"fragment_count",std::stoull(r[0][2])},{"text_bytes",std::stoull(r[0][3])},
        {"line_count",std::stoull(r[0][4])},{"pipeline_sha",r[0][5]},{"state","ready"}};
}
}  // namespace

bool MySqlMediaProcessingRepository::CheckSchema() {
    try {
        auto guard=pool_->Acquire();
        for(const auto* sql:{"SELECT claim_key,revision_id,pipeline_sha FROM room_media_claim LIMIT 0",
            "SELECT revision_id,attempt,manifest_sha FROM room_media_revision LIMIT 0",
            "SELECT revision_id,payload,text_bytes FROM room_media_fragment LIMIT 0",
            "SELECT asset_id,revision_id FROM room_media_publication LIMIT 0",
            "SELECT revision_id,pipeline_kind,parser_sha,coverage,artifact_count,artifact_bytes FROM room_media_revision_profile LIMIT 0"}) Query(guard.get(),sql);
        return true;
    } catch(const std::exception&) { return false; }
}

ServiceStatus MySqlMediaProcessingRepository::Claim(const std::string& key,const std::string& pipeline,bool allocate,Json* out,const std::string& parser_sha) {
    const bool media=!parser_sha.empty();
    const std::string kind=media?"media-parser-v1":"media-text-v1";
    auto compatible=[&](const Json& v) { return !v.is_null() && v.at("pipeline_kind")==kind && v.at("parser_sha256")==parser_sha; };
    try {
        auto guard=pool_->Acquire(); auto* conn=guard.get(); media_storage::NextTransactionCurrent(conn); room_sql::Transaction tx(conn);
        if(!allocate) {
            const auto old=Query(conn,"SELECT revision_id,pipeline_sha FROM room_media_claim WHERE claim_key=?",{key});
            if(old.empty()) return {410,"expired claim identity has no recoverable assignment"};
            if(old[0][1]!=pipeline) return {409,"claim belongs to a different pipeline"};
            *out=Assignment(conn,old[0][0]); if(!compatible(*out)) return {409,"claim profile mismatch"}; tx.Commit(); return {};
        }
        // Insert before locking candidates: identical concurrent claims serialize
        // on this one identity. Never reassign an expired claim to another asset.
        Query(conn,"INSERT INTO room_media_claim(claim_key,pipeline_sha) VALUES(?,?) "
            "ON DUPLICATE KEY UPDATE claim_key=claim_key",{key,pipeline});
        auto prior=Query(conn,"SELECT revision_id,pipeline_sha FROM room_media_claim WHERE claim_key=? FOR UPDATE",{key});
        if(prior[0][1]!=pipeline) return {409,"claim belongs to a different pipeline"};
        if(!prior[0][0].empty()) {
            *out=Assignment(conn,prior[0][0]); if(!compatible(*out)) return {409,"claim profile mismatch"}; tx.Commit(); return {};
        }
        const std::string mime_filter=media?
            "a.mime IN ('application/pdf','application/vnd.openxmlformats-officedocument.wordprocessingml.document','image/png','image/jpeg','image/webp','image/gif','audio/wav','audio/mpeg','audio/flac','audio/ogg','audio/mp4','audio/webm','video/mp4','video/webm','video/ogg')":
            "a.mime IN ('text/plain','text/markdown')";
        const auto query="SELECT a.id FROM attachment a JOIN room_media_job j ON j.asset_id=a.id WHERE "+mime_filter+
            " AND a.status IN ('queued','running') AND (j.state='queued' OR (j.state='running' AND (j.lease_until<=NOW(3) OR "
            "EXISTS(SELECT 1 FROM room_media_revision r WHERE r.asset_id=j.asset_id AND r.attempt=j.attempt "
            "AND (r.state='expired' OR TIMESTAMPADD(SECOND,3600,r.created_at)<=NOW(3)))))) ORDER BY j.job_id LIMIT 32";
        auto candidates=Query(conn,query.c_str());
        for(const auto& candidate:candidates) {
            const auto id=candidate[0];
            auto asset=Query(conn,"SELECT status,mime FROM attachment WHERE id=? FOR UPDATE",{id});
            if(asset.empty() || (asset[0][0]!="queued" && asset[0][0]!="running")) continue;
            auto jobs=Query(conn,"SELECT attempt,state,lease_until>NOW(3) FROM room_media_job WHERE asset_id=? FOR UPDATE",{id});
            if(jobs.empty()) continue;
            const auto attempt=std::stoull(jobs[0][0]);
            if(jobs[0][1]=="running" && jobs[0][2]=="1") {
                auto deadline=Query(conn,"SELECT 1 FROM room_media_revision WHERE asset_id=? AND attempt=? "
                    "AND (state='expired' OR TIMESTAMPADD(SECOND,3600,created_at)<=NOW(3))",{id,std::to_string(attempt)});
                if(deadline.empty()) continue;
            } else if(jobs[0][1]!="queued" && jobs[0][1]!="running") continue;
            // Local text extraction has no external side effects. Abandon only
            // its unpublished staging; no current published revision is touched.
            Query(conn,"UPDATE room_media_revision SET state='expired' WHERE asset_id=? AND state='running'",{id});
            Query(conn,"DELETE f FROM room_media_fragment f JOIN room_media_revision r ON r.revision_id=f.revision_id "
                "WHERE r.asset_id=? AND r.state IN ('expired','failed')",{id});
            uint64_t consumed=attempt;
            if(media) consumed=std::stoull(Query(conn,"SELECT COUNT(*) FROM room_media_revision_storage WHERE asset_id=?",{id})[0][0]);
            if(consumed>=3 || attempt>=4294967295ULL) {
                Query(conn,"UPDATE room_media_job SET state='failed',lease_until=NULL,error_code='attempts_exhausted',"
                    "error_message='processing worker repeatedly lost its lease' WHERE asset_id=?",{id});
                Query(conn,"UPDATE attachment SET status='failed' WHERE id=?",{id});
                continue;
            }
            std::string revision,token;
            if(!GenerateSecureToken(&revision) || !GenerateSecureToken(&token)) return Unavailable();
            Query(conn,"INSERT INTO room_media_revision(revision_id,asset_id,attempt,pipeline_sha,lease_token,state) "
                "VALUES(?,?,?,?,?,'running')",{revision,id,std::to_string(attempt+1),pipeline,token});
            if(media) Query(conn,"INSERT INTO room_media_revision_profile(revision_id,pipeline_kind,parser_sha) VALUES(?,?,?)",{revision,kind,parser_sha});
            Query(conn,"UPDATE room_media_job SET state='running',attempt=?,pipeline=?,lease_token=?,"
                "lease_until=TIMESTAMPADD(SECOND,120,NOW(3)),error_code='',error_message='' WHERE asset_id=?",
                {std::to_string(attempt+1),kind,token,id});
            Query(conn,"UPDATE attachment SET status='running' WHERE id=?",{id});
            Query(conn,"UPDATE room_media_claim SET revision_id=? WHERE claim_key=?",{revision,key});
            *out=Assignment(conn,revision); tx.Commit(); return {};
        }
        // No allocation was made. Drop this empty polling identity, but commit
        // terminal transitions for exhausted jobs visited in this scan.
        Query(conn,"DELETE FROM room_media_claim WHERE claim_key=?",{key});
        *out={{"idle",true}}; tx.Commit(); return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaProcessingRepository::Check(const MediaLease& lease,Json* out,bool require_live) {
    try {
        auto guard=pool_->Acquire();
        auto value=Assignment(guard.get(),lease.revision);
        if(value.is_null() || value.at("id")!=lease.asset || value.at("attempt")!=lease.attempt ||
           !ConstantTimeEquals(value.at("lease_token"),lease.token) || (require_live && !value.at("live").get<bool>())) return Stale();
        *out=std::move(value); return {};
    } catch(const std::exception&) { return Unavailable(); }
}
ServiceStatus MySqlMediaProcessingRepository::Heartbeat(const MediaLease& lease) {
    try {
        auto guard=pool_->Acquire(); auto* conn=guard.get(); room_sql::Transaction tx(conn);
        if(!Lock(conn,lease) || !Live(conn,lease)) return Stale();
        uint64_t changed=0;
        Query(conn,"UPDATE room_media_job j JOIN room_media_revision r ON r.asset_id=j.asset_id AND r.attempt=j.attempt "
            "SET j.lease_until=LEAST(TIMESTAMPADD(SECOND,120,NOW(3)),TIMESTAMPADD(SECOND,3600,r.created_at)) "
            "WHERE r.revision_id=? AND j.lease_until>NOW(3) AND TIMESTAMPADD(SECOND,3600,r.created_at)>NOW(3)",
            {lease.revision},&changed);
        // Identical millisecond heartbeat is harmless; recheck the predicate.
        if(!Live(conn,lease)) return Stale();
        tx.Commit(); return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaProcessingRepository::Stage(const MediaLease& lease,uint64_t index,const Json& fragment,Json* out) {
    try {
        auto guard=pool_->Acquire(); auto* conn=guard.get(); room_sql::Transaction tx(conn);
        if(!Lock(conn,lease) || !Live(conn,lease)) return Stale();
        auto source=Assignment(conn,lease.revision);
        const bool media=source.at("pipeline_kind")=="media-parser-v1";
        if(!media && source.at("pipeline_kind")!="media-text-v1") return Stale();
        if(media) {
            auto storage=Query(conn,"SELECT state,charged_bytes FROM room_media_revision_storage WHERE revision_id=? FOR UPDATE",{lease.revision});
            if(storage.empty() || storage[0][0]!="reserved" || std::stoull(storage[0][1])!=media_storage::kRevisionReservation) return Stale();
            if(index>=kMediaParsedFragments || !ValidateMediaFragment(fragment)) return {400,"invalid media fragment"};
        }
        const auto encoded=fragment.dump(); const auto sha=Sha256Hex(encoded);
        const auto text=fragment.at("text").get<std::string>();
        auto prior=Query(conn,"SELECT sha256 FROM room_media_fragment WHERE revision_id=? AND fragment_index=?",
            {lease.revision,std::to_string(index)});
        if(!prior.empty()) {
            if(prior[0][0]!=sha) return {409,"fragment index already contains different content"};
        } else {
            auto budget=Query(conn,"SELECT COUNT(*),COALESCE(SUM(text_bytes),0) FROM room_media_fragment WHERE revision_id=?",{lease.revision});
            const auto cap=media?kMediaTextBytes:std::min(kMediaTextBytes,source.at("bytes").get<uint64_t>());
            const auto used=std::stoull(budget[0][1]);
            if(std::stoull(budget[0][0])>=(media?kMediaParsedFragments:kMediaFragmentCount) || used>cap || text.size()>cap-used)
                return {413,"derived text budget exceeded"};
            Query(conn,"INSERT INTO room_media_fragment(revision_id,fragment_index,sha256,payload,text,text_bytes) VALUES(?,?,?,?,?,?)",
                {lease.revision,std::to_string(index),sha,encoded,text,std::to_string(text.size())});
        }
        if(!Live(conn,lease)) return Stale();
        *out={{"index",index},{"sha256",sha}}; tx.Commit(); return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaProcessingRepository::Complete(const MediaLease& lease,uint64_t count,const std::string& manifest,Json* out) {
    try {
        auto guard=pool_->Acquire(); auto* conn=guard.get(); room_sql::Transaction tx(conn);
        if(!Lock(conn,lease)) return Stale();
        if(!Query(conn,"SELECT revision_id FROM room_media_revision_profile WHERE revision_id=?",{lease.revision}).empty())
            return {409,"media revision requires the media manifest protocol"};
        auto receipt=Receipt(conn,lease.revision);
        if(!receipt.is_null()) {
            if(receipt.at("fragment_count")!=count || receipt.at("manifest_sha256")!=manifest)
                return {409,"completion receipt conflicts with original manifest"};
            *out=receipt; tx.Commit(); return {};
        }
        if(!Live(conn,lease)) return Stale();
        auto fragments=Query(conn,"SELECT fragment_index,sha256,payload,text FROM room_media_fragment "
            "WHERE revision_id=? ORDER BY fragment_index",{lease.revision});
        if(fragments.size()!=count) return {409,"fragment manifest is incomplete"};
        Json hashes=Json::array(); std::string original; uint64_t next_line=1;
        for(size_t i=0;i<fragments.size();++i) {
            const auto& row=fragments[i]; auto part=Json::parse(row[2]);
            if(std::stoull(row[0])!=i || part.at("anchor").at("line_start")!=next_line)
                return {409,"fragment indexes or source lines are not contiguous"};
            const auto lines=std::count(row[3].begin(),row[3].end(),'\n')+(row[3].back()!='\n');
            if(part.at("anchor").at("line_end")!=next_line+lines-1 ||
               (i+1<count && row[3].back()!='\n')) return {422,"fragment line anchors do not match source text"};
            next_line+=lines; original+=row[3]; hashes.push_back(row[1]);
        }
        auto source=Assignment(conn,lease.revision);
        if(original.size()!=source.at("bytes").get<uint64_t>() || Sha256Hex(original)!=source.at("sha256"))
            return {422,"derived text does not reproduce the original UTF-8 bytes"};
        if(Sha256Hex(hashes.dump())!=manifest) return {409,"fragment manifest digest mismatch"};
        uint64_t changed=0;
        Query(conn,"UPDATE room_media_job j JOIN room_media_revision r ON r.asset_id=j.asset_id AND r.attempt=j.attempt "
            "SET j.state='completed',j.lease_until=NULL WHERE r.revision_id=? AND j.state='running' "
            "AND j.lease_until>NOW(3) AND TIMESTAMPADD(SECOND,3600,r.created_at)>NOW(3)",{lease.revision},&changed);
        if(!changed) return Stale();
        Query(conn,"UPDATE room_media_revision SET state='ready',manifest_sha=?,fragment_count=?,text_bytes=?,line_count=? WHERE revision_id=?",
            {manifest,std::to_string(count),std::to_string(original.size()),std::to_string(next_line-1),lease.revision});
        Query(conn,"INSERT INTO room_media_publication(asset_id,revision_id) VALUES(?,?)",{lease.asset,lease.revision});
        Query(conn,"UPDATE attachment SET status='ready' WHERE id=?",{lease.asset});
        *out=Receipt(conn,lease.revision); tx.Commit(); return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaProcessingRepository::Fail(const MediaLease& lease,const std::string& error) {
    try {
        auto guard=pool_->Acquire(); auto* conn=guard.get(); room_sql::Transaction tx(conn);
        if(!Lock(conn,lease) || !Live(conn,lease)) return Stale();
        uint64_t changed=0;
        Query(conn,"UPDATE room_media_job SET state='failed',lease_until=NULL,error_code=?,error_message='' "
            "WHERE asset_id=? AND lease_until>NOW(3)",{error,lease.asset},&changed);
        if(!changed) return Stale();
        Query(conn,"UPDATE room_media_revision SET state='failed' WHERE revision_id=?",{lease.revision});
        Query(conn,"DELETE FROM room_media_fragment WHERE revision_id=?",{lease.revision});
        Query(conn,"UPDATE attachment SET status='failed' WHERE id=?",{lease.asset});
        tx.Commit(); return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaProcessingRepository::Info(const std::string& asset,Json* out) {
    try {
        auto guard=pool_->Acquire();
        auto rows=Query(guard.get(),"SELECT a.status,j.state,j.attempt,j.error_code,COALESCE(p.revision_id,''),a.mime "
            "FROM attachment a JOIN room_media_job j ON j.asset_id=a.id LEFT JOIN room_media_publication p ON p.asset_id=a.id WHERE a.id=?",{asset});
        if(rows.empty()) return {409,"original has not been finalized"};
        const auto& r=rows[0];
        *out={{"id",asset},{"state",r[0]},{"job_state",r[1]},{"attempt",std::stoull(r[2])},
            {"error_code",r[3]},{"revision",r[4]},{"text_pipeline_supported",r[5]=="text/plain" || r[5]=="text/markdown"}};
        if(!r[4].empty()) {
            (*out)["result"]=Receipt(guard.get(),r[4]);
            auto profile=Query(guard.get(),"SELECT pipeline_kind,coverage,artifact_count,artifact_bytes FROM room_media_revision_profile WHERE revision_id=?",{r[4]});
            if(!profile.empty()) (*out)["result"].update({{"pipeline_kind",profile[0][0]},{"coverage",Json::parse(profile[0][1])},
                {"artifact_count",std::stoull(profile[0][2])},{"artifact_bytes",std::stoull(profile[0][3])}});
        }
        return {};
    } catch(const std::exception&) { return Unavailable(); }
}
ServiceStatus MySqlMediaProcessingRepository::Fragments(const std::string& asset,const std::string& revision,uint64_t offset,uint64_t limit,Json* out) {
    try {
        auto guard=pool_->Acquire();
        auto current=Query(guard.get(),"SELECT p.revision_id,r.fragment_count FROM room_media_publication p "
            "JOIN attachment a ON a.id=p.asset_id JOIN room_media_revision r ON r.revision_id=p.revision_id AND r.asset_id=a.id "
            "WHERE p.asset_id=? AND a.status='ready' AND r.state='ready'",{asset});
        if(current.empty()) return {409,"derived content is not ready"};
        if(!revision.empty() && revision!=current[0][0]) return {409,"requested revision is not currently published"};
        Json items=Json::array();
        for(const auto& row:Query(guard.get(),"SELECT fragment_index,sha256,payload FROM room_media_fragment "
            "WHERE revision_id=? AND fragment_index>=? ORDER BY fragment_index LIMIT ?",
            {current[0][0],std::to_string(offset),std::to_string(limit)}))
            items.push_back({{"index",std::stoull(row[0])},{"sha256",row[1]},{"content",Json::parse(row[2])}});
        const auto next=offset+items.size();
        *out={{"id",asset},{"revision",current[0][0]},{"items",items},{"next_offset",next},
            {"has_more",next<std::stoull(current[0][1])}}; return {};
    } catch(const std::exception&) { return Unavailable(); }
}
ServiceStatus MySqlMediaProcessingRepository::Search(const std::string& session,const std::string& query,uint64_t limit,Json* out) {
    try {
        auto guard=pool_->Acquire(); Json items=Json::array();
        // Literal substring matching also handles Chinese and SQL metacharacters.
        // Scope and publication visibility are applied before LIMIT, never after.
        auto rows=Query(guard.get(),"SELECT a.id,a.display_name,p.revision_id,f.fragment_index,f.payload,f.sha256 "
            "FROM attachment a JOIN room_media_asset m ON m.asset_id=a.id "
            "JOIN room_media_publication p ON p.asset_id=a.id JOIN room_media_revision r ON r.revision_id=p.revision_id AND r.asset_id=a.id "
            "JOIN room_media_fragment f ON f.revision_id=p.revision_id WHERE a.session_id=? "
            "AND a.status='ready' AND r.state='ready' AND LOCATE(?,f.text)>0 ORDER BY m.ordinal DESC,f.fragment_index LIMIT ?",
            {session,query,std::to_string(limit+1)});
        const bool more=rows.size()>limit; if(more) rows.resize(limit);
        for(const auto& r:rows) items.push_back({{"id",r[0]},{"name",r[1]},{"revision",r[2]},
            {"index",std::stoull(r[3])},{"content",Json::parse(r[4])},{"sha256",r[5]}});
        *out={{"items",items},{"has_more",more}}; return {};
    } catch(const std::exception&) { return Unavailable(); }
}
ServiceStatus MySqlMediaProcessingRepository::Current(const std::string& asset,const std::string& revision) {
    try { auto g=pool_->Acquire();
        if(Query(g.get(),"SELECT p.revision_id FROM room_media_publication p JOIN attachment a ON a.id=p.asset_id "
            "JOIN room_media_revision r ON r.revision_id=p.revision_id AND r.asset_id=a.id WHERE p.asset_id=? AND p.revision_id=? AND a.status='ready' AND r.state='ready'",
            {asset,revision}).empty()) return {409,"media publication changed or unavailable"};
        return {};
    } catch(const std::exception&) {return Unavailable();}
}
}  // namespace sparkpush
