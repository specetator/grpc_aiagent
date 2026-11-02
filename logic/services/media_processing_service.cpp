#include "services/media_processing_service.h"
#include "services/media_json.h"
#include "security.h"
#include "services/media_manifest.h"
#include <chrono>
#include <limits>
#include <map>
#include <regex>
#include <set>

namespace sparkpush {
namespace {
using media_json::Number;
using media_json::Fields;
using media_json::Hex;
using Json=nlohmann::json;
}  // namespace
ServiceStatus MediaProcessingService::Worker(const std::string& key,const std::string& op,const Json& in,Json* out) {
    if(out) *out=Json::object();
    const bool media_enabled=artifacts_ && Hex(media_pipeline_) && Hex(parser_) && media_pipeline_!=pipeline_;
    if(!out || !jobs_ || !files_ || key_.size()<32 || key_.size()>256 || (!Hex(pipeline_) && !media_enabled))
        return {503,"media worker is not configured"};
    if(!ConstantTimeEquals(key,key_)) return {401,"media worker authentication required"};
    std::set<std::string> fields={"id","attempt","revision","lease_token"};
    if(op=="claim" || op=="claim_media") fields={"claim_id","pipeline_sha"};
    else if(op=="reclaim") fields={"revision"};
    else if(op=="reclaim_candidates") fields={"after","limit"};
    else if(op=="source") {fields.insert("offset");fields.insert("length");}
    else if(op=="stage") {fields.insert("index");fields.insert("fragment");}
    else if(op=="stage_artifact") {fields.insert("index");fields.insert("metadata");fields.insert("data");}
    else if(op=="complete") {fields.insert("fragment_count");fields.insert("manifest_sha256");fields.insert("artifact_count");fields.insert("coverage");}
    else if(op=="fail") fields.insert("error_code");
    else if(op!="heartbeat" && op!="check" && op!="reserve") return {400,"invalid processing operation"};
    if(!Fields(in,fields)) return {400,"unsupported processing field"};
    try {
        if(op=="reclaim_candidates") {
            const auto after=in.value("after",std::string());const auto limit=Number(in,"limit",20,5);
            if(!artifacts_ || (!after.empty() && !Hex(after)) || !limit) return {400,"valid reclamation page required"};
            return artifacts_->ReclaimCandidates(after,limit,out);
        }
        if(op=="reclaim") {
            const auto revision=in.at("revision").get<std::string>();
            if(!artifacts_ || !Hex(revision)) return {400,"valid media revision required"};
            return artifacts_->Reclaim(revision);
        }
        if(op=="claim" || op=="claim_media") {
            const bool media=op=="claim_media";
            if((media && !media_enabled) || (!media && !Hex(pipeline_))) return {503,"requested worker pipeline is not configured"};
            const auto id=in.at("claim_id").get<std::string>();
            static const std::regex shape("[0-9]{10}-[a-f0-9]{32}");
            if(!std::regex_match(id,shape)) return {400,"timestamped claim identity required"};
            const auto issued=std::stoll(id.substr(0,10));
            const auto now=std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            if(issued>now+60) return {409,"claim identity is in the future"};
            const auto& approved=media?media_pipeline_:pipeline_;
            if(in.at("pipeline_sha")!=approved) return {409,"pipeline fingerprint is not approved by the operator"};
            return jobs_->Claim(Sha256Hex(id),approved,issued>=now-86400,out,media?parser_:"");
        }
        MediaLease lease{in.at("id"),in.at("revision"),in.at("lease_token"),Number(in,"attempt",std::numeric_limits<uint32_t>::max())};
        if(!IsValidAttachmentId(lease.asset) || !Hex(lease.revision) || !Hex(lease.token) || !lease.attempt)
            return {400,"valid assignment identity required"};
        Json check;auto status=jobs_->Check(lease,&check,false);if(!status.ok()) return status;
        const bool media=check.at("pipeline_kind")=="media-parser-v1";
        if(media) {
            if(!media_enabled || check.at("pipeline_sha")!=media_pipeline_ || check.at("parser_sha256")!=parser_)
                return {409,"media assignment pipeline is no longer approved"};
        } else if(check.at("pipeline_kind")!="media-text-v1" || check.at("pipeline_sha")!=pipeline_)
            return {409,"text assignment pipeline is no longer approved"};
        if(op=="check") return jobs_->Check(lease,out);
        if(op=="heartbeat") return jobs_->Heartbeat(lease);
        if(op=="reserve") return media?artifacts_->Reserve(lease):ServiceStatus{409,"media profile required"};
        if(op=="stage_artifact") {
            if(!media || !in.contains("index")) return {400,"media image index required"};
            const auto index=Number(in,"index",1023);const auto encoded=in.at("data").get<std::string>();std::string bytes;
            if(encoded.size()>4*((kMediaChunkBytes+2)/3) || !DecodeBase64(encoded,&bytes) || bytes.empty() || bytes.size()>kMediaChunkBytes)
                return {400,"bounded base64 image required"};
            return artifacts_->Stage(lease,index,in.at("metadata"),bytes,out);
        }
        if(op=="stage") {
            if(!in.contains("index")) return {400,"fragment index required"};
            const auto index=Number(in,"index",(media?kMediaParsedFragments:kMediaFragmentCount)-1);
            const auto& f=in.at("fragment");
            if(media) {
                if(!ValidateMediaFragment(f)) return {400,"invalid media fragment or anchor"};
            } else {
                if(!Fields(f,{"kind","text","anchor"}) || f.size()!=3 || f.at("kind")!="text" ||
                   !Fields(f.at("anchor"),{"line_start","line_end"}) || f.at("anchor").size()!=2)
                    return {400,"text fragment and exact line anchors required"};
                const auto text=f.at("text").get<std::string>();
                const auto begin=Number(f.at("anchor"),"line_start",kMediaTextBytes),end=Number(f.at("anchor"),"line_end",kMediaTextBytes);
                if(text.empty() || text.size()>kMediaFragmentBytes || text.find('\0')!=std::string::npos || !begin || end<begin)
                    return {400,"invalid fragment size, text or source lines"};
            }
            return jobs_->Stage(lease,index,f,out);
        }
        if(op=="complete") {
            const auto digest=in.at("manifest_sha256").get<std::string>();if(!Hex(digest)) return {400,"manifest digest required"};
            if(media) return artifacts_->Complete(lease,{{"fragment_count",Number(in,"fragment_count",kMediaParsedFragments)},
                {"artifact_count",Number(in,"artifact_count",1024)},{"coverage",in.at("coverage")},{"manifest_sha256",digest}},out);
            const auto count=Number(in,"fragment_count",kMediaFragmentCount);
            if(!count || in.contains("coverage") || in.contains("artifact_count")) return {400,"nonempty text manifest required"};
            return jobs_->Complete(lease,count,digest,out);
        }
        if(op=="fail") {
            const auto error=in.at("error_code").get<std::string>();
            static const std::set<std::string> errors={"invalid_utf8","text_too_large","line_too_long","too_many_fragments", "source_integrity","processing_error",
                "parser_error","unsupported_format","encrypted_document","invalid_media_timing","parser_cancelled","parser_timeout","asr_model_unavailable","output_limit"};
            if(!errors.count(error)) return {400,"unsupported processing error code"};
            return jobs_->Fail(lease,error);
        }
        status=jobs_->Check(lease,&check);if(!status.ok()) return status;
        RoomMedia file;status=files_->Find(lease.asset,&file);if(!status.ok()) return status;
        const auto offset=Number(in,"offset",kMediaMaxBytes),length=Number(in,"length",kMediaChunkBytes,kMediaChunkBytes);
        if(!length || offset>=file.file.bytes) return {416,"source range outside original"};
        std::string bytes,encoded;status=files_->Read(file,offset,length,&bytes);if(!status.ok()) return status;
        status=jobs_->Check(lease,&check);if(!status.ok()) return status;EncodeBase64(bytes,&encoded);
        *out={{"id",lease.asset},{"revision",lease.revision},{"offset",offset},{"data",encoded},
            {"bytes",bytes.size()},{"sha256",file.file.sha256},{"total_bytes",file.file.bytes}};return {};
    } catch(const Json::exception&) {return {400,"invalid processing field type"};}
      catch(const std::invalid_argument&) {return {400,"invalid processing integer"};}
}
ServiceStatus MediaProcessingService::Access(int64_t actor,const std::string& session) {
    if(actor<=0 || !conversations_ || session.rfind("r_",0)!=0) return {403,"current room membership required"};
    Session authorized; return conversations_->CheckAccess(actor,session,&authorized);
}
ServiceStatus MediaProcessingService::Member(int64_t actor,const std::string& op,const Json& in,Json* out) {
    if(out) *out=Json::object();
    if(!out || !jobs_ || !files_) return {503,"media processing migration required"};
    if(actor<=0) return {403,"authenticated room member required"};
    static const std::map<std::string,std::set<std::string>> fields={
        {"processing",{"id"}},{"fragments",{"id","revision","offset","limit"}},
        {"search",{"room_id","query","limit"}}, {"artifacts",{"id","revision","offset","limit"}},
        {"artifact",{"id","revision","index"}}};
    auto allowed=fields.find(op);
    if(allowed==fields.end() || !Fields(in,allowed->second)) return {400,"invalid media content operation"};
    try {
        std::string session; RoomMedia media;
        if(op=="search") {
            auto room=Number(in,"room_id",std::numeric_limits<int64_t>::max());
            if(!room) return {400,"room_id required"};
            session="r_"+std::to_string(room);
        } else {
            const auto id=in.at("id").get<std::string>();
            if(!IsValidAttachmentId(id)) return {404,"media not found"};
            auto status=files_->Find(id,&media); if(!status.ok()) return status;
            session=media.file.session_id;
        }
        auto status=Access(actor,session); if(!status.ok()) return status;
        if(op!="search" && (media.file.status=="uploading" || media.file.status=="cancelling" || media.file.status=="expired") && media.file.owner_user_id!=actor)
            return {403,"unpublished upload belongs to another member"};
        Json result;
        if(op=="processing") status=jobs_->Info(media.file.id,&result);
        else if(op=="fragments") {
            const auto limit=Number(in,"limit",20,10), offset=Number(in,"offset",kMediaParsedFragments);
            const auto revision=in.value("revision",std::string());
            if(!limit || (!revision.empty() && !Hex(revision))) return {400,"invalid revision or page limit"};
            status=jobs_->Fragments(media.file.id,revision,offset,limit,&result);
        } else if(op=="artifacts" || op=="artifact") {
            if(!artifacts_) return {503,"media artifact storage unavailable"};
            const auto revision=in.value("revision",std::string());
            if((!revision.empty() && !Hex(revision)) || (op=="artifact" && (revision.empty() || !in.contains("index"))))
                return {400,"fixed media image revision and index required"};
            if(op=="artifact") status=artifacts_->Read(media.file.id,revision,Number(in,"index",1023),&result);
            else { const auto limit=Number(in,"limit",20,10);if(!limit)return {400,"positive page limit required"};
                status=artifacts_->Artifacts(media.file.id,revision,Number(in,"offset",1024),limit,&result); }
        } else {
            const auto query=in.at("query").get<std::string>();
            const auto limit=Number(in,"limit",20,10);
            if(query.empty() || query.size()>240 || query.find('\0')!=std::string::npos || !limit)
                return {400,"bounded nonempty query and limit required"};
            status=jobs_->Search(session,query,limit,&result);
        }
        if(!status.ok()) return status;
        status=Access(actor,session); if(!status.ok()) return status;
        if(op=="search") {
            for(const auto& item:result.at("items")) {status=jobs_->Current(item.at("id"),item.at("revision"));if(!status.ok())return status;}
        } else if(result.contains("revision") && !result.at("revision").get<std::string>().empty()) {
            status=jobs_->Current(media.file.id,result.at("revision"));if(!status.ok())return status;
        }
        *out=std::move(result); return {};
    } catch(const Json::exception&) { return {400,"invalid media content field type"}; }
      catch(const std::invalid_argument&) { return {400,"invalid media content integer"}; }
}
}  // namespace sparkpush
