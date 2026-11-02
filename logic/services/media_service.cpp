#include "services/media_service.h"
#include "services/media_json.h"
#include <openssl/evp.h>
#include <limits>
#include <map>
#include <regex>
#include <set>

namespace sparkpush {
namespace {
using media_json::Number;
using media_json::Fields;
using Json = nlohmann::json;
const std::map<std::string,std::string> kKinds = {
    {"application/pdf","document"},
    {"application/vnd.openxmlformats-officedocument.wordprocessingml.document","document"},
    {"text/plain","document"},{"text/markdown","document"},
    {"image/png","image"},{"image/jpeg","image"},{"image/gif","image"},{"image/webp","image"},
    {"audio/wav","audio"},{"audio/mpeg","audio"},{"audio/flac","audio"},
    {"audio/ogg","audio"},{"audio/mp4","audio"},{"audio/webm","audio"},
    {"video/mp4","video"},{"video/webm","video"},{"video/ogg","video"}
};
bool DecodeChunk(const std::string& encoded, std::string* bytes) {
    if(encoded.empty() || encoded.size()%4 || encoded.size()>((kMediaChunkBytes+2)/3)*4) return false;
    bytes->resize(encoded.size()/4*3);
    const int n=EVP_DecodeBlock(reinterpret_cast<unsigned char*>(bytes->data()),
        reinterpret_cast<const unsigned char*>(encoded.data()),static_cast<int>(encoded.size()));
    if(n<0) return false;
    size_t padding=0;
    if(encoded.back()=='=') ++padding;
    if(encoded[encoded.size()-2]=='=') ++padding;
    if(static_cast<size_t>(n)<padding) return false;
    bytes->resize(n-padding);
    std::string canonical;
    EncodeBase64(*bytes,&canonical);
    return !bytes->empty() && bytes->size()<=kMediaChunkBytes && canonical==encoded;
}
Json View(const RoomMedia& media, bool manifest=true) {
    const auto& f=media.file;
    Json value={{"id",f.id},{"name",f.display_name},{"mime",f.mime},{"bytes",f.bytes},
        {"sha256",f.sha256},{"state",f.status},{"kind",media.kind},{"ordinal",media.ordinal},
        {"owner_user_id",f.owner_user_id},{"chunk_bytes",kMediaChunkBytes}};
    if(manifest) {
        value["chunks"]=Json::array();
        for(const auto& chunk:media.chunks) value["chunks"].push_back({{"index",chunk.index},{"bytes",chunk.bytes},{"sha256",chunk.sha256}});
    }
    return value;
}
}  // namespace

ServiceStatus RoomMediaService::Access(int64_t actor, const std::string& session) {
    if(actor<=0) return {403,"authenticated room member required"};
    if(!conversations_ || session.rfind("r_",0)!=0) return {403,"room media requires current room membership"};
    Session authorized;
    return conversations_->CheckAccess(actor,session,&authorized);
}

ServiceStatus RoomMediaService::Execute(int64_t actor, const std::string& operation,
                                      const Json& input, Json* output) {
    if(output) *output=Json::object();
    if(!repository_ || !output) return {503,"room media migration is required"};
    if(actor<=0) return {403,"authenticated room member required"};
    static const std::map<std::string,std::set<std::string>> fields={
        {"init",{"room_id","client_upload_id","name","mime","bytes","sha256"}},
        {"chunk",{"id","index","data"}}, {"finish",{"id"}}, {"abort",{"id"}},
        {"status",{"id"}}, {"list",{"room_id","before","limit"}},
        {"read",{"id","offset","length"}}, {"uploads",{"room_id"}}
    };
    const auto allowed=fields.find(operation);
    if(allowed==fields.end() || !input.is_object()) return {400,"invalid media operation"};
    if(!Fields(input,allowed->second)) return {400,"unsupported media field"};
    try {
        if(operation=="init" || operation=="list" || operation=="uploads") {
            const auto room=Number(input,"room_id",std::numeric_limits<int64_t>::max());
            if(!room) return {400,"room_id is required"};
            const auto session="r_"+std::to_string(room);
            auto status=Access(actor,session);
            if(!status.ok()) return status;
            if(operation=="uploads") {
                std::vector<RoomMedia> drafts;
                status=repository_->Uploads(session,actor,&drafts); if(!status.ok()) return status;
                status=Access(actor,session); if(!status.ok()) return status;
                Json items=Json::array();
                for(const auto& draft:drafts) items.push_back(View(draft,false));
                *output={{"items",items}}; return {};
            }
            if(operation=="list") {
                const auto limit=Number(input,"limit",100,50);
                if(!limit) return {400,"limit must be positive"};
                std::vector<RoomMedia> rows;
                status=repository_->List(session,Number(input,"before",std::numeric_limits<int64_t>::max()),limit+1,&rows);
                if(!status.ok()) return status;
                status=Access(actor,session); if(!status.ok()) return status;
                const bool more=rows.size()>limit;
                if(more) rows.resize(limit);
                Json items=Json::array();
                for(const auto& row:rows) items.push_back(View(row,false));
                *output={{"items",items},{"has_more",more},{"next_before",rows.empty()?0:rows.back().ordinal}};
                return {};
            }
            MediaUpload upload;
            upload.actor=actor; upload.room=room; upload.session=session;
            upload.client_id=input.value("client_upload_id",""); upload.name=input.value("name","");
            upload.mime=input.value("mime",""); upload.sha256=input.value("sha256","");
            upload.bytes=Number(input,"bytes",kMediaMaxBytes);
            static const std::regex token("[A-Za-z0-9_-]{8,128}"), digest("[a-f0-9]{64}");
            if(!std::regex_match(upload.client_id,token) || !std::regex_match(upload.sha256,digest) ||
               !upload.bytes || upload.name.empty() || upload.name.size()>240 || !kKinds.count(upload.mime))
                return {400,"valid upload ID, filename, supported MIME, SHA-256 and size (1 byte to 1 GiB) required"};
            for(unsigned char c:upload.name) if(c<32 || c==127) return {400,"filename contains control characters"};
            upload.kind=kKinds.at(upload.mime);
            RoomMedia media;
            status=repository_->Create(upload,&media);
            if(!status.ok()) return status;
            status=Access(actor,session); if(!status.ok()) return status;
            *output=View(media); return {};
        }
        const auto id=input.value("id","");
        if(!IsValidAttachmentId(id)) return {404,"media not found"};
        RoomMedia media;
        auto status=repository_->Find(id,&media);
        if(!status.ok()) return status;
        status=Access(actor,media.file.session_id); if(!status.ok()) return status;
        const bool mutation=operation=="chunk" || operation=="finish" || operation=="abort";
        if((mutation || media.file.status=="uploading" || media.file.status=="cancelling" || media.file.status=="expired") && media.file.owner_user_id!=actor)
            return {403,"only the uploading member can manage this upload"};
        if(operation=="chunk") {
            if(!input.contains("index")) return {400,"chunk index required"};
            const auto index=Number(input,"index",kMediaMaxBytes/kMediaChunkBytes-1);
            std::string bytes;
            if(!DecodeChunk(input.value("data",""),&bytes)) return {400,"canonical base64 chunk of at most 1 MiB required"};
            status=repository_->PutChunk(id,index,bytes,&media);
        } else if(operation=="finish") status=repository_->Finalize(id,&media);
        else if(operation=="abort") status=repository_->Abort(id,&media);
        else if(operation=="read") {
            if(media.file.status=="uploading" || media.file.status=="cancelling" || media.file.status=="deleted" || media.file.status=="expired")
                return {409,"original file is not available"};
            const auto offset=Number(input,"offset",kMediaMaxBytes);
            const auto length=Number(input,"length",kMediaChunkBytes,kMediaChunkBytes);
            if(offset>=media.file.bytes || !length) return {416,"media range outside original file"};
            std::string bytes,encoded;
            status=repository_->Read(media,offset,length,&bytes); if(!status.ok()) return status;
            status=Access(actor,media.file.session_id); if(!status.ok()) return status;
            EncodeBase64(bytes,&encoded);
            *output={{"id",id},{"offset",offset},{"bytes",bytes.size()},{"total_bytes",media.file.bytes},
                {"mime",media.file.mime},{"sha256",media.file.sha256},{"data",encoded}};
            return {};
        }
        if(!status.ok()) return status;
        status=Access(actor,media.file.session_id); if(!status.ok()) return status;
        *output=View(media); return {};
    } catch(const Json::exception&) { return {400,"invalid media field type"}; }
      catch(const std::invalid_argument&) { return {400,"invalid media integer"}; }
}
}  // namespace sparkpush
