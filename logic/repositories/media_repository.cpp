#include "repositories/media_repository.h"
#include "repositories/room_sql.h"
#include "repositories/media_storage_quota.h"
#include "logging.h"
#include <algorithm>

namespace sparkpush {
namespace {
using room_sql::Query;
const char* kFields = "SELECT a.id,a.owner_user_id,a.session_id,a.display_name,a.mime,a.bytes,a.sha256,a.status,m.ordinal,m.kind "
                      "FROM room_media_asset m JOIN attachment a ON a.id=m.asset_id ";
RoomMedia Decode(const std::vector<std::string>& r) {
    RoomMedia media;
    media.file.id=r[0]; media.file.owner_user_id=std::stoll(r[1]); media.file.session_id=r[2];
    media.file.display_name=r[3]; media.file.mime=r[4]; media.file.bytes=std::stoull(r[5]);
    media.file.sha256=r[6]; media.file.status=r[7]; media.ordinal=std::stoull(r[8]); media.kind=r[9];
    return media;
}
bool Load(MYSQL* conn, const std::string& id, RoomMedia* out, bool lock) {
    const auto sql=std::string(kFields)+"WHERE m.asset_id=?"+(lock ? " FOR UPDATE" : "");
    const auto rows=Query(conn,sql.c_str(),{id});
    if(rows.empty()) return false;
    *out=Decode(rows[0]);
    for(const auto& row:Query(conn,"SELECT chunk_index,bytes,sha256 FROM room_media_chunk WHERE asset_id=? ORDER BY chunk_index",{id}))
        out->chunks.push_back({std::stoull(row[0]),std::stoull(row[1]),row[2]});
    return true;
}
ServiceStatus Unavailable() { return {503,"media storage unavailable; retry with the same upload identity"}; }
}  // namespace

bool MySqlMediaRepository::CheckSchema() {
    if(!pool_) return false;
    try {
        auto guard=pool_->Acquire();
        Query(guard.get(),"SELECT asset_id,ordinal,upload_key,kind FROM room_media_asset LIMIT 0");
        Query(guard.get(),"SELECT asset_id,chunk_index,bytes,sha256 FROM room_media_chunk LIMIT 0");
        Query(guard.get(),"SELECT asset_id,pipeline,state,attempt,lease_token,lease_until FROM room_media_job LIMIT 0");
        Query(guard.get(),"SELECT room_id,charged_bytes,state FROM room_media_revision_storage LIMIT 0");
        return true;
    } catch(const std::exception&) { return false; }
}

ServiceStatus MySqlMediaRepository::Create(const MediaUpload& in, RoomMedia* out) {
    if(!pool_ || !out) return Unavailable();
    try {
        auto guard=pool_->Acquire(); auto* conn=guard.get();
        media_storage::NextTransactionCurrent(conn);
        room_sql::Transaction tx(conn);
        // Serialize room quota reservations across processes, not just threads.
        if(Query(conn,"SELECT id FROM im_group WHERE id=? FOR UPDATE",{std::to_string(in.room)}).empty())
            return {404,"room not found"};
        const auto upload_key=Sha256Hex(nlohmann::json::array({in.actor,in.session,in.client_id}).dump());
        const auto prior=Query(conn,"SELECT asset_id FROM room_media_asset WHERE upload_key=?",{upload_key});
        if(!prior.empty()) {
            if(!Load(conn,prior[0][0],out,true)) return Unavailable();
            const auto& file=out->file;
            if(file.owner_user_id!=in.actor || file.session_id!=in.session || file.bytes!=in.bytes ||
               file.sha256!=in.sha256 || file.mime!=in.mime || file.display_name!=in.name)
                return {409,"upload ID already belongs to different content"};
            tx.Commit(); return {};
        }
        const auto quota=Query(conn,"SELECT COALESCE(SUM(a.bytes),0),COALESCE(SUM(a.status IN ('uploading','cancelling') AND a.owner_user_id=?),0) "
            "FROM room_media_asset m JOIN attachment a ON a.id=m.asset_id WHERE a.session_id=? AND a.status NOT IN ('deleted','expired')",
            {std::to_string(in.actor),in.session});
        const auto used=std::stoull(quota[0][0]);
        const auto derived=media_storage::Charged(conn,in.room);
        if(used>kMediaRoomBytes || derived>kMediaRoomBytes-used || in.bytes>kMediaRoomBytes-used-derived)
            return {413,"room media quota reached"};
        if(std::stoull(quota[0][1])>=4) return {429,"finish or cancel an existing upload before starting another"};
        std::string id;
        if(!GenerateAttachmentId(&id)) return Unavailable();
        Query(conn,"INSERT INTO attachment(id,owner_user_id,session_id,display_name,mime,bytes,sha256,storage_key,status) VALUES(?,?,?,?,?,?,?,?, 'uploading')",
            {id,std::to_string(in.actor),in.session,in.name,in.mime,std::to_string(in.bytes),in.sha256,id});
        Query(conn,"INSERT INTO room_media_asset(asset_id,upload_key,kind) VALUES(?,?,?)",{id,upload_key,in.kind});
        if(!Load(conn,id,out,false)) return Unavailable();
        tx.Commit(); return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaRepository::Find(const std::string& id, RoomMedia* out) {
    if(!pool_ || !out || !IsValidAttachmentId(id)) return {404,"media not found"};
    try {
        auto guard=pool_->Acquire();
        if(!Load(guard.get(),id,out,false)) return {404,"media not found"};
        return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaRepository::PutChunk(const std::string& id, uint64_t index,
                                           const std::string& bytes, RoomMedia* out) {
    if(!pool_ || !out || !IsValidAttachmentId(id)) return {404,"media not found"};
    try {
        auto file_lock=blobs_.Lock(id);
        auto guard=pool_->Acquire(); auto* conn=guard.get(); room_sql::Transaction tx(conn);
        if(!Load(conn,id,out,true)) return {404,"media not found"};
        if(out->file.status=="expired" || out->file.status=="deleted" || out->file.status=="cancelling") return {409,"upload is no longer available"};
        const auto count=(out->file.bytes+kMediaChunkBytes-1)/kMediaChunkBytes;
        if(index>=count || bytes.size()!=std::min(kMediaChunkBytes,static_cast<uint64_t>(out->file.bytes)-index*kMediaChunkBytes))
            return {400,"chunk index or length does not match upload manifest"};
        const auto sha=Sha256Hex(bytes);
        for(const auto& chunk:out->chunks) if(chunk.index==index) {
            if(chunk.sha256!=sha || chunk.bytes!=bytes.size()) return {409,"chunk already contains different bytes"};
            if(out->file.status=="uploading") blobs_.WriteChunk(id,index,bytes);
            tx.Commit(); return {};
        }
        if(out->file.status!="uploading") return {409,"upload no longer accepts new chunks"};
        blobs_.WriteChunk(id,index,bytes);
        Query(conn,"INSERT INTO room_media_chunk(asset_id,chunk_index,bytes,sha256) VALUES(?,?,?,?)",
            {id,std::to_string(index),std::to_string(bytes.size()),sha});
        if(!Load(conn,id,out,false)) return Unavailable();
        tx.Commit(); return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaRepository::Finalize(const std::string& id, RoomMedia* out) {
    if(!pool_ || !out || !IsValidAttachmentId(id)) return {404,"media not found"};
    try {
        auto file_lock=blobs_.Lock(id);
        {
            auto guard=pool_->Acquire(); auto* conn=guard.get(); room_sql::Transaction tx(conn);
            if(!Load(conn,id,out,true)) return {404,"media not found"};
            if(out->file.status=="deleted" || out->file.status=="expired" || out->file.status=="cancelling") return {409,"upload is no longer available"};
            if(out->file.status=="uploading") {
                if(out->chunks.size()!=(out->file.bytes+kMediaChunkBytes-1)/kMediaChunkBytes)
                    return {409,"upload is incomplete; resume missing chunks"};
                try { blobs_.Assemble(*out); }
                catch(const std::exception&) { return {422,"file size, digest, format or storage validation failed"}; }
                Query(conn,"UPDATE attachment SET status='queued' WHERE id=?",{id});
                Query(conn,"INSERT INTO room_media_job(asset_id,pipeline,state) VALUES(?,'media-v1','queued')",{id});
                out->file.status="queued";
            }
            tx.Commit();
        }
        // The committed original survives chunk cleanup. A failed cleanup must
        // not convert an accepted upload into a false processing failure.
        try { blobs_.RemoveChunks(*out); }
        catch(const std::exception&) { LOG_WARN << "media upload chunk cleanup deferred"; }
        return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaRepository::List(const std::string& session, uint64_t before, int limit,
                                       std::vector<RoomMedia>* out) {
    if(!pool_ || !out) return Unavailable();
    try {
        auto guard=pool_->Acquire(); out->clear();
        const auto sql=std::string(kFields)+"WHERE a.session_id=? AND a.status IN ('queued','running','ready','failed','unknown') "
            "AND (?=0 OR m.ordinal<?) ORDER BY m.ordinal DESC LIMIT ?";
        for(const auto& row:Query(guard.get(),sql.c_str(),{session,std::to_string(before),std::to_string(before),std::to_string(limit)}))
            out->push_back(Decode(row));
        return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaRepository::Abort(const std::string& id, RoomMedia* out) {
    if(!pool_ || !out || !IsValidAttachmentId(id)) return {404,"media not found"};
    try {
        // File ownership remains serialized even if MySQL disconnects and drops
        // its row lock during I/O. Every writer acquires this same lock first.
        auto file_lock=blobs_.Lock(id);
        {
            auto guard=pool_->Acquire(); auto* conn=guard.get(); room_sql::Transaction tx(conn);
            if(!Load(conn,id,out,true)) return {404,"media not found"};
            if(out->file.status!="uploading" && out->file.status!="cancelling" && out->file.status!="expired")
                return {409,"shared media requires a separate removal operation"};
            if(out->file.status!="expired")
                Query(conn,"UPDATE attachment SET status='cancelling' WHERE id=?",{id});
            tx.Commit();
        }
        blobs_.DiscardUpload(*out);
        // A failed cleanup/commit leaves cancelling and its reserved quota.
        // The uploader can rediscover this record and retry abort after restart.
        auto guard=pool_->Acquire(); auto* conn=guard.get(); room_sql::Transaction tx(conn);
        Query(conn,"UPDATE attachment SET status='expired' WHERE id=? AND status IN ('cancelling','expired')",{id});
        out->file.status="expired"; tx.Commit();
        return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaRepository::Uploads(const std::string& session, int64_t actor,
                                          std::vector<RoomMedia>* out) {
    if(!pool_ || !out) return Unavailable();
    try {
        auto guard=pool_->Acquire(); out->clear();
        const auto sql=std::string(kFields)+"WHERE a.session_id=? AND a.owner_user_id=? AND a.status IN ('uploading','cancelling') ORDER BY m.ordinal DESC LIMIT 4";
        for(const auto& row:Query(guard.get(),sql.c_str(),{session,std::to_string(actor)}))
            out->push_back(Decode(row));
        return {};
    } catch(const std::exception&) { return Unavailable(); }
}

ServiceStatus MySqlMediaRepository::Read(const RoomMedia& media, uint64_t offset, uint64_t length, std::string* out) {
    if(!out) return Unavailable();
    try { *out=blobs_.Read(media,offset,length); return {}; }
    catch(const std::exception&) { return Unavailable(); }
}
}  // namespace sparkpush
