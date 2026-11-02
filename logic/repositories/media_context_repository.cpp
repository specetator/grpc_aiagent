#include "repositories/media_context_repository.h"
#include "repositories/room_sql.h"
#include "repositories/media_storage_quota.h"
#include "repositories/room_context_source_sql.h"
#include <algorithm>

namespace sparkpush {
namespace {
using Json=nlohmann::json;
using room_sql::Query;
// Malformed historical JSON is excluded without letting JSON_EXTRACT abort the
// whole query. SQL receives parameters only, including operator-owned bot IDs.
const std::string body="IF(JSON_VALID(m.content_json),m.content_json,'{}')";
const std::string text="CASE WHEN JSON_TYPE(JSON_EXTRACT("+body+",'$.content'))='STRING' "
    "THEN JSON_UNQUOTE(JSON_EXTRACT("+body+",'$.content')) "
    "WHEN JSON_TYPE(JSON_EXTRACT("+body+",'$.content.text'))='STRING' "
    "THEN JSON_UNQUOTE(JSON_EXTRACT("+body+",'$.content.text')) ELSE '' END";
std::string Id(int64_t id){return std::to_string(id);}
ServiceStatus Unavailable(){return {503,"room context storage unavailable"};}
ServiceStatus Changed(){return {409,"room context or source changed during read"};}

bool Current(MYSQL* conn,const MediaContextScope& scope,const Json& ref) {
    const auto room=scope.collaboration.at("room_id").get<int64_t>();
    if(ref.at("room_id")!=room)return false;
    const auto kind=ref.at("kind").get<std::string>();
    if(kind=="message") {
        const auto seq=ref.at("msg_seq").get<int64_t>();
        const auto offset=ref.at("offset").get<uint64_t>(),length=ref.at("length").get<uint64_t>();
        if(seq<=0 || seq>scope.message_ceiling || length>4096 || offset>16*1024*1024 ||
            ref.at("msg_id")!="r_"+Id(room)+"-"+Id(seq))return false;
        const auto rows=Query(conn,("SELECT SHA2("+text+",256),SHA2(SUBSTRING("+text+",?,?),256),CHAR_LENGTH("+text+") "
            "FROM message m WHERE m.session_id=BINARY ? AND m.msg_seq=?").c_str(),
            {std::to_string(offset+1),std::to_string(length),"r_"+Id(room),Id(seq)});
        return rows.size()==1 && rows[0][0]==ref.at("source_sha256") && rows[0][1]==ref.at("sha256") &&
            offset+length<=std::stoull(rows[0][2]);
    }
    if(kind=="media") {
        const auto rows=Query(conn,"SELECT r.manifest_sha FROM attachment a JOIN room_media_publication p ON p.asset_id=a.id "
            "JOIN room_media_revision r ON r.revision_id=p.revision_id AND r.asset_id=a.id "
            "WHERE a.session_id=BINARY ? AND a.id=? AND p.revision_id=? AND a.status='ready' AND r.state='ready'",
            {"r_"+Id(room),ref.at("asset_id"),ref.at("revision")});
        return rows.size()==1 && rows[0][0]==ref.at("sha256");
    }
    const bool image=kind=="image" || kind=="image_metadata";
    if(!image && kind!="fragment")return false;
    const auto rows=Query(conn,("SELECT f.sha256,f."+std::string(image?"metadata":"payload")+" FROM attachment a JOIN room_media_publication p ON p.asset_id=a.id "
        "JOIN room_media_revision r ON r.revision_id=p.revision_id AND r.asset_id=a.id JOIN "+
        std::string(image?"room_media_artifact":"room_media_fragment")+" f ON f.revision_id=p.revision_id "
        "WHERE a.session_id=BINARY ? AND a.id=? AND p.revision_id=? AND a.status='ready' AND r.state='ready' AND f."+
        (image?"artifact_index":"fragment_index")+"=?"+(image?" AND f.installed=TRUE":"")).c_str(),
        {"r_"+Id(room),ref.at("asset_id"),ref.at("revision"),Id(ref.at("index"))});
    if(rows.size()!=1 || rows[0][0]!=ref.at("sha256"))return false;
    const auto payload=Json::parse(rows[0][1]);
    if(ref.value("anchor",Json())!=payload.value("anchor",Json()))return false;
    return image?payload.at("sha256")==ref.at("sha256"):Sha256Hex(payload.dump())==rows[0][0];
}
}

ServiceStatus room_context_sql::Resolve(MYSQL* conn,int64_t room,const std::string& id,
        const Json& ids,const std::string& after,uint64_t limit,Json* out) {
    const auto request=Query(conn,"SELECT payload FROM room_agent_request WHERE request_id=? AND room_id=?",{id,Id(room)});
    if(request.empty())return {404,"source request not found in room"};
    const auto canonical=Json::parse(request[0][0]);
    if(canonical.at("request_id")!=id || canonical.at("session_id")!="r_"+Id(room) ||
       canonical.at("collaboration").at("room_id")!=room)return {409,"inconsistent source request"};
    MediaContextScope scope;scope.request_id=id;scope.collaboration=canonical.at("collaboration");
    scope.message_ceiling=canonical.at("message_seq");
    const auto generations=Query(conn,"SELECT generation FROM room_context WHERE room_id=?",{Id(room)});
    const int64_t generation=generations.empty()?0:std::stoll(generations[0][0]);
    const Json captured=scope.collaboration.value("context_generation",Json(0));
    const bool current=captured.is_number_integer() && captured>=0 && captured<=9007199254740991LL && captured==generation;
    std::string sql="SELECT source_id,room_id,context_generation,reference FROM room_context_source WHERE request_id=?";
    std::vector<std::string> values{id};
    if(ids.empty()){sql+=" AND source_id>? ORDER BY source_id LIMIT ?";values.insert(values.end(),{after,std::to_string(limit+1)});}
    else {
        sql+=" AND source_id IN (";
        for(const auto& source:ids){if(sql.back()!='(')sql+=",";sql+="?";values.push_back(source);}
        sql+=") ORDER BY source_id";
    }
    auto rows=Query(conn,sql.c_str(),values);
    if(!ids.empty() && rows.size()!=ids.size())return {404,"source not found in original request"};
    const bool more=ids.empty() && rows.size()>limit;if(more)rows.resize(limit);
    Json items=Json::array();
    for(const auto& row:rows) {
        Json item={{"source_id",row[0]},{"available",false},{"reason",current?"source_changed":"context_changed"}};
        Json reference;
        bool valid=false;
        try {
            reference=Json::parse(row[3]);
            valid=current && row[1]==Id(room) && row[2]==Id(generation) &&
                row[0]=="src_"+Sha256Hex(Json::array({"room-context-source-v1",id,reference}).dump()) && Current(conn,scope,reference);
        }catch(const Json::exception&){valid=false;}
        if(valid) {
            item.erase("reason");item["available"]=true;item["reference"]=reference;
            if(reference.at("kind")=="message") {
                const auto excerpt=Query(conn,("SELECT SUBSTRING("+text+",?,?) FROM message m WHERE m.session_id=BINARY ? AND m.msg_seq=?").c_str(),
                    {std::to_string(reference.at("offset").get<uint64_t>()+1),std::to_string(reference.at("length").get<uint64_t>()),
                     "r_"+Id(room),Id(reference.at("msg_seq"))});
                item["name"]="房间讨论";item["text"]=excerpt.at(0).at(0);
            }else {
                const auto file=Query(conn,"SELECT display_name FROM attachment WHERE id=? AND session_id=BINARY ?",{reference.at("asset_id"),"r_"+Id(room)});
                item["name"]=file.at(0).at(0);
            }
        }
        items.push_back(std::move(item));
    }
    *out={{"request_id",id},{"context_generation",generation},{"items",items},{"has_more",more},
        {"next_after",rows.empty()?after:rows.back()[0]}};return {};
}

ServiceStatus MySqlMediaContextRepository::Messages(const MediaContextScope& scope,const std::string& query,
        int64_t before,int64_t sequence,uint64_t offset,uint64_t length,uint64_t limit,Json* out) {
    try {
        const auto room=scope.collaboration.at("room_id").get<int64_t>();
        std::vector<std::string> values={std::to_string(offset+1),std::to_string(length),
            "r_"+Id(room),Id(scope.message_ceiling),Id(before),Id(sequence),Id(sequence),query};
        std::string bot="m.sender_id IN (";
        for(auto id:scope.bot_users){if(bot.back()!='(')bot+=",";bot+="?";values.push_back(Id(id));}
        // Canonical requests can verify Agent messages; prompt/body fields alone
        // cannot. Eligibility is evaluated before LIMIT so stale replies cannot
        // hide earlier eligible human discussion.
        if(scope.bot_users.empty())bot="FALSE";else bot+=")";
        const std::string origin="CASE WHEN JSON_TYPE(JSON_EXTRACT("+body+",'$.request_id'))='STRING' "
            "THEN JSON_UNQUOTE(JSON_EXTRACT("+body+",'$.request_id')) "
            "WHEN LEFT(m.client_msg_id,7)='hermes:' THEN SUBSTRING(m.client_msg_id,8) ELSE '' END";
        const std::string epoch="JSON_EXTRACT(r.payload,'$.collaboration.context_generation')";
        const std::string sql="SELECT m.msg_seq,m.sender_id,m.timestamp_ms,SUBSTRING("+text+",?,?),CHAR_LENGTH("+text+"),SHA2("+text+",256) "
            "FROM message m LEFT JOIN room_agent_request r ON r.request_id=BINARY ("+origin+") "
            "WHERE m.session_id=BINARY ? AND m.msg_seq<=? AND m.msg_seq<? AND (?=0 OR m.msg_seq=?) "
            "AND JSON_TYPE("+body+")='OBJECT' AND LOCATE(?,"+text+")>0 AND "
            "(NOT ("+bot+" OR LEFT(m.client_msg_id,7)='hermes:' OR JSON_CONTAINS_PATH("+body+",'one','$.agent_id')) "
            "OR (r.room_id=? AND JSON_UNQUOTE(JSON_EXTRACT(r.payload,'$.session_id'))=BINARY m.session_id "
            "AND JSON_EXTRACT(r.payload,'$.bot_user_id')=CAST(m.sender_id AS JSON) "
            "AND JSON_EXTRACT(r.payload,'$.collaboration.room_id')=CAST(? AS JSON) "
            "AND (("+epoch+" IS NULL AND ?=0) OR (JSON_TYPE("+epoch+")='INTEGER' AND "+epoch+"=CAST(? AS JSON))))) "
            "ORDER BY m.msg_seq DESC LIMIT ?";
        const auto generation=Id(scope.collaboration.at("context_generation"));
        values.insert(values.end(),{Id(room),Id(room),generation,generation,std::to_string(limit+1)});
        auto guard=pool_->Acquire();auto rows=Query(guard.get(),sql.c_str(),values);
        const bool more=rows.size()>limit;if(more)rows.resize(limit);
        Json items=Json::array();
        for(const auto& row:rows) {
            const auto seq=std::stoll(row[0]);
            const auto returned=std::count_if(row[3].begin(),row[3].end(),[](unsigned char byte){return (byte&0xc0)!=0x80;});
            Json ref={{"kind","message"},{"room_id",room},{"msg_id","r_"+Id(room)+"-"+Id(seq)},
                {"msg_seq",seq},{"offset",offset},{"length",returned},{"source_sha256",row[5]},
                {"sha256",Sha256Hex(row[3])}};
            items.push_back({{"reference",ref},{"sender_user_id",std::stoll(row[1])},
                {"timestamp_ms",std::stoll(row[2])},{"text",row[3]},{"total_characters",std::stoull(row[4])}});
        }
        *out={{"items",items},{"has_more",more},{"before_seq",items.empty()?before:items.back().at("reference").at("msg_seq").get<int64_t>()}};
        return {};
    }catch(const std::exception&){return Unavailable();}
}

ServiceStatus MySqlMediaContextRepository::Media(const MediaContextScope& scope,const std::string& query,
        uint64_t before,uint64_t limit,Json* out) {
    try {
        const auto room=scope.collaboration.at("room_id").get<int64_t>();
        auto guard=pool_->Acquire();
        auto rows=Query(guard.get(),"SELECT a.id,a.display_name,a.mime,p.revision_id,r.manifest_sha,r.fragment_count,"
            "COALESCE(v.artifact_count,0),COALESCE(v.coverage,'{}'),m.ordinal FROM attachment a "
            "JOIN room_media_asset m ON m.asset_id=a.id JOIN room_media_publication p ON p.asset_id=a.id "
            "JOIN room_media_revision r ON r.revision_id=p.revision_id AND r.asset_id=a.id "
            "LEFT JOIN room_media_revision_profile v ON v.revision_id=p.revision_id WHERE a.session_id=BINARY ? "
            "AND a.status='ready' AND r.state='ready' AND m.ordinal<? AND (LOCATE(?,a.display_name)>0 OR EXISTS "
            "(SELECT 1 FROM room_media_fragment f WHERE f.revision_id=p.revision_id AND LOCATE(?,f.text)>0)) "
            "ORDER BY m.ordinal DESC LIMIT ?",{"r_"+Id(room),std::to_string(before),query,query,std::to_string(limit+1)});
        const bool more=rows.size()>limit;if(more)rows.resize(limit);Json items=Json::array();
        for(const auto& row:rows)items.push_back({{"reference",{{"kind","media"},{"room_id",room},{"asset_id",row[0]},
            {"revision",row[3]},{"sha256",row[4]}}},{"name",row[1]},{"mime",row[2]},
            {"fragment_count",std::stoull(row[5])},{"image_count",std::stoull(row[6])},
            {"coverage",Json::parse(row[7])},{"ordinal",std::stoull(row[8])}});
        *out={{"items",items},{"has_more",more},{"before_ordinal",items.empty()?before:items.back().at("ordinal").get<uint64_t>()}};
        return {};
    }catch(const std::exception&){return Unavailable();}
}

ServiceStatus MySqlMediaContextRepository::Record(const MediaContextScope& scope,const Json& refs,Json* out) {
    try {
        auto guard=pool_->Acquire();auto* conn=guard.get();media_storage::NextTransactionCurrent(conn);room_sql::Transaction tx(conn);
        const auto room=Id(scope.collaboration.at("room_id"));
        if(Query(conn,"SELECT id FROM im_group WHERE id=? FOR UPDATE",{room}).empty())return Changed();
        const auto current=Query(conn,"SELECT generation FROM room_context WHERE room_id=? FOR UPDATE",{room});
        const int64_t generation=current.empty()?0:std::stoll(current[0][0]);
        if(scope.collaboration.at("context_generation")!=generation)return Changed();
        const auto request=Query(conn,"SELECT payload FROM room_agent_request WHERE request_id=? FOR UPDATE",{scope.request_id});
        if(request.size()!=1)return Changed();
        const auto canonical=Json::parse(request[0][0]);
        if(canonical.at("collaboration")!=scope.collaboration || canonical.at("message_seq")!=scope.message_ceiling ||
            canonical.at("request_id")!=scope.request_id)return Changed();
        auto count=std::stoull(Query(conn,"SELECT COUNT(*) FROM room_context_source WHERE request_id=?",{scope.request_id})[0][0]);
        Json receipts=Json::array();
        for(const auto& ref:refs) {
            if(!Current(conn,scope,ref))return Changed();
            const auto encoded=ref.dump();
            const std::string id="src_"+Sha256Hex(Json::array({"room-context-source-v1",scope.request_id,ref}).dump());
            const auto prior=Query(conn,"SELECT reference FROM room_context_source WHERE request_id=? AND source_id=?",{scope.request_id,id});
            if(prior.empty()) {
                if(count>=8192)return {429,"request source receipt budget exhausted"};
                Query(conn,"INSERT INTO room_context_source(request_id,source_id,room_id,context_generation,reference) VALUES(?,?,?,?,?)",
                    {scope.request_id,id,room,Id(generation),encoded});++count;
            } else if(Json::parse(prior[0][0])!=ref)return Changed();
            receipts.push_back(id);
        }
        tx.Commit();*out=std::move(receipts);return {};
    }catch(const std::exception&){return Unavailable();}
}
}  // namespace sparkpush
