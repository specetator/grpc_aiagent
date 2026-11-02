#include "services/media_context_service.h"
#include "services/media_json.h"
#include "services/media_manifest.h"
#include <map>
#include <set>
#include <limits>

namespace sparkpush {
namespace {
using media_json::Number;
using media_json::Fields;
using media_json::Hex;
using Json=nlohmann::json;
constexpr uint64_t kSafeInteger=9007199254740991ULL;
bool SameScope(const MediaContextScope& a,const MediaContextScope& b) {
    return a.request_id==b.request_id && a.collaboration==b.collaboration &&
        a.message_ceiling==b.message_ceiling && a.bot_users==b.bot_users;
}
Json Reference(int64_t room,const std::string& kind,const std::string& asset,const std::string& revision,const Json& item) {
    Json ref={{"kind",kind},{"room_id",room},{"asset_id",asset},{"revision",revision},
        {"index",item.at("index")},{"sha256",item.at("sha256")}};
    const auto& content=kind=="fragment"?item.at("content"):item;
    if(content.contains("anchor"))ref["anchor"]=content.at("anchor");
    // The original UTF-8 text pipeline uses line ranges rather than media anchors.
    for(const auto* key:{"line_start","line_end","byte_start","byte_end"})
        if(content.contains(key))ref[key]=content.at(key);
    return ref;
}
}

ServiceStatus MediaContextService::Authorize(const std::string& key,const std::string& id,MediaContextScope* scope) {
    Json authorized;auto status=agents_->AuthorizeContext(key,id,&authorized);if(!status.ok())return status;
    scope->request_id=id;scope->collaboration=authorized.at("collaboration");
    scope->message_ceiling=authorized.at("message_ceiling");scope->bot_users=authorized.at("bot_users").get<std::vector<int64_t>>();
    if(scope->message_ceiling<=0 || scope->message_ceiling>=static_cast<int64_t>(kSafeInteger))
        return {409,"room source message range unavailable"};
    return {};
}

ServiceStatus MediaContextService::Execute(const std::string& key,const std::string& operation,const Json& input,Json* output) {
    if(output)*output=Json::object();
    if(!output || !agents_ || !repository_ || !processing_ || !files_)return {503,"room context unavailable"};
    static const std::map<std::string,std::set<std::string>> fields={
        {"search",{"request_id","query","type","limit","before_seq","before_ordinal"}},
        {"read",{"request_id","kind","msg_seq","asset_id","revision","offset","limit"}},
        {"media",{"request_id","asset_id","revision","index"}}};
    const auto allowed=fields.find(operation);
    if(allowed==fields.end() || !Fields(input,allowed->second))return {400,"invalid room context fields"};
    try {
        const auto id=input.at("request_id").get<std::string>();
        MediaContextScope scope;auto status=Authorize(key,id,&scope);if(!status.ok())return status;
        const auto room=scope.collaboration.at("room_id").get<int64_t>();
        const auto actor=scope.collaboration.at("actor_user_id").get<int64_t>();
        Json result={{"request_id",id},{"context_generation",scope.collaboration.at("context_generation")},{"items",Json::array()}};
        if(operation=="search") {
            const auto query=input.value("query",std::string());const auto type=input.value("type",std::string("all"));
            const auto limit=Number(input,"limit",20,10);
            if(query.size()>240 || query.find('\0')!=std::string::npos || !limit ||
                (type!="all" && type!="messages" && type!="media" && type!="fragments") ||
                (type=="fragments" && query.empty()))return {400,"invalid bounded context search"};
            if(type=="all" || type=="messages") {
                Json messages;status=repository_->Messages(scope,query,Number(input,"before_seq",kSafeInteger,scope.message_ceiling+1),0,0,300,limit,&messages);
                if(!status.ok())return status;
                for(auto& item:messages.at("items"))result["items"].push_back(std::move(item));
                result["messages_page"]={{"before_seq",messages.at("before_seq")},{"has_more",messages.at("has_more")}};
            }
            if(type=="all" || type=="media") {
                Json media;status=repository_->Media(scope,query,Number(input,"before_ordinal",kSafeInteger,kSafeInteger),limit,&media);
                if(!status.ok())return status;
                for(auto& item:media.at("items"))result["items"].push_back(std::move(item));
                result["media_page"]={{"before_ordinal",media.at("before_ordinal")},{"has_more",media.at("has_more")}};
            }
            if((type=="all" || type=="fragments") && !query.empty()) {
                Json matches;status=processing_->Member(actor,"search",{{"room_id",room},{"query",query},{"limit",limit}},&matches);
                if(!status.ok())return status;
                for(const auto& item:matches.at("items"))result["items"].push_back({
                    {"reference",Reference(room,"fragment",item.at("id"),item.at("revision"),item)},
                    {"name",item.at("name")},{"content",item.at("content")}});
                // Use the returned asset/revision/index to continue reading;
                // refine the literal query if more matching fragments exist.
                result["fragments_has_more"]=matches.at("has_more");
            }
        } else if(operation=="read" && input.value("kind",std::string())=="message") {
            if(input.contains("asset_id") || input.contains("revision"))return {400,"message read does not accept media identity"};
            const auto sequence=Number(input,"msg_seq",kSafeInteger),offset=Number(input,"offset",16*1024*1024),length=Number(input,"limit",4096,4096);
            if(!sequence || sequence>static_cast<uint64_t>(scope.message_ceiling) || !length)return {400,"invalid message range"};
            Json messages;status=repository_->Messages(scope,"",scope.message_ceiling+1,sequence,offset,length,1,&messages);
            if(!status.ok())return status;if(messages.at("items").empty())return {404,"authorized message not found"};
            auto& item=messages.at("items").at(0);
            if(offset>item.at("total_characters").get<uint64_t>())return {416,"message range outside content"};
            result["items"]=messages.at("items");
            result["next_offset"]=std::min<uint64_t>(offset+length,item.at("total_characters"));
            result["has_more"]=result.at("next_offset")<item.at("total_characters");
        } else {
            const auto asset=input.at("asset_id").get<std::string>(),revision=input.at("revision").get<std::string>();
            if(!IsValidAttachmentId(asset) || !Hex(revision))return {400,"fixed media identity required"};
            RoomMedia file;status=files_->Find(asset,&file);if(!status.ok())return status;
            // Even a member of both rooms cannot import another room's source.
            if(file.file.session_id!="r_"+std::to_string(room))return {404,"authorized media not found"};
            Json page;
            if(operation=="media") {
                if(!input.contains("index"))return {400,"image index required"};
                status=processing_->Member(actor,"artifact",{{"id",asset},{"revision",revision},{"index",Number(input,"index",1023)}},&page);
                if(!status.ok())return status;
                auto item=page.at("metadata");item["index"]=page.at("index");
                result["items"].push_back({{"reference",Reference(room,"image",asset,revision,item)},
                    {"mime",item.at("mime")},{"data",page.at("data")},{"metadata",page.at("metadata")}});
            } else {
                const auto kind=input.at("kind").get<std::string>();
                if(input.contains("msg_seq") || (kind!="fragment" && kind!="image_metadata"))return {400,"invalid media read kind"};
                const auto limit=Number(input,"limit",10,5),offset=Number(input,"offset",kind=="fragment"?kMediaParsedFragments:1024);
                if(!limit)return {400,"positive read limit required"};
                status=processing_->Member(actor,kind=="fragment"?"fragments":"artifacts",{{"id",asset},{"revision",revision},{"offset",offset},{"limit",limit}},&page);
                if(!status.ok())return status;
                for(const auto& item:page.at("items"))result["items"].push_back({{"reference",Reference(room,kind,asset,revision,item)},
                    {kind=="fragment"?"content":"metadata",kind=="fragment"?item.at("content"):item}});
                result["next_offset"]=page.at("next_offset");result["has_more"]=page.at("has_more");
            }
        }
        // Each new source_id field encodes at most 83 bytes (68 ASCII bytes
        // plus JSON key/quotes/comma); reserve 96 per item and envelope headroom.
        if(result.dump().size()+result.at("items").size()*96>2*1024*1024-4096)
            return {413,"context response too large; use a smaller range or limit"};
        MediaContextScope again;status=Authorize(key,id,&again);if(!status.ok())return status;
        if(!SameScope(again,scope))return {409,"room context changed"};
        Json refs=Json::array(),receipts;
        for(const auto& item:result.at("items"))refs.push_back(item.at("reference"));
        status=repository_->Record(scope,refs,&receipts);if(!status.ok())return status;
        status=Authorize(key,id,&again);if(!status.ok())return status;
        if(!SameScope(again,scope) || receipts.size()!=result.at("items").size())return {409,"room context changed"};
        for(size_t i=0;i<receipts.size();++i) {
            if(!receipts.at(i).is_string())return {503,"invalid context receipt"};
            const auto receipt=receipts.at(i).get<std::string>();
            if(receipt.size()!=68 || receipt.rfind("src_",0)!=0 || !Hex(receipt.substr(4)))return {503,"invalid context receipt"};
            result["items"][i]["source_id"]=receipt;
        }
        if(result.dump().size()>2*1024*1024-4096)return {413,"context response too large; use a smaller range or limit"};
        *output=std::move(result);return {};
    }catch(const Json::exception&){return {400,"invalid room context value"};}
     catch(const std::invalid_argument&){return {400,"invalid room context range"};}
}
}  // namespace sparkpush
