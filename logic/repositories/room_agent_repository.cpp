#include "repositories/room_agent_repository.h"
#include "repositories/room_sql.h"
namespace sparkpush {
namespace {
using Json=nlohmann::json;
using room_sql::Query;
std::string Id(int64_t id) { return std::to_string(id); }
template<class F> bool Run(MySqlConnectionPool* pool,std::string* error,F fn) {
    try {
        if(!pool) throw std::runtime_error("room Agent database unavailable");
        auto guard=pool->Acquire();
        if(!guard.get()) throw std::runtime_error("room Agent database unavailable");
        return fn(guard.get());
    } catch(const std::exception&) { if(error) *error="room Agent database operation failed"; return false; }
}
Json One(MYSQL* conn,const char* sql,const std::vector<std::string>& values) {
    const auto rows=Query(conn,sql,values);
    return rows.empty() || rows[0][0].empty() ? Json(nullptr) : Json::parse(rows[0][0]);
}
}
bool MySqlRoomAgentRepository::EnsureSchema(std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) {
        Query(conn,"CREATE TABLE IF NOT EXISTS room_agent_task (room_id BIGINT PRIMARY KEY, revision BIGINT NOT NULL, payload JSON NOT NULL) ENGINE=InnoDB");
        Query(conn,"CREATE TABLE IF NOT EXISTS room_agent_checkpoint (room_id BIGINT NOT NULL, revision BIGINT NOT NULL, payload JSON NOT NULL, PRIMARY KEY(room_id,revision)) ENGINE=InnoDB");
        Query(conn,"CREATE TABLE IF NOT EXISTS room_agent_request (request_id VARCHAR(121) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY, room_id BIGINT NOT NULL, message_seq BIGINT NOT NULL, payload JSON NOT NULL, result JSON NULL, INDEX room_requests(room_id,message_seq)) ENGINE=InnoDB");
        return true;
    });
}
bool MySqlRoomAgentRepository::Access(int64_t room,int64_t user,int64_t* owner,bool* member,std::string* error) {
    *owner=0; *member=false;
    return Run(pool_,error,[&](MYSQL* conn) {
        const auto rows=Query(conn,"SELECT g.owner_id,CASE WHEN u.status=1 AND u.deleted_at IS NULL THEN COALESCE(m.role,-1) ELSE -1 END FROM im_group g LEFT JOIN group_member m ON m.group_id=g.id AND m.user_id=? LEFT JOIN `user` u ON u.id=m.user_id WHERE g.id=? AND g.group_type=1",{Id(user),Id(room)});
        if(rows.empty()) return false;
        *owner=std::stoll(rows[0][0]); const int role=std::stoi(rows[0][1]); *member=role>=0 && role<=2; return true;
    });
}
bool MySqlRoomAgentRepository::Role(int64_t room,int64_t user,int* role,std::string* error) {
    return members_ && members_->GetRole(room,user,role,error);
}
bool MySqlRoomAgentRepository::Load(int64_t room,Json* task,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) { *task=One(conn,"SELECT payload FROM room_agent_task WHERE room_id=?",{Id(room)}); return true; });
}
bool MySqlRoomAgentRepository::Save(int64_t room,int64_t revision,const Json& task,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) {
        room_sql::Transaction transaction(conn);
        uint64_t affected=0;
        if(revision==0)
            Query(conn,"INSERT INTO room_agent_task(room_id,revision,payload) VALUES(?,1,?) ON DUPLICATE KEY UPDATE room_id=VALUES(room_id)",{Id(room),task.dump()},&affected);
        else
            Query(conn,"UPDATE room_agent_task SET revision=revision+1,payload=? WHERE room_id=? AND revision=?",{task.dump(),Id(room),Id(revision)},&affected);
        if(affected!=1) { if(error)*error="task revision conflict"; return false; }
        Query(conn,"INSERT INTO room_agent_checkpoint(room_id,revision,payload) VALUES(?,?,?)",{Id(room),Id(revision+1),task.dump()});
        transaction.Commit(); return true;
    });
}
bool MySqlRoomAgentRepository::Request(const std::string& id,Json* request,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) { *request=One(conn,"SELECT payload FROM room_agent_request WHERE request_id=?",{id}); return true; });
}
bool MySqlRoomAgentRepository::PutRequest(const Json& request,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) {
        room_sql::Transaction transaction(conn);
        const auto id=request.at("request_id").get<std::string>();
        Query(conn,"INSERT INTO room_agent_request(request_id,room_id,message_seq,payload) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE request_id=VALUES(request_id)",
            {id,Id(request.at("collaboration").at("room_id").get<int64_t>()),Id(request.at("message_seq").get<int64_t>()),request.dump()});
        if(One(conn,"SELECT payload FROM room_agent_request WHERE request_id=?",{id})!=request) return false;
        if(request.at("collaboration").value("kind","")=="room_agent_v1")
            Query(conn,"INSERT INTO room_agent_dispatch(request_id,source_msg_id,session_id,message_seq) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE request_id=VALUES(request_id)",
                {id,request.at("source_msg_id"),request.at("session_id"),Id(request.at("message_seq").get<int64_t>())});
        transaction.Commit();return true;
    });
}
bool MySqlRoomAgentRepository::Finish(const std::string& id,const Json& result,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) {
        Query(conn,"UPDATE room_agent_request SET result=? WHERE request_id=? AND result IS NULL",{result.dump(),id});
        auto saved=One(conn,"SELECT result FROM room_agent_request WHERE request_id=?",{id});
        auto replay=result;
        // Bridge restart may reconstruct the same cached runtime result with a
        // new delivery timestamp. Keep the first receipt; reject semantic changes.
        if(saved.is_object()) saved.erase("completed_at_ms");
        if(replay.is_object()) replay.erase("completed_at_ms");
        return saved==replay;
    });
}
bool MySqlRoomAgentRepository::Requests(int64_t room,Json* requests,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn) {
        *requests=Json::array();
        for(const auto& row:Query(conn,catalog_schema_?
            "SELECT r.payload,COALESCE(CAST(r.result AS CHAR),'null'),COALESCE(d.published,0),COALESCE(d.attempted,0),COALESCE(d.error,'') FROM room_agent_request r LEFT JOIN room_agent_dispatch d ON d.request_id=r.request_id WHERE r.room_id=? ORDER BY r.message_seq DESC LIMIT 40":
            "SELECT payload,COALESCE(CAST(result AS CHAR),'null'),'0','0','' FROM room_agent_request WHERE room_id=? ORDER BY message_seq DESC LIMIT 20",{Id(room)})) {
            const auto request=Json::parse(row[0]);auto completed=Json::parse(row[1]);
            std::string status=completed.is_null()?(request.contains("agent_id")?(row[3]=="1"?"unknown":"queued"):"submitted"):completed.value("ok",false)?"completed":"failed";
            if(!completed.is_null() && !completed.value("ok",false) && completed.value("terminal_status",Json())=="cancelled") status="cancelled";
            if(completed.is_null() && row[2]=="2") {
                status=row[3]=="1"?"unknown":"cancelled";
                completed={{"ok",false},{"text",row[4]},{"not_executed",row[3]!="1"}};
            }
            requests->push_back({{"request_id",request.at("request_id")},{"actor_user_id",request.at("user_id")},
                {"agent_id",request.value("agent_id","cann")},{"agent_name",request.value("agent_name","CANN")},{"source_msg_id",request.value("source_msg_id",request.at("request_id").get<std::string>())},{"message_seq",request.at("message_seq")},{"status",status},{"result",completed}});
        }
        return true;
    });
}

bool MySqlRoomAgentRepository::CheckCatalogSchema(std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        Query(conn,"SELECT request_id,input_digest,reply FROM room_agent_publication LIMIT 0");
        Query(conn,"SELECT room_id,project_id,generation,updated_by FROM room_workspace LIMIT 0");
        Query(conn,"SELECT task_id,room_id,revision,payload FROM room_collaboration_task LIMIT 0");
        Query(conn,"SELECT task_id,revision,payload FROM room_collaboration_task_history LIMIT 0");
        Query(conn,"SELECT room_id,agent_id,installed,generation,installed_by FROM room_agent_binding LIMIT 0");
        Query(conn,"SELECT request_id,source_msg_id,session_id,message_seq,published,attempted,error FROM room_agent_dispatch LIMIT 0");catalog_schema_=true;return true;
    });
}
bool MySqlRoomAgentRepository::Bindings(int64_t room,Json* bindings,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        *bindings=Json::array();
        for(const auto& row:Query(conn,"SELECT agent_id,installed,generation FROM room_agent_binding WHERE room_id=? ORDER BY agent_id",{Id(room)}))
            bindings->push_back({{"agent_id",row[0]},{"installed",row[1]=="1"},{"generation",std::stoll(row[2])}});
        return true;
    });
}
bool MySqlRoomAgentRepository::SetBinding(int64_t room,int64_t owner,const std::string& agent,bool installed,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        room_sql::Transaction transaction(conn);
        const auto row=Query(conn,"SELECT owner_id FROM im_group WHERE id=? FOR UPDATE",{Id(room)});
        if(row.empty() || row[0][0]!=Id(owner))return false;
        Query(conn,"INSERT INTO room_agent_binding(room_id,agent_id,installed,installed_by) VALUES(?,?,?,?) ON DUPLICATE KEY UPDATE generation=generation+(installed<>VALUES(installed)),installed=VALUES(installed),installed_by=VALUES(installed_by)",{Id(room),agent,installed?"1":"0",Id(owner)});
        transaction.Commit();return true;
    });
}
bool MySqlRoomAgentRepository::Workspace(int64_t room,Json* output,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        const auto rows=Query(conn,"SELECT project_id,generation FROM room_workspace WHERE room_id=?",{Id(room)});
        *output=rows.empty()?Json{{"project_id",""},{"generation",0}}:
            Json{{"project_id",rows[0][0]},{"generation",std::stoll(rows[0][1])}};
        return true;
    });
}
bool MySqlRoomAgentRepository::SetWorkspace(int64_t room,int64_t owner,int64_t generation,const std::string& project,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        room_sql::Transaction transaction(conn);
        const auto group=Query(conn,"SELECT owner_id FROM im_group WHERE id=? FOR UPDATE",{Id(room)});
        if(group.empty() || group[0][0]!=Id(owner))return false;
        const auto rows=Query(conn,"SELECT project_id,generation FROM room_workspace WHERE room_id=? FOR UPDATE",{Id(room)});
        if((rows.empty()?0:std::stoll(rows[0][1]))!=generation)return false;
        if(rows.empty())Query(conn,"INSERT INTO room_workspace(room_id,project_id,generation,updated_by) VALUES(?,?,1,?)",{Id(room),project,Id(owner)});
        else if(rows[0][0]!=project)Query(conn,"UPDATE room_workspace SET project_id=?,generation=generation+1,updated_by=? WHERE room_id=?",{project,Id(owner),Id(room)});
        transaction.Commit();return true;
    });
}
bool MySqlRoomAgentRepository::Publication(const Json& request,const std::string& digest,
    const PublicationDecision& decide,Json* output,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        room_sql::Transaction transaction(conn);
        const auto id=request.at("request_id").get<std::string>();
        const auto room=Id(request.at("collaboration").at("room_id"));
        const auto actor=Id(request.at("user_id"));
        // All member, binding and workspace mutations lock im_group first.
        // The user row additionally serializes disabling/deleting an account.
        const auto group=Query(conn,"SELECT group_type FROM im_group WHERE id=? FOR UPDATE",{room});
        const auto user=Query(conn,"SELECT status,deleted_at IS NULL FROM `user` WHERE id=? FOR UPDATE",{actor});
        if(One(conn,"SELECT payload FROM room_agent_request WHERE request_id=? FOR UPDATE",{id})!=request)return false;
        const auto saved=Query(conn,"SELECT input_digest,reply FROM room_agent_publication WHERE request_id=? FOR UPDATE",{id});
        if(!saved.empty()) {
            if(saved[0][0]!=digest){if(error)*error="conflicting Agent final payload";return false;}
            *output=Json::parse(saved[0][1]);transaction.Commit();return true;
        }
        const auto member=Query(conn,"SELECT role FROM group_member WHERE group_id=? AND user_id=? FOR UPDATE",{room,actor});
        const auto workspace=Query(conn,"SELECT project_id,generation FROM room_workspace WHERE room_id=? FOR UPDATE",{room});
        Json bindings=Json::array();
        for(const auto& row:Query(conn,"SELECT agent_id,installed,generation FROM room_agent_binding WHERE room_id=? FOR UPDATE",{room}))
            bindings.push_back({{"agent_id",row[0]},{"installed",row[1]=="1"},{"generation",std::stoll(row[2])}});
        const bool active=!group.empty() && group[0][0]=="1" && !user.empty() && user[0][0]=="1" && user[0][1]=="1" &&
            !member.empty() && std::stoi(member[0][0])>=0 && std::stoi(member[0][0])<=2;
        *output=decide({{"member",active},{"bindings",bindings},{"workspace",workspace.empty()?Json{{"project_id",""},{"generation",0}}:
            Json{{"project_id",workspace[0][0]},{"generation",std::stoll(workspace[0][1])}}}});
        Query(conn,"INSERT INTO room_agent_publication(request_id,input_digest,reply) VALUES(?,?,?)",{id,digest,output->dump()});
        // This commit is the authorization linearization point. Kafka/outbox
        // confirmation and request completion still occur in their usual order.
        transaction.Commit();return true;
    });
}
bool MySqlRoomAgentRepository::Pending(Json* requests,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        *requests=Json::array();
        // Match source identity as well as sequence. Stage 0/1/4 cannot start a model.
        for(const auto& row:Query(conn,
            "SELECT r.payload FROM room_agent_dispatch d JOIN room_agent_request r ON r.request_id=d.request_id "
            "WHERE d.published=0 AND (EXISTS(SELECT 1 FROM message_reservation m WHERE m.session_id=BINARY d.session_id AND m.msg_seq=d.message_seq AND m.stage IN(2,3) AND CONCAT(m.session_id,'-',m.msg_seq)=BINARY d.source_msg_id) "
            "OR EXISTS(SELECT 1 FROM message m WHERE m.session_id=BINARY d.session_id AND m.msg_seq=d.message_seq AND JSON_UNQUOTE(JSON_EXTRACT(m.content_json,'$.msg_id'))=BINARY d.source_msg_id)) ORDER BY d.created_at,d.message_seq,d.request_id LIMIT 10"))
            requests->push_back(Json::parse(row[0]));
        return true;
    });
}
bool MySqlRoomAgentRepository::Dispatched(const std::string& id,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){Query(conn,"UPDATE room_agent_dispatch SET published=1 WHERE request_id=?",{id});return true;});
}

bool MySqlRoomAgentRepository::MarkAttempt(const std::string& id,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){Query(conn,"UPDATE room_agent_dispatch SET attempted=1 WHERE request_id=? AND published=0",{id});return true;});
}
bool MySqlRoomAgentRepository::RecoverDispatch(const std::string& id,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        room_sql::Transaction tx(conn);
        const auto result=Query(conn,"SELECT CAST(result AS CHAR) FROM room_agent_request WHERE request_id=? FOR UPDATE",{id});
        if(result.empty() || !result[0][0].empty())return false;
        const auto dispatch=Query(conn,"SELECT published FROM room_agent_dispatch WHERE request_id=? FOR UPDATE",{id});
        if(dispatch.empty() || dispatch[0][0]=="2")return false;
        Query(conn,"UPDATE room_agent_dispatch SET published=0,error='' WHERE request_id=? AND published<>2",{id});
        tx.Commit();return true;
    });
}
bool MySqlRoomAgentRepository::BlockDispatch(const std::string& id,const std::string& reason,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){Query(conn,"UPDATE room_agent_dispatch SET published=2,error=? WHERE request_id=? AND published=0",{reason,id});return true;});
}

bool MySqlRoomAgentRepository::Task(int64_t room,const std::string& id,Json* task,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){*task=One(conn,"SELECT payload FROM room_collaboration_task WHERE room_id=? AND task_id=?",{Id(room),id});return true;});
}
bool MySqlRoomAgentRepository::Tasks(int64_t room,Json* result,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        Json tasks=Json::array();
        for(const auto& row:Query(conn,"SELECT payload FROM room_collaboration_task WHERE room_id=? ORDER BY created_at DESC LIMIT 100",{Id(room)})) {
            auto task=Json::parse(row[0]);Json history=Json::array();
            for(const auto& h:Query(conn,"SELECT payload FROM room_collaboration_task_history WHERE task_id=? ORDER BY revision DESC LIMIT 20",{task.at("task_id")}))history.push_back(Json::parse(h[0]));
            task["history"]=history;tasks.push_back(task);
        }
        *result={{"tasks",tasks}};return true;
    });
}
bool MySqlRoomAgentRepository::SaveTask(int64_t room,int64_t revision,const Json& task,std::string* error) {
    return Run(pool_,error,[&](MYSQL* conn){
        room_sql::Transaction transaction(conn);uint64_t affected=0;const auto id=task.at("task_id").get<std::string>();
        if(revision==0)Query(conn,"INSERT INTO room_collaboration_task(task_id,room_id,revision,payload) VALUES(?,?,1,?)",{id,Id(room),task.dump()},&affected);
        else Query(conn,"UPDATE room_collaboration_task SET revision=revision+1,payload=? WHERE task_id=? AND room_id=? AND revision=?",{task.dump(),id,Id(room),Id(revision)},&affected);
        if(affected!=1)return false;
        Query(conn,"INSERT INTO room_collaboration_task_history(task_id,revision,payload) VALUES(?,?,?)",{id,Id(revision+1),task.dump()});transaction.Commit();return true;
    });
}
}
