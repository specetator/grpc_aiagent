#include "message_dao.h"
#include "user_session_state_dao.h"
#include "sql_helpers.h"
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <unistd.h>
using namespace sparkpush;
std::string Env(const char* n, const char* fallback) { auto* v=std::getenv(n);return v?v:fallback; }
void Need(bool yes, const std::string& msg) { if(!yes)throw std::runtime_error(msg); }
int main() {
    if(Env("SPARK_PUSH_RUN_MYSQL_TESTS","0")!="1")return 77;
    try {
        MySqlConfig cfg;cfg.host=Env("SPARK_PUSH_MYSQL_HOST","127.0.0.1");
        cfg.port=std::stoi(Env("SPARK_PUSH_MYSQL_PORT","3306"));cfg.user=Env("SPARK_PUSH_MYSQL_USER","root");
        cfg.password=Env("SPARK_PUSH_MYSQL_PASSWORD","");cfg.db=Env("SPARK_PUSH_MYSQL_TEST_DB","spark_push_test");
        Need(cfg.db!="spark_push","isolated test database required");
        MySqlConnectionPool pool;Need(pool.Init(cfg),"pool init");
        UserSessionStateDao state(&pool);MessageDao dao(&pool);std::string err;
        Need(state.EnsureReceivedSchema(&err),err);
        const std::string session="device_test_"+std::to_string(getpid());
        auto cleanup=[&] {
            auto guard=pool.Acquire();auto* c=guard.get();if(!c)return;
            auto where=" WHERE session_id="+SqlQuote(c,session);
            SqlExec(c,"DELETE FROM device_receipt"+where,nullptr);
            SqlExec(c,"DELETE FROM device_session_state"+where,nullptr);
            SqlExec(c,"DELETE FROM message"+where,nullptr);
        };
        struct Final { decltype(cleanup)& f;~Final(){f();} } final{cleanup};
        cleanup();
        auto insert=[&](int seq) {
            Message m;m.session_id=session;m.msg_seq=seq;m.sender_id=101;m.msg_type="text";
            m.content_json="{}";m.timestamp_ms=123;m.client_msg_id="receipt-test-"+std::to_string(seq);
            Need(dao.InsertMessage(m,&err),err);
        };
        insert(1);insert(3);
        Need(state.MarkReceived(102,"device-A",session,0,{3},&err),err);
        std::vector<Message> msgs;
        Need(dao.ListMessagesAfter(session,0,20,&msgs,&err,102,"device-A") && msgs.size()==1 && msgs[0].msg_seq==1,"sparse receipt hid a gap");
        Need(dao.ListMessagesAfter(session,0,20,&msgs,&err,102,"device-B") && msgs.size()==2,"device isolation failed");
        Need(state.MarkReceived(102,"device-A",session,1,{},&err),err);
        insert(2);
        Need(dao.ListMessagesAfter(session,1,20,&msgs,&err,102,"device-A") && msgs.size()==1 && msgs[0].msg_seq==2,"late missing sequence was lost");
        Need(state.MarkReceived(102,"device-A",session,3,{},&err),err);
        Need(state.MarkReceived(102,"device-A",session,1,{2},&err),err);
        int64_t prefix=0;Need(state.GetReceivedSeq(102,"device-A",session,&prefix,&err)&&prefix==3,"receipt prefix regressed");
        Need(state.GetReceivedSeq(101,"device-A",session,&prefix,&err)&&prefix==0,"user isolation failed");
        Need(!state.MarkReceived(102,"bad:device",session,0,{1},&err),"invalid device accepted");
        std::cout<<"PASS device isolation, sparse gap, late arrival, monotonic prefix, validation\n";
        return 0;
    }catch(const std::exception& ex){std::cerr<<ex.what()<<'\n';return 1;}
}
