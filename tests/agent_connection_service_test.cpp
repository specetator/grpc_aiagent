#include "services/agent_connection_service.h"
#include <cassert>
#include <iostream>
using namespace sparkpush;
int main() {
    nlohmann::json seen, output;
    int calls=0;
    AgentConnectionService service([&](const nlohmann::json& input) {
        ++calls; seen=input;
        return nlohmann::json{{"code",0},{"data",{{"connections",nlohmann::json::array()}}}};
    });
    assert(service.Execute(7,{{"action","list"}},&output).ok());
    assert(seen["actor_user_id"]==7 && calls==1);
    assert(service.Execute(7,{{"action","save"},{"actor_user_id",8}},&output).code==400);
    assert(service.Execute(7,{{"action","save"},{"owner_user_ids",{8}}},&output).code==400);
    assert(service.Execute(7,{{"action","save"},{"binary","/bin/sh"}},&output).code==400);
    assert(service.Execute(7,{{"action","save"},{"endpoint",nullptr}},&output).code==400);
    assert(service.Execute(7,{{"action","save"},{"token",std::string(17000,'x')}},&output).code==400);
    assert(service.Execute(0,{{"action","list"}},&output).code==400);
    assert(calls==1);
    AgentConnectionService disabled({});
    assert(disabled.Execute(7,{{"action","list"}},&output).code==503);
    AgentConnectionService unavailable([](const nlohmann::json&)->nlohmann::json {
        throw std::runtime_error("secret upstream response");
    });
    const auto error=unavailable.Execute(7,{{"action","probe"}},&output);
    assert(error.code==503 && error.message.find("secret")==std::string::npos);
    bool rejected=false;
    try { AgentConnectionService::LocalGateway(8643,"bad\r\nheader"); }
    catch(const std::invalid_argument&) { rejected=true; }
    assert(rejected);
    std::cout << "Agent connection ownership, input and error contracts passed\n";
}
