#include <cstdlib>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include "redis_store.h"
using namespace sparkpush;
void Require(bool value) { if (!value) throw std::runtime_error("route fencing regression"); }
int main() {
    if (!std::getenv("SPARK_PUSH_RUN_REDIS_TESTS")) return 77;
    RedisConfig config;
    if (const char* host = std::getenv("SPARK_PUSH_REDIS_HOST")) config.host=host; else config.host="127.0.0.1";
    if (const char* port = std::getenv("SPARK_PUSH_REDIS_PORT")) config.port=std::stoi(port);
    if (const char* password = std::getenv("SPARK_PUSH_REDIS_PASSWORD")) config.password=password;
    RedisConnectionPool pool;
    Require(pool.Init(config));
    RedisStore routes(&pool);
    const int64_t user = 700000000000LL + getpid();
    const std::string comet="route-test-" + std::to_string(getpid());
    for (int i = 0; i < 50; ++i) {
        int64_t old_generation, new_generation;
        std::string old_token, new_token;
        Require(routes.AddRouteGeneration(user, comet, &old_generation, &old_token));
        bool removed=false, added=false;
        std::thread disconnect([&] { removed=routes.RemoveRouteGeneration(user,comet,old_generation,old_token); });
        std::thread reconnect([&] { added=routes.AddRouteGeneration(user,comet,&new_generation,&new_token); });
        disconnect.join(); reconnect.join();
        Require(removed && added && new_generation > old_generation && new_token != old_token);
        std::vector<std::string> found;
        Require(routes.GetUserRoutes(user,&found) && found.size()==1 && found[0]==comet);
        Require(routes.RemoveRouteGeneration(user,comet,new_generation,new_token));
        Require(routes.GetUserRoutes(user,&found) && found.empty());
    }
    // Losing the counter must not let an old same-number lease delete a replacement.
    int64_t old_generation, replacement;
    std::string old_token, replacement_token;
    Require(routes.AddRouteGeneration(user,comet,&old_generation,&old_token));
    const auto counter="route:generation:" + std::to_string(user) + ":" + comet;
    {
        auto guard=pool.Acquire();
        auto* reply=static_cast<redisReply*>(redisCommand(guard.get(),"DEL %s",counter.c_str()));
        Require(reply && reply->type==REDIS_REPLY_INTEGER); freeReplyObject(reply);
    }
    Require(routes.AddRouteGeneration(user,comet,&replacement,&replacement_token));
    Require(routes.RemoveRouteGeneration(user,comet,old_generation,old_token));
    std::vector<std::string> found;
    Require(routes.GetUserRoutes(user,&found) && found.size()==1);
    Require(routes.RemoveRouteGeneration(user,comet,replacement,replacement_token));
    auto guard=pool.Acquire();
    const auto key="route:generation:" + std::to_string(user) + ":" + comet;
    auto* reply=static_cast<redisReply*>(redisCommand(guard.get(),"DEL %s",key.c_str()));
    if(reply) freeReplyObject(reply);
}
