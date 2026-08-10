#pragma once

#include <string>

namespace sparkpush {

struct HermesBridgeConfig {
    std::string kafka_brokers{"127.0.0.1:9092"};
    std::string request_topic{"ai_request"};
    std::string delta_topic{"ai_delta"};
    std::string reply_topic{"ai_reply"};
    std::string consumer_group{"spark_push_hermes_bridge"};

    // 第一阶段只支持本地 HTTP Hermes API；HTTPS/TLS 可在反向代理层终止。
    std::string hermes_base_url;
    std::string hermes_api_key;
    std::string hermes_model{"hermes-agent"};
    bool streaming{true};
    int request_timeout_ms{120000};
    int reply_delivery_timeout_ms{5000};
};

bool LoadHermesBridgeConfig(const std::string& path,
                            HermesBridgeConfig* config,
                            std::string* err_msg);

}  // namespace sparkpush
