#include "config.h"

#include <cstdlib>
#include <fstream>
#include <sstream>

namespace sparkpush {
namespace {

void Trim(std::string* value) {
    if (!value) return;
    const size_t begin = value->find_first_not_of(" \t\r\n");
    const size_t end = value->find_last_not_of(" \t\r\n");
    if (begin == std::string::npos || end == std::string::npos) {
        value->clear();
        return;
    }
    *value = value->substr(begin, end - begin + 1);
}

std::string ExpandEnvironment(const std::string& input) {
    std::string output;
    size_t cursor = 0;
    while (cursor < input.size()) {
        const size_t begin = input.find("${", cursor);
        if (begin == std::string::npos) {
            output.append(input, cursor, std::string::npos);
            break;
        }
        output.append(input, cursor, begin - cursor);
        const size_t end = input.find('}', begin + 2);
        if (end == std::string::npos) {
            output.append(input, begin, std::string::npos);
            break;
        }
        const std::string name = input.substr(begin + 2, end - begin - 2);
        const char* value = name.empty() ? nullptr : std::getenv(name.c_str());
        if (value) output += value;
        cursor = end + 1;
    }
    return output;
}

bool ParseInt(const std::string& value, int* output) {
    if (!output || value.empty()) return false;
    try {
        size_t consumed = 0;
        const int parsed = std::stoi(value, &consumed);
        if (consumed != value.size()) return false;
        *output = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

bool LoadHermesBridgeConfig(const std::string& path,
                            HermesBridgeConfig* config,
                            std::string* err_msg) {
    if (!config) {
        if (err_msg) *err_msg = "config output is null";
        return false;
    }
    std::ifstream input(path);
    if (!input) {
        if (err_msg) *err_msg = "cannot open config: " + path;
        return false;
    }

    std::string line;
    int line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        Trim(&line);
        if (line.empty() || line[0] == '#') continue;
        const size_t separator = line.find('=');
        if (separator == std::string::npos) continue;
        std::string key = line.substr(0, separator);
        std::string value = line.substr(separator + 1);
        Trim(&key);
        Trim(&value);
        value = ExpandEnvironment(value);

        if (key == "kafka_brokers") {
            config->kafka_brokers = value;
        } else if (key == "kafka_request_topic") {
            config->request_topic = value;
        } else if (key == "kafka_delta_topic") {
            config->delta_topic = value;
        } else if (key == "kafka_reply_topic") {
            config->reply_topic = value;
        } else if (key == "kafka_consumer_group") {
            config->consumer_group = value;
        } else if (key == "hermes_base_url") {
            config->hermes_base_url = value;
        } else if (key == "hermes_api_key") {
            config->hermes_api_key = value;
        } else if (key == "hermes_model") {
            config->hermes_model = value;
        } else if (key == "hermes_streaming") {
            if (!value.empty()) {
                config->streaming = value == "true" || value == "1";
            }
        } else if (key == "request_timeout_ms") {
            if (!ParseInt(value, &config->request_timeout_ms)) {
                if (err_msg) *err_msg = "invalid request_timeout_ms at line " +
                                        std::to_string(line_number);
                return false;
            }
        } else if (key == "reply_delivery_timeout_ms") {
            if (!ParseInt(value, &config->reply_delivery_timeout_ms)) {
                if (err_msg) {
                    *err_msg = "invalid reply_delivery_timeout_ms at line " +
                               std::to_string(line_number);
                }
                return false;
            }
        }
    }

    const char* env_value = std::getenv("SPARK_PUSH_HERMES_BASE_URL");
    if (env_value) config->hermes_base_url = env_value;
    env_value = std::getenv("SPARK_PUSH_HERMES_API_KEY");
    if (env_value) config->hermes_api_key = env_value;
    env_value = std::getenv("SPARK_PUSH_HERMES_MODEL");
    if (env_value) config->hermes_model = env_value;
    env_value = std::getenv("SPARK_PUSH_HERMES_STREAMING");
    if (env_value && *env_value) {
        config->streaming = std::string(env_value) == "true" ||
                            std::string(env_value) == "1";
    }

    if (config->kafka_brokers.empty() || config->request_topic.empty() ||
        config->delta_topic.empty() ||
        config->reply_topic.empty() || config->consumer_group.empty()) {
        if (err_msg) *err_msg = "Kafka bridge configuration is incomplete";
        return false;
    }
    if (config->hermes_base_url.empty() || config->hermes_api_key.empty()) {
        if (err_msg) *err_msg =
            "SPARK_PUSH_HERMES_BASE_URL and SPARK_PUSH_HERMES_API_KEY are required";
        return false;
    }
    if (config->request_timeout_ms <= 0 ||
        config->reply_delivery_timeout_ms <= 0) {
        if (err_msg) *err_msg = "Hermes bridge timeouts must be positive";
        return false;
    }
    return true;
}

}  // namespace sparkpush
