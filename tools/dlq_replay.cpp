#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <nlohmann/json.hpp>
#include "kafka_producer.h"

namespace {
std::string DecodeHex(const std::string& hex) {
    if (hex.size() % 2 || hex.size() > 32 * 1024 * 1024)
        throw std::runtime_error("invalid/oversized hex field");
    auto digit = [](char value) -> int {
        if (value >= '0' && value <= '9') return value - '0';
        if (value >= 'a' && value <= 'f') return value - 'a' + 10;
        if (value >= 'A' && value <= 'F') return value - 'A' + 10;
        throw std::runtime_error("invalid hex digit");
    };
    std::string bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2)
        bytes.push_back(static_cast<char>((digit(hex[i]) << 4) | digit(hex[i + 1])));
    return bytes;
}
}
int main(int argc, char** argv) {
    try {
        std::string input, topic, brokers;
        bool apply = false, recovery = false;
        for (int i = 1; i < argc; ++i) {
            const std::string option = argv[i];
            if (option == "--apply") apply = true;
            else if (option == "--recovery") recovery = true;
            else if ((option == "--file" || option == "--topic" || option == "--brokers") && i + 1 < argc) {
                const std::string value = argv[++i];
                if (option == "--file") input = value;
                else if (option == "--topic") topic = value;
                else brokers = value;
            } else throw std::runtime_error("usage: dlq_replay --file record.json --topic expected-topic [--recovery] [--apply --brokers host:port]");
        }
        if (input.empty() || topic.empty() || topic.size() > 249 ||
            (topic.size() >= 4 && topic.substr(topic.size() - 4) == ".dlq"))
            throw std::runtime_error("require input and a non-DLQ target topic");
        std::ifstream file(input, std::ios::binary | std::ios::ate);
        if (!file || file.tellg() < 0 || file.tellg() > 40 * 1024 * 1024)
            throw std::runtime_error("unreadable/oversized DLQ record");
        file.seekg(0);
        nlohmann::json record;
        file >> record;
        file >> std::ws;
        if (!file.eof() || record.value("schema", "") != "sparkpush.dead_letter.v1")
            throw std::runtime_error("require exactly one sparkpush.dead_letter.v1 record");
        const auto& source = recovery ? record.at("recovery") : record;
        if (source.at("topic").get<std::string>() != topic)
            throw std::runtime_error("record topic does not match explicit target");
        // Current Spark producers use non-null keys/payloads. Do not silently convert Kafka nulls.
        if (!recovery && (record.value("key_is_null", false) || record.value("payload_is_null", false)))
            throw std::runtime_error("null Kafka fields require a null-preserving replay client");
        const auto key = DecodeHex(source.at("key_hex").get<std::string>());
        const auto payload = DecodeHex(source.at("payload_hex").get<std::string>());
        std::cout << "Validated target=" << topic << " key_bytes=" << key.size()
                  << " payload_bytes=" << payload.size() << " mode=" << (apply ? "apply" : "dry-run") << '\n';
        if (!apply) return 0;
        if (brokers.empty()) throw std::runtime_error("--apply requires explicit --brokers");
        sparkpush::KafkaProducer producer;
        if (!producer.Init(brokers, topic) || !producer.SendAndWait(key, payload, 5000))
            throw std::runtime_error("replay not confirmed; retry the same record after checking broker state");
        std::cout << "Replay confirmed by Kafka; original offset and DLQ record remain unchanged\n";
        return 0;
    } catch (const nlohmann::json::exception&) {
        std::cerr << "Invalid DLQ schema/field types\n";
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
    }
    return 1;
}
