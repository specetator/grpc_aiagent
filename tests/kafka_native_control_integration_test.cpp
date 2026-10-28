#include "kafka_consumer.h"
#include "kafka_producer.h"
#include "../hermes_bridge/native_control.h"
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {
void Require(bool value, const char* message) {
    if (!value) throw std::runtime_error(message);
}
template <class F> void Wait(F predicate, const char* reason) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(12);
    while (!predicate()) {
        Require(std::chrono::steady_clock::now() < deadline, reason);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}
struct Cluster {
    rd_kafka_t* owner;
    rd_kafka_mock_cluster_t* mock;
    std::string brokers;
    Cluster() {
        char error[512];
        auto* config = rd_kafka_conf_new();
        rd_kafka_conf_set(config, "log_level", "0", nullptr, 0);
        owner = rd_kafka_new(RD_KAFKA_PRODUCER, config, error, sizeof(error));
        Require(owner, "mock owner");
        mock = rd_kafka_mock_cluster_new(owner, 1);
        Require(mock, "mock cluster");
        brokers = rd_kafka_mock_cluster_bootstraps(mock);
        Require(!rd_kafka_mock_topic_create(mock, "parallel", 1, 1), "topic create");
    }
    ~Cluster() { rd_kafka_mock_cluster_destroy(mock); rd_kafka_destroy(owner); }
};
int64_t Committed(const Cluster& cluster, const std::string& group) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    std::string error;
    conf->set("bootstrap.servers", cluster.brokers, error);
    conf->set("group.id", group, error);
    conf->set("enable.auto.commit", "false", error);
    std::unique_ptr<RdKafka::KafkaConsumer> observer(RdKafka::KafkaConsumer::create(conf.get(), error));
    std::vector<RdKafka::TopicPartition*> parts{RdKafka::TopicPartition::create("parallel", 0)};
    Require(observer->committed(parts, 3000) == RdKafka::ERR_NO_ERROR, "offset query");
    const auto offset = parts[0]->offset();
    RdKafka::TopicPartition::destroy(parts); observer->close();
    return offset;
}
}

int main() {
    try {
        using sparkpush::IsNativeInterrupt;
        const std::set<int64_t> contacts{900000000101LL};
        auto req = nlohmann::json{{"call_model", false}, {"bot_user_id", 900000000101LL},
                                 {"command", {{"name", "stop"}, {"local_error", ""}}}};
        Require(IsNativeInterrupt(req.dump(), contacts), "native stop not selected");
        for (const auto* command : {"approve", "deny", "status"}) {
            req["command"]["name"] = command;
            Require(IsNativeInterrupt(req.dump(), contacts), "native control not selected");
        }
        for (const auto* command : {"new", "model", "retry", "restart", "compact"}) {
            req["command"]["name"] = command;
            Require(!IsNativeInterrupt(req.dump(), contacts), "ordinary control escaped FIFO");
        }
        req["command"]["name"] = "stop";
        req["bot_user_id"] = 900000000001LL;
        Require(!IsNativeInterrupt(req.dump(), contacts), "Pi entered native control lane");
        req["bot_user_id"] = 900000000101LL;
        req["call_model"] = true;
        Require(!IsNativeInterrupt(req.dump(), contacts), "prompt entered control lane");
        Require(!IsNativeInterrupt("invalid", contacts), "invalid JSON accepted");

        Cluster cluster;
        sparkpush::KafkaProducer producer;
        Require(producer.Init(cluster.brokers, "parallel"), "producer");
        Require(producer.SendAndWait("same-session", "long", 3000), "seed long");
        Require(producer.SendAndWait("same-session", "followup", 3000), "seed followup");
        Require(producer.SendAndWait("same-session", "stop", 3000), "seed stop");
        sparkpush::KafkaConsumer::Options options;
        options.max_batch_records = 8;
        options.max_processing_attempts = 1;
        options.session_timeout_ms = 6000;
        options.interrupt_predicate = [](const std::string&, const std::string& value) {
            return value == "stop";
        };
        std::atomic<bool> release{false}, controlled{false}, entered{false};
        std::atomic<int> followups{0};
        sparkpush::KafkaConsumer consumer;
        Require(consumer.Init(cluster.brokers, "native-ok", "parallel",
            [&](const std::string&, const std::string& value) {
                if (value == "stop") { controlled = true; return true; }
                if (value == "long") {
                    entered = true;
                    Wait([&] { return release.load(); }, "test did not release long run");
                } else { Require(release, "followup overtook active session"); ++followups; }
                return true;
            }, options), "control lane init");
        consumer.Start();
        Wait([&] { return controlled.load() && entered.load(); }, "stop blocked behind long task");
        const bool withheld = Committed(cluster, "native-ok") < 0 && followups == 0;
        release = true;
        Wait([&] { return Committed(cluster, "native-ok") == 3; }, "control prefix commit");
        consumer.Stop();
        Require(withheld, "control advanced offset past incomplete task");
        Require(followups == 1, "followup lost or repeated");

        controlled = false;
        sparkpush::KafkaConsumer failure;
        Require(failure.Init(cluster.brokers, "native-fail", "parallel",
            [&](const std::string&, const std::string& value) {
                if (value == "stop") { controlled = true; return true; }
                Wait([&] { return controlled.load(); }, "failure control never delivered");
                return false;
            }, options), "failure init");
        failure.Start();
        Wait([&] { return failure.failed(); }, "failed work did not halt");
        failure.Stop();
        Require(Committed(cluster, "native-fail") < 0, "control skipped failed earlier offset");
        std::atomic<int> replayed{0};
        sparkpush::KafkaConsumer replay;
        options.interrupt_predicate = {};
        Require(replay.Init(cluster.brokers, "native-fail", "parallel",
            [&](const std::string&, const std::string&) { ++replayed; return true; }, options), "replay init");
        replay.Start();
        Wait([&] { return replayed == 3; }, "uncommitted requests not replayed");
        replay.Stop();
        std::cout << "Native control preemption, FIFO, durable offsets and failure replay passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n'; return 1;
    }
}
