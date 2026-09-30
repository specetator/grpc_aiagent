#include "kafka_consumer.h"
#include "kafka_producer.h"
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
        Cluster cluster;
        sparkpush::KafkaProducer producer;
        Require(producer.Init(cluster.brokers, "parallel"), "producer");
        Require(producer.SendAndWait("session-a", "1", 3000), "seed 1");
        Require(producer.SendAndWait("session-a", "2", 3000), "seed 2");
        Require(producer.SendAndWait("session-b", "3", 3000), "seed 3");
        sparkpush::KafkaConsumer::Options options;
        options.processing_workers = 2;
        options.max_batch_records = 4;
        options.batch_window_ms = 100;
        options.session_timeout_ms = 6000;
        std::atomic<bool> release{false}, other_done{false};
        std::atomic<int> same_session{0};
        sparkpush::KafkaConsumer consumer;
        Require(consumer.Init(cluster.brokers, "parallel-ok", "parallel",
            [&](const std::string& key, const std::string& value) {
                if (key == "session-b") { other_done = true; return true; }
                if (value == "1") {
                    while (!release) std::this_thread::sleep_for(std::chrono::milliseconds(2));
                    Require(same_session++ == 0, "first session turn order");
                } else Require(same_session++ == 1, "second session turn order");
                return true;
            }, options), "parallel init");
        consumer.Start();
        Wait([&] { return other_done.load(); }, "different session did not run concurrently");
        const bool withheld = Committed(cluster, "parallel-ok") < 0 && same_session == 0;
        release = true;
        Wait([&] { return Committed(cluster, "parallel-ok") == 3; }, "durable batch commit");
        consumer.Stop();
        Require(withheld, "offset advanced past incomplete session");
        Require(same_session == 2, "same-session work lost");

        options.max_processing_attempts = 1;
        sparkpush::KafkaConsumer failure;
        Require(failure.Init(cluster.brokers, "parallel-fail", "parallel",
            [](const std::string&, const std::string& value) { return value != "1"; }, options), "failure init");
        failure.Start();
        Wait([&] { return failure.failed(); }, "failed batch did not halt");
        failure.Stop();
        Require(Committed(cluster, "parallel-fail") < 0, "failed batch offset skipped");
        std::atomic<int> replayed{0};
        sparkpush::KafkaConsumer replay;
        Require(replay.Init(cluster.brokers, "parallel-fail", "parallel",
            [&](const std::string&, const std::string&) { ++replayed; return true; }, options), "replay init");
        replay.Start();
        Wait([&] { return replayed == 3; }, "failed batch was not replayed");
        replay.Stop();
        std::cout << "Kafka parallel order, durable commit and replay passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n'; return 1;
    }
}
