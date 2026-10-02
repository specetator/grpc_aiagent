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
int64_t Committed(const Cluster& cluster, const std::string& group,
                  const std::string& topic = "parallel", int partition = 0) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    std::string error;
    conf->set("bootstrap.servers", cluster.brokers, error);
    conf->set("group.id", group, error);
    conf->set("enable.auto.commit", "false", error);
    std::unique_ptr<RdKafka::KafkaConsumer> observer(RdKafka::KafkaConsumer::create(conf.get(), error));
    std::vector<RdKafka::TopicPartition*> parts{RdKafka::TopicPartition::create(topic, partition)};
    Require(observer->committed(parts, 3000) == RdKafka::ERR_NO_ERROR, "offset query");
    const auto offset = parts[0]->offset();
    RdKafka::TopicPartition::destroy(parts); observer->close();
    return offset;
}
void SeedPartitions(const Cluster& cluster, const std::string& topic) {
    char error[512];
    auto* conf = rd_kafka_conf_new();
    Require(rd_kafka_conf_set(conf, "bootstrap.servers", cluster.brokers.c_str(),
                            error, sizeof(error)) == RD_KAFKA_CONF_OK, "seed brokers");
    auto* producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
    Require(producer, "partition seed producer");
    auto* handle = rd_kafka_topic_new(producer, topic.c_str(), nullptr);
    Require(handle, "partition seed topic");
    for (int partition = 0; partition < 2; ++partition) {
        const std::string key = "partition-" + std::to_string(partition);
        for (int index = 0; index < 3; ++index) {
            const std::string payload = std::to_string(index);
            Require(rd_kafka_produce(handle, partition, RD_KAFKA_MSG_F_COPY,
                const_cast<char*>(payload.data()), payload.size(), key.data(), key.size(), nullptr) == 0,
                "partition seed record");
        }
    }
    Require(rd_kafka_flush(producer, 5000) == RD_KAFKA_RESP_ERR_NO_ERROR, "partition seed flush");
    rd_kafka_topic_destroy(handle); rd_kafka_destroy(producer);
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
        // The second OffsetCommit must never be used for this three-record
        // batch. A per-record commit loop would halt on that rejection.
        rd_kafka_mock_push_request_errors(cluster.mock, 8, 2,
            RD_KAFKA_RESP_ERR_NO_ERROR, RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED);
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
        Require(!consumer.failed(), "batch issued a redundant per-record commit");
        rd_kafka_mock_clear_request_errors(cluster.mock, 8);

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

        std::atomic<bool> cancelled{false};
        options.processing_cancelled = [&] { return cancelled.load(); };
        sparkpush::KafkaConsumer cancelling;
        Require(cancelling.Init(cluster.brokers, "parallel-cancel", "parallel",
            [&](const std::string&, const std::string&) { cancelled = true; return true; }, options),
            "parallel cancel init");
        cancelling.Start();
        Wait([&] { return cancelled.load(); }, "parallel cancel callback missing");
        cancelling.Stop();
        Require(Committed(cluster, "parallel-cancel") < 0, "cancelled batch committed");
        options.processing_cancelled = {};

        std::atomic<int> completed{0};
        sparkpush::KafkaConsumer commit_failure;
        Require(commit_failure.Init(cluster.brokers, "parallel-commit-fail", "parallel",
            [&](const std::string&, const std::string&) {
                if (++completed == 1) rd_kafka_mock_push_request_errors(cluster.mock, 8, 1,
                    RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED);
                return true;
            }, options), "parallel commit failure init");
        commit_failure.Start();
        Wait([&] { return commit_failure.failed(); }, "batch commit failure did not halt");
        commit_failure.Stop();
        Require(completed == 3 && Committed(cluster, "parallel-commit-fail") < 0,
                "failed grouped commit advanced offsets");
        completed = 0;
        sparkpush::KafkaConsumer commit_replay;
        Require(commit_replay.Init(cluster.brokers, "parallel-commit-fail", "parallel",
            [&](const std::string&, const std::string&) { ++completed; return true; }, options),
            "grouped commit replay init");
        commit_replay.Start();
        Wait([&] { return Committed(cluster, "parallel-commit-fail") == 3; }, "grouped commit replay");
        commit_replay.Stop();
        Require(completed == 3, "failed grouped commit not replayed in full");

        Require(!rd_kafka_mock_topic_create(cluster.mock, "multi-partition", 2, 1), "multi topic");
        SeedPartitions(cluster, "multi-partition");
        options.max_batch_records = 8;
        options.batch_window_ms = 500;
        std::atomic<int> multi_completed{0};
        sparkpush::KafkaConsumer multi;
        Require(multi.Init(cluster.brokers, "parallel-multi", "multi-partition",
            [&](const std::string&, const std::string&) { ++multi_completed; return true; }, options),
            "multi consumer init");
        multi.Start();
        Wait([&] { return Committed(cluster, "parallel-multi", "multi-partition", 0) == 3 &&
                         Committed(cluster, "parallel-multi", "multi-partition", 1) == 3; },
             "partition next-offset aggregation");
        multi.Stop();
        Require(multi_completed == 6 && !multi.failed(), "multi-partition work lost");
        std::cout << "Kafka parallel order, grouped partition commits, cancellation and replay passed\n";
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n'; return 1;
    }
}
