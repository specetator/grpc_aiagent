#include "kafka_consumer.h"
#include "kafka_producer.h"
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>
#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <sys/socket.h>
#include <unistd.h>

namespace {
void Require(bool ok, const char* reason) {
    if (!ok) throw std::runtime_error(reason);
}
template <class F> void Wait(F condition, const char* reason) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(12);
    while (!condition()) {
        Require(std::chrono::steady_clock::now() < deadline, reason);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}
struct Cluster {
    rd_kafka_t* owner;
    rd_kafka_mock_cluster_t* mock;
    std::string brokers;
    Cluster() {
        const int probe = socket(AF_INET, SOCK_STREAM, 0);
        Require(probe >= 0, "test requires permission to create a temporary local mock broker socket");
        close(probe);
        char error[512];
        auto* config = rd_kafka_conf_new();
        rd_kafka_conf_set(config, "log_level", "0", nullptr, 0);
        owner = rd_kafka_new(RD_KAFKA_PRODUCER, config, error, sizeof(error));
        Require(owner != nullptr, "mock owner create failed");
        mock = rd_kafka_mock_cluster_new(owner, 1);
        Require(mock != nullptr, "mock broker create failed");
        brokers = rd_kafka_mock_cluster_bootstraps(mock);
    }
    ~Cluster() {
        rd_kafka_mock_cluster_destroy(mock);
        rd_kafka_destroy(owner);
    }
    void Topic(const std::string& name) {
        Require(rd_kafka_mock_topic_create(mock, name.c_str(), 1, 1) == 0, "topic create failed");
    }
};
std::unique_ptr<RdKafka::KafkaConsumer> Observer(const Cluster& cluster, const std::string& group) {
    std::unique_ptr<RdKafka::Conf> config(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    std::string error;
    config->set("bootstrap.servers", cluster.brokers, error);
    config->set("group.id", group, error);
    config->set("enable.auto.commit", "false", error);
    auto result = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(config.get(), error));
    Require(result != nullptr, "observer create failed");
    return result;
}
int64_t Committed(const Cluster& cluster, const std::string& group, const std::string& topic) {
    auto observer = Observer(cluster, group);
    std::vector<RdKafka::TopicPartition*> partitions{RdKafka::TopicPartition::create(topic, 0)};
    Require(observer->committed(partitions, 3000) == RdKafka::ERR_NO_ERROR, "offset query failed");
    const auto offset = partitions[0]->offset();
    RdKafka::TopicPartition::destroy(partitions);
    observer->close();
    return offset;
}
std::string ReadFirst(const Cluster& cluster, const std::string& topic) {
    auto reader = Observer(cluster, "dlq-reader");
    std::vector<RdKafka::TopicPartition*> partitions{RdKafka::TopicPartition::create(topic, 0, 0)};
    Require(reader->assign(partitions) == RdKafka::ERR_NO_ERROR, "DLQ assign failed");
    RdKafka::TopicPartition::destroy(partitions);
    std::unique_ptr<RdKafka::Message> record(reader->consume(5000));
    Require(record && record->err() == RdKafka::ERR_NO_ERROR, "durable DLQ record missing");
    std::string value(static_cast<const char*>(record->payload()), record->len());
    reader->close();
    return value;
}
void Seed(const Cluster& cluster, const std::string& topic) {
    sparkpush::KafkaProducer producer;
    Require(producer.Init(cluster.brokers, topic), "source producer init failed");
    Require(producer.SendAndWait("key", std::string("\xff\0", 2), 3000), "poison produce failed");
    Require(producer.SendAndWait("next", "healthy", 3000), "second produce failed");
}
}

int main() {
    try {
        Cluster cluster;
        cluster.Topic("reply"); cluster.Topic("reply.dlq");
        Seed(cluster, "reply");
        sparkpush::KafkaConsumer::Options options;
        options.session_timeout_ms = 6000;
        options.dead_letter_topic = "reply.dlq";
        options.max_processing_attempts = 2;
        options.processing_retry_backoff_ms = 1;
        options.failure_recovery = [](const std::string&, const std::string&) {
            return sparkpush::KafkaConsumer::RecoveryRecord{"ai_reply", "id", "generated answer"};
        };
        std::atomic<int> poison{0}, healthy{0};
        sparkpush::KafkaConsumer consumer;
        Require(consumer.Init(cluster.brokers, "success-group", "reply",
            [&](const std::string& key, const std::string&) {
                if (key == "next") { ++healthy; return true; }
                ++poison;
                throw std::runtime_error("private-data-must-not-be-logged");
            }, options), "consumer init failed");
        consumer.Start();
        Wait([&] { return healthy.load() == 1; }, "failed record blocked after DLQ success");
        consumer.Stop();
        Require(!consumer.failed() && poison == 2, "callback exception retry count incorrect");
        const auto dead = nlohmann::json::parse(ReadFirst(cluster, "reply.dlq"));
        Require(dead["payload_hex"] == "ff00" && dead["key_hex"] == "6b6579" &&
                dead["topic"] == "reply" && dead["offset"] == 0 && dead["attempts"] == 2,
                "DLQ lost original bytes or source identity");
        Require(dead["recovery"]["topic"] == "ai_reply" &&
                dead["recovery"]["payload_hex"] == "67656e65726174656420616e73776572",
                "generated answer recovery lost");
        Require(Committed(cluster, "success-group", "reply") == 2, "successful DLQ not committed");

        cluster.Topic("blocked"); cluster.Topic("blocked.dlq");
        Seed(cluster, "blocked");
        // Reject the next Produce request (source seeding is already complete).
        rd_kafka_mock_push_request_errors(cluster.mock, 0, 1, RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE);
        options.dead_letter_topic = "blocked.dlq";
        options.dead_letter_timeout_ms = 300;
        poison = 0; healthy = 0;
        sparkpush::KafkaConsumer blocked;
        Require(blocked.Init(cluster.brokers, "blocked-group", "blocked",
            [&](const std::string& key, const std::string&) {
                if (key == "next") ++healthy; else ++poison;
                return false;
            }, options), "blocked consumer init failed");
        blocked.Start();
        Wait([&] { return blocked.failed(); }, "DLQ failure did not halt consumer");
        blocked.Stop(); // also tests failed-thread join/destructor safety
        Require(healthy == 0 && poison == 2, "later offset skipped failed record");
        Require(Committed(cluster, "blocked-group", "blocked") < 0, "failed DLQ still committed source");

        // Replacement with the same group must see BOTH original records.
        options.dead_letter_topic.clear();
        std::atomic<int> replayed{0};
        sparkpush::KafkaConsumer replacement;
        Require(replacement.Init(cluster.brokers, "blocked-group", "blocked",
            [&](const std::string&, const std::string&) { ++replayed; return true; }, options), "replacement init failed");
        replacement.Start();
        Wait([&] { return replayed.load() == 2; }, "uncommitted record not replayed on restart");
        replacement.Stop();

        cluster.Topic("commit-failure"); cluster.Topic("commit-failure.dlq");
        Seed(cluster, "commit-failure");
        options.dead_letter_topic = "commit-failure.dlq";
        std::atomic<int> processed{0};
        sparkpush::KafkaConsumer commit_failure;
        Require(commit_failure.Init(cluster.brokers, "commit-group", "commit-failure",
            [&](const std::string&, const std::string&) {
                ++processed;
                // OffsetCommit (8): business succeeds but source commit fails.
                rd_kafka_mock_push_request_errors(cluster.mock, 8, 1,
                    RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED);
                return true;
            }, options), "commit failure consumer init failed");
        commit_failure.Start();
        Wait([&] { return commit_failure.failed(); }, "unconfirmed commit did not halt");
        commit_failure.Stop();
        Require(processed == 1 && Committed(cluster, "commit-group", "commit-failure") < 0,
                "later commit skipped unconfirmed source offset");
        std::cout << "Kafka durable failure integration tests passed\n";
        return 0;
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
}
