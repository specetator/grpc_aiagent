#include "config.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

int main() {
    char path[] = "/tmp/spark_push_config_test_XXXXXX";
    int fd = mkstemp(path);
    if (fd < 0) return 1;
    close(fd);

    setenv("SPARK_PUSH_TEST_SECRET", "placeholder-value", 1);
    setenv("SPARK_PUSH_MYSQL_PASSWORD", "override-value", 1);
    {
        std::ofstream out(path);
        out << "listen_port=9123\n"
            << "redis_password=${SPARK_PUSH_TEST_SECRET}\n"
            << "mysql_password=file-value\n"
            << "kafka_single_topic=test_single\n"
            << "kafka_group_topic=test_group\n"
            << "single_rate_per_sec=17\n"
            << "single_burst=4\n";
    }

    sparkpush::Config cfg = sparkpush::LoadConfig(path);
    unlink(path);
    unsetenv("SPARK_PUSH_TEST_SECRET");
    unsetenv("SPARK_PUSH_MYSQL_PASSWORD");

    if (cfg.listen_port != 9123 ||
        cfg.redis_password != "placeholder-value" ||
        cfg.mysql_password != "override-value" ||
        cfg.kafka_single_topic != "test_single" ||
        cfg.kafka_group_topic != "test_group" ||
        cfg.rate_limit.single_rate_per_sec != 17.0 ||
        cfg.rate_limit.single_burst != 4.0) {
        std::cerr << "environment expansion/override failed\n";
        return 1;
    }
    return 0;
}
