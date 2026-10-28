#include "../hermes_bridge/config.h"
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <stdexcept>

bool Load(const std::string& extra, sparkpush::HermesBridgeConfig* config) {
    char path[] = "/tmp/spark-native-config-XXXXXX";
    const int fd = mkstemp(path);
    if (fd < 0) throw std::runtime_error("temp config");
    close(fd);
    {
        std::ofstream file(path);
        file << "hermes_base_url=http://127.0.0.1:8643/v1\nhermes_api_key=fixture-only\n" << extra;
    }
    std::string error;
    const bool ok = sparkpush::LoadHermesBridgeConfig(path, config, &error);
    std::remove(path);
    return ok;
}
int main() {
    // This test is hermetic; remove only this child process's optional overrides.
    for (const auto* key : {"SPARK_PUSH_NATIVE_CONTROL_BOTS", "SPARK_PUSH_BRIDGE_WORKERS",
                           "SPARK_PUSH_BRIDGE_MAX_BATCH_RECORDS"}) unsetenv(key);
    sparkpush::HermesBridgeConfig legacy;
    if (!Load("", &legacy) || !legacy.native_control_bot_users.empty()) return 1;
    sparkpush::HermesBridgeConfig native;
    if (!Load("native_control_bot_users=900000000101,900000000211\n", &native) ||
        native.native_control_bot_users.size() != 2) return 2;
    for (const auto* ids : {"900000000001", "900000000201", "1,", "0", "-1", "abc", "1,2x"}) {
        sparkpush::HermesBridgeConfig invalid;
        if (Load(std::string("native_control_bot_users=") + ids + "\n", &invalid)) return 3;
    }
    for (const auto* settings : {"processing_workers=2\n", "max_batch_records=1\n"}) {
        sparkpush::HermesBridgeConfig invalid;
        if (Load(std::string("native_control_bot_users=900000000101\n") + settings, &invalid)) return 4;
    }
    std::cout << "Native control defaults, identity allowlist and bounds passed\n";
}
