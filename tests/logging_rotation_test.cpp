#include <filesystem>
#include <stdexcept>
#include <thread>
#include <vector>
#include <cstdlib>
#include <unistd.h>
#include "logging.h"
int main() {
    char path[]="/tmp/spark-logging-test-XXXXXX";
    if (!mkdtemp(path)) return 1;
    const auto previous=std::filesystem::current_path();
    std::filesystem::current_path(path);
    sparkpush::LoggingOptions options;
    options.max_file_bytes=4096; options.keep_files=2;
    if (!sparkpush::InitLogging("test",options)) return 1;
    std::vector<std::thread> threads;
    for(int i=0;i<4;++i) threads.emplace_back([] { for(int j=0;j<500;++j) LOG_WARN << std::string(64,'x'); });
    for(auto& thread:threads) thread.join();
    sparkpush::ShutdownLogging();
    size_t count=0;
    for (const auto& file:std::filesystem::directory_iterator("logs")) {
        ++count;
        if(file.file_size()>4096) throw std::runtime_error("log rotation size exceeded");
    }
    if(count!=3) throw std::runtime_error("log retention count");
    std::filesystem::current_path(previous);
    std::filesystem::remove_all(path);
}
