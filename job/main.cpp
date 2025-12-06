#include "config.h"
#include "logging.h"

#include "service.h"

#include <cstdio>
#include <chrono>
#include <string>
#include <thread>

namespace {

    enum class ArgParseResult { kOk, kHelp, kError };

    // 打印命令行使用帮助，提示配置文件参数。
    void PrintUsage(const char* prog) {
        std::fprintf(stderr,
                     "用法: %s [--config <配置文件>] [config_path]\n"
                     "默认配置文件为 conf/job.conf。\n",
                     prog);
    }

    // 解析命令行参数，优先处理 -c/--config，其次处理位置参数。
    ArgParseResult ParseConfigPath(int argc,
                                   char** argv,
                                   std::string* config_path) {
        const std::string default_path = "conf/job.conf";
        *config_path = default_path;
        bool positional_used = false;
        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];
            if (arg == "-c" || arg == "--config") {
                if (i + 1 >= argc) {
                    std::fprintf(stderr, "%s 需要一个配置文件路径\n", arg.c_str());
                    return ArgParseResult::kError;
                }
                *config_path = argv[++i];
            } else if (arg == "-h" || arg == "--help") {
                return ArgParseResult::kHelp;
            } else if (!arg.empty() && arg[0] == '-') {
                std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
                return ArgParseResult::kError;
            } else {
                if (positional_used) {
                    std::fprintf(stderr, "重复的配置文件参数: %s\n", arg.c_str());
                    return ArgParseResult::kError;
                }
                *config_path = arg;
                positional_used = true;
            }
        }
        return ArgParseResult::kOk;
    }

}  // namespace

namespace sparkpush {

    // 创建并运行 JobRunner，启动后保持主线程存活。
    int RunJob(const Config& cfg) {
        JobRunner runner(cfg);
        if (!runner.Init()) {
            LogError("JobRunner init failed");
            return 1;
        }
        runner.Start();
        LogInfo("Job runner started. Waiting for Kafka messages...");

        // 简单阻塞主线程：运行过程中无需退出，保持进程常驻。
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
        runner.Stop();  // 理论上不会执行到这里，保留以防未来调整
        return 0;
    }

}  // namespace sparkpush

// 程序入口：解析参数、初始化日志与配置，并启动 Job 逻辑。
int main(int argc, char** argv) {
    std::string config_path;
    ArgParseResult result = ParseConfigPath(argc, argv, &config_path);
    if (result == ArgParseResult::kHelp) {
        PrintUsage(argv[0]);
        return 0;
    }
    if (result == ArgParseResult::kError) {
        PrintUsage(argv[0]);
        return 1;
    }

    // 初始化日志到独立文件 logs/job.log。
    sparkpush::InitLogging("job");

    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    int ret = sparkpush::RunJob(cfg);

    sparkpush::ShutdownLogging();
    return ret;
}





