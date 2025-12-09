#include "app.h"
#include "config.h"
#include "logging.h"

#include <cstdio>
#include <string>

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

// 打印命令行用法提示，支持配置文件参数说明。
void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/comet.conf。\n",
                 prog);
}

// 解析命令行，支持 --config/-c 或位置参数指定配置路径。
// 返回 kHelp/kError 以便主流程决定是否退出。
ArgParseResult ParseConfigPath(int argc,
                               char** argv,
                               std::string* config_path) {
    const std::string default_path = "conf/comet.conf";
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
            // 位置参数模式仅允许出现一次。
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

// 进程入口：解析配置路径，初始化日志，加载配置并启动 comet。
int main(int argc, char** argv) {
    std::string config_path;
    ArgParseResult result = ParseConfigPath(argc, argv, &config_path);
    if (result == ArgParseResult::kHelp) {
        // 用户主动请求帮助，打印用法后退出。
        PrintUsage(argv[0]);
        return 0;
    }
    if (result == ArgParseResult::kError) {
        PrintUsage(argv[0]);
        return 1;
    }

    // 初始化日志到独立文件 logs/comet.log，便于与其他进程区分。
    sparkpush::InitLogging("comet");

    // 加载配置并启动 comet 主流程（WebSocket + gRPC）。
    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    sparkpush::RunComet(cfg);

    sparkpush::ShutdownLogging();
    return 0;
}
