#include "app.h"
#include "config.h"
#include "logging.h"

#include <cstdio>
#include <string>

namespace {

enum class ArgParseResult { kOk, kHelp, kError };

void PrintUsage(const char* prog) {
  std::fprintf(stderr,
               "用法: %s [--config <配置文件>] [config_path]\n"
               "默认配置文件为 conf/comet.conf。\n",
               prog);
}

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

  // 初始化日志到独立文件 logs/comet.log
  sparkpush::InitLogging("comet");

  sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
  sparkpush::RunComet(cfg);

  sparkpush::ShutdownLogging();
  return 0;
}
