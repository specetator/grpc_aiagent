// ============================================================================
// comet 服务主程序
// 
// 功能：
// - 启动 WebSocket 服务器，接受客户端长连接
// - 在握手时向 logic 验证 token，完成鉴权
// - 维护用户连接映射，后续可用于消息推送
// 
// 依赖：
// - logic 服务：提供 gRPC 接口进行 token 验证
// - muduo 网络库：提供事件驱动的 TCP 服务器
// ============================================================================
#include "app.h"
#include "config.h"
#include "logging.h"

#include <cstdio>
#include <string>

namespace {

// 命令行参数解析结果
enum class ArgParseResult { kOk, kHelp, kError };

// 打印命令行用法说明
void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/comet.conf。\n",
                 prog);
}

// 解析命令行参数，提取配置文件路径
// 支持两种方式：选项方式（--config/-c）或位置参数
// @return: 解析结果（成功/帮助/错误）
ArgParseResult ParseConfigPath(int argc,
                               char** argv,
                               std::string* config_path) {
    const std::string default_path = "conf/comet.conf";
    *config_path = default_path;
    bool positional_used = false;
    
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-c" || arg == "--config") {
            // 选项方式：后面必须跟配置文件路径
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s 需要一个配置文件路径\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = argv[++i];
        } else if (arg == "-h" || arg == "--help") {
            return ArgParseResult::kHelp;
        } else if (!arg.empty() && arg[0] == '-') {
            // 未知选项
            std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
            return ArgParseResult::kError;
        } else {
            // 位置参数方式（只允许出现一次）
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

// ============================================================================
// 主函数：程序入口
// ============================================================================
int main(int argc, char** argv) {
    // 1. 解析命令行参数，获取配置文件路径
    std::string config_path;
    ArgParseResult result = ParseConfigPath(argc, argv, &config_path);
    
    if (result == ArgParseResult::kHelp) {
        // 用户请求帮助信息
        PrintUsage(argv[0]);
        return 0;
    }
    if (result == ArgParseResult::kError) {
        // 参数解析错误
        PrintUsage(argv[0]);
        return 1;
    }

    // 2. 初始化日志系统（输出到 logs/comet.log，与 logic 日志分离）
    sparkpush::InitLogging("comet");

    // 3. 加载配置文件
    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    sparkpush::ApplyLoggingConfig("comet", cfg);
    // 4. 运行 comet 服务主逻辑（阻塞，直到服务退出）
    sparkpush::RunComet(cfg);

    // 5. 关闭日志系统
    sparkpush::ShutdownLogging();
    
    return 0;
}
