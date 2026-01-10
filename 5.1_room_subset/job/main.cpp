#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "config.h"
#include "logging.h"
#include "service.h"

namespace {

// 命令行参数解析结果枚举
enum class ArgParseResult { 
    kOk,      // 解析成功
    kHelp,    // 用户请求帮助信息
    kError    // 解析出错
};

// 打印程序使用说明
void PrintUsage(const char* prog) {
    std::fprintf(stderr,
                 "用法: %s [--config <配置文件>] [config_path]\n"
                 "默认配置文件为 conf/job.conf。\n",
                 prog);
}

// 解析命令行参数，提取配置文件路径
// 支持的参数格式：
//   -c <path> 或 --config <path>：指定配置文件路径
//   -h 或 --help：显示帮助信息
//   <path>：位置参数，直接指定配置文件路径
// 返回值：解析结果（成功、需要帮助、出错）
ArgParseResult ParseConfigPath(int argc, char** argv,
                               std::string* config_path) {
    const std::string default_path = "conf/job.conf";
    *config_path = default_path;  // 设置默认配置文件路径
    bool positional_used = false;  // 标记是否已使用位置参数
    
    // 遍历所有命令行参数
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        
        // 处理 -c 或 --config 选项
        if (arg == "-c" || arg == "--config") {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "%s 需要一个配置文件路径\n", arg.c_str());
                return ArgParseResult::kError;
            }
            *config_path = argv[++i];  // 读取下一个参数作为配置文件路径
        } 
        // 处理 -h 或 --help 选项
        else if (arg == "-h" || arg == "--help") {
            return ArgParseResult::kHelp;
        } 
        // 处理未知选项（以 - 开头）
        else if (!arg.empty() && arg[0] == '-') {
            std::fprintf(stderr, "未知参数: %s\n", arg.c_str());
            return ArgParseResult::kError;
        } 
        // 处理位置参数
        else {
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

// 运行 Job 服务的主函数
// 步骤：
// 1. 创建 JobRunner 实例
// 2. 初始化（解析配置、连接 Kafka）
// 3. 启动服务，进入消息处理循环
// 4. 优雅关闭
int RunJob(const Config& cfg) {
    // 创建 JobRunner 实例
    JobRunner runner(cfg);
    
    // 初始化 JobRunner
    if (!runner.Init()) {
        LOG_ERROR << "JobRunner init failed";
        return 1;
    }
    
    // 启动 JobRunner，开始消费 Kafka 消息
    runner.Start();
    LOG_INFO << "Job runner started. Waiting for Kafka messages...";
    
    // 主线程进入等待状态，保持服务运行
    // 实际的消息处理在 Kafka 消费者线程和 RPC 线程池中进行
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    
    // 停止 JobRunner（实际上这段代码在当前实现中无法到达）
    runner.Stop();
    return 0;
}

}  // namespace sparkpush

// Job 服务主程序入口
// 步骤：
// 1. 解析命令行参数，获取配置文件路径
// 2. 初始化日志系统
// 3. 加载配置文件
// 4. 运行 Job 服务
// 5. 清理资源
int main(int argc, char** argv) {
    // 解析命令行参数
    std::string config_path;
    ArgParseResult result = ParseConfigPath(argc, argv, &config_path);
    
    // 如果用户请求帮助信息，打印使用说明后退出
    if (result == ArgParseResult::kHelp) {
        PrintUsage(argv[0]);
        return 0;
    }
    
    // 如果参数解析出错，打印使用说明后返回错误码
    if (result == ArgParseResult::kError) {
        PrintUsage(argv[0]);
        return 1;
    }

    // 初始化日志系统，日志文件前缀为 "job"
    sparkpush::InitLogging("job");
    
    // 从配置文件加载配置
    sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
    sparkpush::ApplyLoggingConfig("job", cfg);
    
    // 运行 Job 服务
    int ret = sparkpush::RunJob(cfg);
    
    // 关闭日志系统，释放资源
    sparkpush::ShutdownLogging();
    return ret;
}
