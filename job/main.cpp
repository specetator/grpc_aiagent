#include "config.h"
#include "logging.h"
#include "metrics.h"

#include "service.h"

#include <cstdio>
#include <chrono>
#include <csignal>
#include <string>
#include <thread>

namespace {

    // Job 没有 muduo EventLoop，因此自建一个信号标志而不是复用
    // common/stop_signal.h：后者需要调用方有轮询循环来配合，
    // 而 Job 的主循环就在本文件里，直接用自己的标志更直接。
    volatile std::sig_atomic_t g_stop_requested = 0;

    // 只置位，不做清理：Kafka consumer 的 close、位点提交都可能阻塞，
    // 在信号上下文里调这些是未定义行为。收尾在 RunJob 的主循环里做。
    void HandleStopSignal(int) {
        g_stop_requested = 1;
    }

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
    //
    // Job 与另两个服务的结构差异：它没有 EventLoop，也不监听业务端口，
    // 本质是四个 Kafka consumer + 若干到 Comet 的双向流。因此主线程
    // 只需要一个轮询循环盯着停机信号与健康状态。
    int RunJob(const Config& cfg) {
        JobRunner runner(cfg);
        // Init 失败直接退出：它负责建 Kafka consumer、MySQL 池与 Comet 长连接，
        // 任一缺失都无法履行「消费并落库」的职责，跑起来也没意义。
        if (!runner.Init()) {
            LOG_ERROR << "JobRunner init failed";
            return 1;
        }
        runner.Start();
        LOG_INFO << "Job runner started. Waiting for Kafka messages...";

        // 响应 SIGINT/SIGTERM，确保 Kafka consumer close 后主动离组。
        //
        // 退出条件有两个，语义完全不同：
        //   g_stop_requested  运维主动停机 -> 返回 0
        //   runner.Failed()   某个 consumer 进入不可恢复状态（比如 DLQ
        //                     写入未确认）-> 返回 1，让容器编排重启它
        // 区分两者很重要：后者必须让进程真的退出，否则一个已停机
        // 的 consumer 会一直占着分区，消息既不被处理也不被重放。
        //
        // 200ms 轮询：停机延迟上限。比这个更短没意义（Kafka close
        // 本身就要花上百毫秒），更长则拖慢编排。
        MetricsRegistry::Instance().Set("spark_push_service_ready", 1);
        while (!g_stop_requested && !runner.Failed()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        const bool failed = runner.Failed();
        // 先把就绪标志置 0 再 Stop：让健康检查立即发现异常，
        // 而不是在长达几秒的关闭过程中还报「健康」。
        MetricsRegistry::Instance().Set("spark_push_service_ready", 0);
        runner.Stop();
        LOG_INFO << "Job runner stopped gracefully";
        return failed ? 1 : 0;
    }

}  // namespace sparkpush

// 程序入口：解析参数、初始化日志与配置，并启动 Job 逻辑。
//
// 信号注册放在最前面（早于参数解析）：即使卡在配置加载阶段，
// Ctrl-C 也能立即生效而不是等默认行为杀进程。
int main(int argc, char** argv) {
    std::signal(SIGINT, HandleStopSignal);
    std::signal(SIGTERM, HandleStopSignal);

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

    int ret = 1;
    // 用 try 包住整个启动过程：Init 里的连接池/gRPC 初始化可能抛异常，
    // 未捕获会直接 terminate，连一行错误日志都看不到。
    try {
        sparkpush::Config cfg = sparkpush::LoadConfig(config_path);
        ret = sparkpush::RunJob(cfg);
    } catch (const std::exception& error) {
        LOG_ERROR << "Job startup/runtime failure: " << error.what();
    }

    // 刷盘并关日志句柄。必须在 return 前调：否则最后几行错误日志
    // 可能还在 stdio 缓冲里没落盘，排障时看不到关键信息。
    sparkpush::ShutdownLogging();
    return ret;
}




