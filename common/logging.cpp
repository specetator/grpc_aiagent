#include "logging.h"

#include <cstdio>
#include <cstring>
#include <cerrno>
#include <string>
#include <mutex>
#include <algorithm>
#include <cstdlib>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace {

// 全局文件句柄，用于 muduo 重定向输出
//
// 用全局变量而不是单例类：muduo 的 Logger::setOutput 只接受普通
// 函数指针，没法携带 this。所有访问都由 g_log_mutex 串行化。
FILE* g_log_file = nullptr;
std::mutex g_log_mutex;
std::string g_log_filename;
sparkpush::LoggingOptions g_log_options;
size_t g_log_bytes = 0;
// 只报一次错：日志写失败时如果每条都往 stderr 喷，反而会把真正
// 的错误信息淹没，也可能在磁盘满时形成日志风暴。
bool g_log_error_reported = false;
void LogIoError() {
    if (!g_log_error_reported) {
        g_log_error_reported = true;
        std::fprintf(stderr, "Spark log write/rotation failed; check log storage\n");
    }
}

// 日志轮转：app.log -> app.log.1 -> app.log.2 ... -> app.log.N（最旧的丢弃）。
//
// 必须从最大下标往小倒着 rename：若从 .1 开始，.1 -> .2 会覆盖还没
// 移走的旧 .2，造成日志丢失。
//
// rename 失败且 errno == ENOENT 是**正常**情况（该编号的文件本来
// 就不存在，比如首次轮转时没有 .3），因此不报错。
void RotateLog() {
    if (g_log_file && std::fclose(g_log_file) != 0) LogIoError();
    g_log_file = nullptr;
    for (int index = g_log_options.keep_files; index > 1; --index) {
        const auto source = g_log_filename + "." + std::to_string(index - 1);
        const auto target = g_log_filename + "." + std::to_string(index);
        if (std::rename(source.c_str(), target.c_str()) != 0 && errno != ENOENT) LogIoError();
    }
    if (std::rename(g_log_filename.c_str(), (g_log_filename + ".1").c_str()) != 0 && errno != ENOENT) LogIoError();
    // "ae" = append + close-on-exec：追加模式避开多进程互相截断，
    // CLOEXEC 保证 fork/exec 子进程不会继承并意外持有日志句柄。
    g_log_file = std::fopen(g_log_filename.c_str(), "ae");
    if (!g_log_file) LogIoError();
    g_log_bytes = 0;
}

// muduo 自定义输出函数：写文件，同时保留控制台输出方便调试
//
// 轮转判定那行看着绕，实际是在做**防下溢**的剩余空间计算：
//   len > max_file_bytes - min(g_log_bytes, max_file_bytes)
// 先把 g_log_bytes 钳到 max_file_bytes，保证减法不会下溢（size_t
// 下溢会得到巨大值，反而永远不触发轮转）。语义就是「写入后会超限吗」。
//
// g_log_bytes != 0 的前置条件：空文件（刚轮转完）不立即再轮转，
// 避免单条超长日志造成轮转死循环。
//
// RotateLog 后重新检查 g_log_file：轮转可能失败（磁盘满），
// 此时不能再写空指针。失败时退到 stderr，保证日志不会静默消失。
void FileOutputFunc(const char* msg, int len) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (g_log_options.console) std::fwrite(msg, 1, len, stdout);
    if (g_log_file) {
        if (g_log_bytes && static_cast<size_t>(len) > g_log_options.max_file_bytes - std::min(g_log_bytes, g_log_options.max_file_bytes)) RotateLog();
        if (g_log_file) {
            const auto written = std::fwrite(msg, 1, len, g_log_file);
            g_log_bytes += written;   // 按实际写入量计，而不是 len（部分写入时后者会虚高）
            if (written != static_cast<size_t>(len)) LogIoError();
        }
    } else std::fwrite(msg, 1, len, stderr);
}

// muduo 的刷盘回调。muduo 默认每 3 秒调一次，也可在 Logger 析构时调。
void FileFlushFunc() {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (g_log_file && std::fflush(g_log_file) != 0) LogIoError();
    if (g_log_options.console) std::fflush(stdout);
}

}  // namespace

namespace sparkpush {

// 初始化日志。失败时返回 false，调用方应继续跑（日志会退到 stderr），
// 而不是因此拒绝启动服务——日志不可用不等于业务不可用。
//
// 拒绝重复初始化（g_log_file 已存在）：muduo 的 output 回调是全局的，
// 重复设置会丢掉旧句柄造成泄漏。
bool InitLogging(const std::string& program, const LoggingOptions& options) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    // keep_files 上限 20：轮转靠逐层 rename，文件数越多轮转越慢，
    // 且过大的值通常意味着配置写错了。
    if (g_log_file || options.max_file_bytes == 0 || options.keep_files < 1 || options.keep_files > 20) return false;
    // 创建 logs 目录（如果不存在）
    const char* kLogDir = "logs";
    struct stat st;
    if (::stat(kLogDir, &st) != 0) {
        // 目录不存在时尝试创建
        if (::mkdir(kLogDir, 0755) != 0) {
            std::fprintf(stderr,
                         "InitLogging mkdir %s failed: %s\n",
                         kLogDir,
                         std::strerror(errno));
            return false;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        std::fprintf(stderr,
                     "InitLogging: %s exists but is not directory\n",
                     kLogDir);
        return false;
    }

    // 每个进程一个独立日志文件
    std::string filename = std::string(kLogDir) + "/" + program + ".log";
    FILE* fp = ::fopen(filename.c_str(), "ae");
    if (!fp) {
        std::fprintf(stderr,
                     "InitLogging fopen %s failed: %s\n",
                     filename.c_str(),
                     std::strerror(errno));
        return false;
    }

    g_log_file = fp;
    g_log_filename = filename;
    g_log_options = options;
    // 环境变量可以覆盖配置里的 console 开关：方便在容器/CI 里
    // 临时把日志打到 stdout 而不用改配置文件。
    if (const char* console = std::getenv("SPARK_LOG_CONSOLE")) g_log_options.console = std::string(console) == "1";
    // 以文件**当前实际大小**初始化计数器：日志是 append 模式打开的，
    // 进程重启后文件已有内容。若从 0 开始计，实际文件会长到
    // 2 倍 max_file_bytes 才轮转。
    struct stat info;
    g_log_bytes = ::fstat(::fileno(fp), &info) == 0 ? static_cast<size_t>(info.st_size) : 0;
    g_log_error_reported = false;
    // 接管 muduo 的输出与刷盘。LOG_INFO/LOG_ERROR 这些宏最终都走这里。
    muduo::Logger::setOutput(FileOutputFunc);
    muduo::Logger::setFlush(FileFlushFunc);

    return true;
}

// 可选关闭日志，落盘后清理句柄，适用于进程退出前。
void ShutdownLogging() {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (g_log_file) {
        if (::fflush(g_log_file) != 0) LogIoError();
        if (::fclose(g_log_file) != 0) LogIoError();
        g_log_file = nullptr;
    }
}

}  // namespace sparkpush


