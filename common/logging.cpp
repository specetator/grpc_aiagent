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
FILE* g_log_file = nullptr;
std::mutex g_log_mutex;
std::string g_log_filename;
sparkpush::LoggingOptions g_log_options;
size_t g_log_bytes = 0;
bool g_log_error_reported = false;
void LogIoError() {
    if (!g_log_error_reported) {
        g_log_error_reported = true;
        std::fprintf(stderr, "Spark log write/rotation failed; check log storage\n");
    }
}
void RotateLog() {
    if (g_log_file && std::fclose(g_log_file) != 0) LogIoError();
    g_log_file = nullptr;
    for (int index = g_log_options.keep_files; index > 1; --index) {
        const auto source = g_log_filename + "." + std::to_string(index - 1);
        const auto target = g_log_filename + "." + std::to_string(index);
        if (std::rename(source.c_str(), target.c_str()) != 0 && errno != ENOENT) LogIoError();
    }
    if (std::rename(g_log_filename.c_str(), (g_log_filename + ".1").c_str()) != 0 && errno != ENOENT) LogIoError();
    g_log_file = std::fopen(g_log_filename.c_str(), "ae");
    if (!g_log_file) LogIoError();
    g_log_bytes = 0;
}

// muduo 自定义输出函数：写文件，同时保留控制台输出方便调试
void FileOutputFunc(const char* msg, int len) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (g_log_options.console) std::fwrite(msg, 1, len, stdout);
    if (g_log_file) {
        if (g_log_bytes && static_cast<size_t>(len) > g_log_options.max_file_bytes - std::min(g_log_bytes, g_log_options.max_file_bytes)) RotateLog();
        if (g_log_file) {
            const auto written = std::fwrite(msg, 1, len, g_log_file);
            g_log_bytes += written;
            if (written != static_cast<size_t>(len)) LogIoError();
        }
    } else std::fwrite(msg, 1, len, stderr);
}
void FileFlushFunc() {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    if (g_log_file && std::fflush(g_log_file) != 0) LogIoError();
    if (g_log_options.console) std::fflush(stdout);
}

}  // namespace

namespace sparkpush {

bool InitLogging(const std::string& program, const LoggingOptions& options) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
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
    if (const char* console = std::getenv("SPARK_LOG_CONSOLE")) g_log_options.console = std::string(console) == "1";
    struct stat info;
    g_log_bytes = ::fstat(::fileno(fp), &info) == 0 ? static_cast<size_t>(info.st_size) : 0;
    g_log_error_reported = false;
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


