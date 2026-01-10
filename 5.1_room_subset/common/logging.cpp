#include "logging.h"

#include <cstdio>
#include <cstring>
#include <cerrno>
#include <string>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "config.h"

namespace {

// 全局文件句柄，用于 muduo 重定向输出
FILE* g_log_file = nullptr;

// muduo 自定义输出函数：写文件，同时保留控制台输出方便调试
void FileOutputFunc(const char* msg, int len) {
    printf("%.*s", len, msg);
    if (g_log_file) {
        size_t n = ::fwrite(msg, 1, len, g_log_file);
        (void)n;
    } else {
        // 如果文件未初始化，则退回到标准错误输出
        ::fwrite(msg, 1, len, stderr);
    }
}

// muduo 刷新函数，确保文件缓冲落盘
void FileFlushFunc() {
    if (g_log_file) {
        ::fflush(g_log_file);
    } else {
        ::fflush(stderr);
    }
}

static bool EnsureDir(const std::string& dir) {
    if (dir.empty()) return true;
    struct stat st;
    if (::stat(dir.c_str(), &st) == 0) {
        return S_ISDIR(st.st_mode);
    }
    auto pos = dir.find_last_of('/');
    if (pos != std::string::npos) {
        std::string parent = dir.substr(0, pos);
        if (!parent.empty() && !EnsureDir(parent)) return false;
    }
    if (::mkdir(dir.c_str(), 0755) == 0) return true;
    if (errno == EEXIST) {
        if (::stat(dir.c_str(), &st) == 0) return S_ISDIR(st.st_mode);
    }
    return false;
}

static bool EnsureDirForFile(const std::string& filepath) {
    auto pos = filepath.find_last_of('/');
    if (pos == std::string::npos) return true;
    std::string dir = filepath.substr(0, pos);
    return EnsureDir(dir);
}

static void ReplaceAll(std::string* s, const std::string& from,
                       const std::string& to) {
    if (!s || from.empty()) return;
    size_t pos = 0;
    while ((pos = s->find(from, pos)) != std::string::npos) {
        s->replace(pos, from.size(), to);
        pos += to.size();
    }
}

static muduo::Logger::LogLevel ParseLogLevel(const std::string& level) {
    std::string v = level;
    for (auto& ch : v) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    if (v == "TRACE") return muduo::Logger::TRACE;
    if (v == "DEBUG") return muduo::Logger::DEBUG;
    if (v == "INFO") return muduo::Logger::INFO;
    if (v == "WARN" || v == "WARNING") return muduo::Logger::WARN;
    if (v == "ERROR") return muduo::Logger::ERROR;
    if (v == "FATAL") return muduo::Logger::FATAL;
    return muduo::Logger::INFO;
}

static bool ReopenLogFile(const std::string& filename) {
    if (filename.empty()) return false;
    if (!EnsureDirForFile(filename)) {
        std::fprintf(stderr,
                     "InitLogging mkdir for %s failed: %s\n",
                     filename.c_str(),
                     std::strerror(errno));
        return false;
    }
    FILE* fp = ::fopen(filename.c_str(), "ae");
    if (!fp) {
        std::fprintf(stderr,
                     "InitLogging fopen %s failed: %s\n",
                     filename.c_str(),
                     std::strerror(errno));
        return false;
    }
    if (g_log_file) {
        ::fflush(g_log_file);
        ::fclose(g_log_file);
    }
    g_log_file = fp;
    muduo::Logger::setOutput(FileOutputFunc);
    muduo::Logger::setFlush(FileFlushFunc);
    return true;
}

}  // namespace

namespace sparkpush {

bool InitLogging(const std::string& program) {
    // 每个进程一个独立日志文件
    std::string filename = std::string("logs/") + program + ".log";
    return ReopenLogFile(filename);
}

void ApplyLoggingConfig(const std::string& program, const Config& cfg) {
    // 1) level
    muduo::Logger::setLogLevel(ParseLogLevel(cfg.log_level));

    // 2) file (optional)
    if (cfg.log_file.empty()) return;

    std::string filename = cfg.log_file;
    ReplaceAll(&filename, "${program}", program);
    ReplaceAll(&filename, "${pid}", std::to_string(::getpid()));
    ReplaceAll(&filename, "${comet_id}", cfg.comet_id);
    ReplaceAll(&filename, "${listen_addr}", cfg.listen_addr);
    ReplaceAll(&filename, "${listen_port}", std::to_string(cfg.listen_port));
    ReplaceAll(&filename, "${http_port}", std::to_string(cfg.http_port));
    ReplaceAll(&filename, "${comet_grpc_port}", std::to_string(cfg.comet_grpc_port));
    ReplaceAll(&filename, "${kafka_consumer_group}", cfg.kafka_consumer_group);

    (void)ReopenLogFile(filename);
}

// 可选关闭日志，落盘后清理句柄，适用于进程退出前。
void ShutdownLogging() {
    if (g_log_file) {
        ::fflush(g_log_file);
        ::fclose(g_log_file);
        g_log_file = nullptr;
    }
}

}  // namespace sparkpush


