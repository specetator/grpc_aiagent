#include "logging.h"

#include <cstdio>
#include <cstring>
#include <cerrno>
#include <string>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace {

FILE* g_log_file = nullptr;

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

void FileFlushFunc() {
  if (g_log_file) {
    ::fflush(g_log_file);
  } else {
    ::fflush(stderr);
  }
}

}  // namespace

namespace sparkpush {

bool InitLogging(const std::string& program) {
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
  muduo::Logger::setOutput(FileOutputFunc);
  muduo::Logger::setFlush(FileFlushFunc);

  return true;
}

void ShutdownLogging() {
  if (g_log_file) {
    ::fflush(g_log_file);
    ::fclose(g_log_file);
    g_log_file = nullptr;
  }
}

}  // namespace sparkpush


