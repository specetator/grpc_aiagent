// function_timer.h
#pragma once
#include <chrono>
#include <cstring>  // C++ 风格（推荐在 C++ 中使用）
#include <iostream>
#include <string>
class FunctionTimer {
   public:
    // 构造函数：记录开始时间 + 位置信息
    FunctionTimer(const char* file, int line, const char* func)
        : m_file(file),
          m_line(line),
          m_func(func),
          m_start(std::chrono::high_resolution_clock::now()) {}

    static const char* basename(const char* path) {
        const char* p = strrchr(path, '/');
        if (!p) p = strrchr(path, '\\');
        return p ? p + 1 : path;
    }

    // 从开始到当前的总耗时（毫秒）
    void elapsed_ms(const char* file, int line, const char* func) const {
        auto now = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - m_start);
        std::cerr << "[PERF] " << basename(file) << ":" << line << " in "
                  << func << "() took " << duration.count() / 1e6 << " ms\n";
    }

    // 析构函数：自动计算并打印耗时
    ~FunctionTimer() {
        auto end = std::chrono::high_resolution_clock::now();
        auto duration =
            std::chrono::duration_cast<std::chrono::nanoseconds>(end - m_start);
        double ms = duration.count() / 1e6;  // 转为毫秒

        std::cerr << "[PERF] " << basename(m_file) << ":" << m_line << " in "
                  << m_func << "() took " << ms << " ms\n";
    }

   private:
    const char* m_file;
    int m_line;
    const char* m_func;
    std::chrono::high_resolution_clock::time_point m_start;
};

#define FUNCTION_TIMER() FunctionTimer _timer(__FILE__, __LINE__, __FUNCTION__)
#define FUNCTION_TIMER_ELAPSED_MS() \
    (_timer.elapsed_ms(__FILE__, __LINE__, __FUNCTION__))