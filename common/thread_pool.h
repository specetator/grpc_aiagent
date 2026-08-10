#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace sparkpush {

// 轻量线程池，支持提交普通函数任务并安全停止
class ThreadPool {
public:
    // 创建线程池，支持自定义线程数与名称。
    explicit ThreadPool(size_t thread_num = 0, const std::string& name = "");
    // 析构时自动停止线程。
    ~ThreadPool();

    // 启动所有工作线程。
    void Start();
    // 停止线程池并回收资源。
    void Stop();

    // 提交任务；线程池已停止或任务为空时返回 false。
    bool Submit(std::function<void()> task);

    size_t size() const { return thread_num_; }

private:
    void WorkerLoop(size_t worker_id);

    size_t thread_num_;
    std::string name_;
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_{false};
};

}  // namespace sparkpush

