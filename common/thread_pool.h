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
    explicit ThreadPool(size_t thread_num = 0, const std::string& name = "", size_t max_tasks = 1024,
                        size_t max_bytes = 32 * 1024 * 1024);
    // 析构时自动停止线程。
    ~ThreadPool();

    // 启动所有工作线程。
    void Start();
    // 停止线程池并回收资源。
    void Stop(bool drain = true);

    // 提交任务；线程池已停止或任务为空时返回 false。
    bool Submit(std::function<void()> task, size_t bytes = 1);

    size_t size() const { return thread_num_; }

private:
    void WorkerLoop(size_t worker_id);

    size_t thread_num_;
    std::string name_;
    std::vector<std::thread> workers_;
    struct Task { std::function<void()> function; size_t bytes; };
    std::queue<Task> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_{false};
    bool started_{false};
    size_t max_tasks_, max_bytes_, queued_bytes_{0};
};

}  // namespace sparkpush

