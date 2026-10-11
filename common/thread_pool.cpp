#include "thread_pool.h"
#include <algorithm>

#include "logging.h"

namespace sparkpush {

// 构造线程池，若未指定线程数则回退为硬件并发数。
ThreadPool::ThreadPool(size_t thread_num, const std::string& name, size_t max_tasks, size_t max_bytes)
    : thread_num_(std::max<size_t>(1, thread_num ? thread_num : std::thread::hardware_concurrency())),
      name_(name), max_tasks_(max_tasks), max_bytes_(max_bytes) {}

// 析构时确保线程停止，防止悬挂任务。
ThreadPool::~ThreadPool() {
    Stop();
}

// 启动工作线程，避免重复启动。
void ThreadPool::Start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!workers_.empty()) {
        return;
    }
    stopping_ = false;
    started_ = true;
    workers_.reserve(thread_num_);
    for (size_t i = 0; i < thread_num_; ++i) {
        workers_.emplace_back([this, i]() { WorkerLoop(i); });
    }
    LOG_INFO << "ThreadPool " << name_ << " started with "
             << std::to_string(thread_num_) << " threads";
}

// 停止线程池，等待所有线程退出并清空任务队列。
void ThreadPool::Stop(bool drain) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
        started_ = false;
        if (!drain) {
            std::queue<Task> empty;
            tasks_.swap(empty);
            queued_bytes_ = 0;
        }
    }
    cv_.notify_all();
    for (auto& t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
    workers_.clear();
    std::queue<Task> empty;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::swap(tasks_, empty);
        queued_bytes_ = 0;
    }
}

// 提交一个任务到队列，如果池已停止则忽略。
bool ThreadPool::Submit(std::function<void()> task, size_t bytes) {
    if (!task) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_ || !started_ || tasks_.size() >= max_tasks_ ||
            bytes > max_bytes_ - queued_bytes_) {
            return false;
        }
        tasks_.push({std::move(task), bytes});
        queued_bytes_ += bytes;
    }
    cv_.notify_one();
    return true;
}

// 工作线程循环，从队列取任务并捕获异常防止线程崩溃。
void ThreadPool::WorkerLoop(size_t worker_id) {
    while (true) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
            if (stopping_ && tasks_.empty()) {
                return;
            }
            queued_bytes_ -= tasks_.front().bytes;
            task = std::move(tasks_.front().function);
            tasks_.pop();
        }
        try {
            task();
        } catch (const std::exception& ex) {
            LOG_ERROR << "ThreadPool " << name_ << " worker "
                      << std::to_string(worker_id)
                      << " caught exception: " << ex.what();
        } catch (...) {
            LOG_ERROR << "ThreadPool " << name_ << " worker "
                      << std::to_string(worker_id)
                      << " caught unknown exception";
        }
    }
}

}  // namespace sparkpush

