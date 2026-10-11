#pragma once
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include "logging.h"
namespace sparkpush {
class PeriodicWorker {
 public:
    PeriodicWorker(std::function<void()> task, std::chrono::milliseconds interval)
        : thread_([this, task = std::move(task), interval] {
            std::unique_lock<std::mutex> lock(mutex_);
            while (!stopping_) {
                lock.unlock();
                try { task(); } catch (const std::exception&) { LOG_ERROR << "periodic worker failed"; }
                lock.lock();
                cv_.wait_for(lock, interval, [this] { return stopping_; });
            }
        }) {}
    ~PeriodicWorker() { Stop(); }
    void Stop() {
        { std::lock_guard<std::mutex> lock(mutex_); stopping_ = true; }
        cv_.notify_all();
        if (thread_.joinable()) thread_.join();
    }
 private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_{false};
    std::thread thread_;
};
}  // namespace sparkpush
