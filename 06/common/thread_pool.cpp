#include "thread_pool.h"

#include "logging.h"

namespace sparkpush {

ThreadPool::ThreadPool(size_t thread_num, const std::string& name)
    : thread_num_(thread_num ? thread_num : std::thread::hardware_concurrency()),
      name_(name) {}

ThreadPool::~ThreadPool() {
  Stop();
}

void ThreadPool::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!workers_.empty()) {
    return;
  }
  stopping_ = false;
  workers_.reserve(thread_num_);
  for (size_t i = 0; i < thread_num_; ++i) {
    workers_.emplace_back([this, i]() { WorkerLoop(i); });
  }
  LogInfo("ThreadPool " + name_ + " started with " + std::to_string(thread_num_) +
          " threads");
}

void ThreadPool::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    stopping_ = true;
  }
  cv_.notify_all();
  for (auto& t : workers_) {
    if (t.joinable()) {
      t.join();
    }
  }
  workers_.clear();
  std::queue<std::function<void()>> empty;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    std::swap(tasks_, empty);
  }
}

void ThreadPool::Submit(std::function<void()> task) {
  if (!task) return;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_) return;
    tasks_.push(std::move(task));
  }
  cv_.notify_one();
}

void ThreadPool::WorkerLoop(size_t worker_id) {
  while (true) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this]() { return stopping_ || !tasks_.empty(); });
      if (stopping_ && tasks_.empty()) {
        return;
      }
      task = std::move(tasks_.front());
      tasks_.pop();
    }
    try {
      task();
    } catch (const std::exception& ex) {
      LogError("ThreadPool " + name_ + " worker " + std::to_string(worker_id) +
               " caught exception: " + ex.what());
    } catch (...) {
      LogError("ThreadPool " + name_ + " worker " + std::to_string(worker_id) +
               " caught unknown exception");
    }
  }
}

}  // namespace sparkpush


