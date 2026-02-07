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

// 固定大小线程池
// 用途：gRPC 异步调用、MySQL 持久化、消息推送等需要并发执行的场景
// 实现：生产者-消费者模型，Submit() 入队，WorkerLoop() 出队执行
// 线程安全：mutex + condition_variable 保护任务队列
class ThreadPool {
 public:
  // thread_num=0 时不创建工作线程（延迟到 Start() 前可修改）
  explicit ThreadPool(size_t thread_num = 0, const std::string& name = "");
  ~ThreadPool();

  void Start();   // 启动所有工作线程
  void Stop();    // 等待队列清空后停止

  // 提交任务到队列（线程安全，可从任意线程调用）
  void Submit(std::function<void()> task);

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


