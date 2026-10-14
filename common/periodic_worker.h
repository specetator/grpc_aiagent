#pragma once
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include "logging.h"
namespace sparkpush {

// 单线程周期任务器。用于 outbox 重发扫描、流超时看门狗、指标刷新等
// 「必须周期跑、但失败不能拖垮主流程」的后台作业。
//
// 与 std::async / 自建循环相比的关键差异：
//   1) 用 cv_.wait_for 而不是 sleep_for，因此 Stop() 能立即唤醒，
//      不必等完整个间隔（间隔 1s 的定时器最多多等 1s 才能退出）；
//   2) 任务抛异常被捕获并记日志，不会终止线程；
//   3) 构造即启动，析构自动 Stop，符合 RAII，不会漏关线程。
//
// 注意：任务在**独立线程**上跑，且上一次执行完才开始计时。
// 因此任务耗时超过间隔时会自然降频（不会重叠执行），
// 但任务里访问共享数据必须自己加锁。
class PeriodicWorker {
 public:
    PeriodicWorker(std::function<void()> task, std::chrono::milliseconds interval)
        : thread_([this, task = std::move(task), interval] {
            std::unique_lock<std::mutex> lock(mutex_);
            while (!stopping_) {
                // 执行任务前必须释锁：否则 Stop() 拿不到 mutex_，
                // 会一直阻塞到任务跑完，长任务下停机就卡住了。
                lock.unlock();
                try { task(); } catch (const std::exception&) { LOG_ERROR << "periodic worker failed"; }
                lock.lock();
                // 谓词式 wait_for：被唤醒后再确认是不是因为 stopping_，
                // 避免虚假唤醒导致提前跑一轮任务。
                cv_.wait_for(lock, interval, [this] { return stopping_; });
            }
        }) {}
    ~PeriodicWorker() { Stop(); }
    // 幂等：可重复调用，也可在析构前显式调用以控制关闭时机。
    // join 前必须 notify，否则线程正在 wait_for 里睡着，停机要等整个间隔。
    void Stop() {
        { std::lock_guard<std::mutex> lock(mutex_); stopping_ = true; }
        cv_.notify_all();
        if (thread_.joinable()) thread_.join();
    }
 private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_{false};
    std::thread thread_;   // 声明在最后：保证它先于 mutex_/cv_ 被销毁
};
}  // namespace sparkpush
