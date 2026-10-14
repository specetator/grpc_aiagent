#include "thread_pool.h"
#include <algorithm>

#include "logging.h"

namespace sparkpush {

// 构造线程池，若未指定线程数则回退为硬件并发数。
//
// max(size_t(1), ...) 的兜底很关键：hardware_concurrency() 在某些
// 容器/WSL 环境下会返回 0，不兜底就会创建一个零线程的池，
// 所有 Submit 都能入队但永远没人执行——表现为消息静默丢失。
ThreadPool::ThreadPool(size_t thread_num, const std::string& name, size_t max_tasks, size_t max_bytes)
    : thread_num_(std::max<size_t>(1, thread_num ? thread_num : std::thread::hardware_concurrency())),
      name_(name), max_tasks_(max_tasks), max_bytes_(max_bytes) {}

// 析构时确保线程停止，防止悬挂任务。
// 默认 drain=true：把已入队的任务跑完再退，避免丢消息。
// 需要快速退出时（如 CometServer 析构）显式传 false。
ThreadPool::~ThreadPool() {
    Stop();
}

// 启动工作线程，避免重复启动。
// 幂等保护靠 workers_.empty() 判定：重复调 Start() 不会叠线程。
// 注意 Start() 与 Submit() 分离：构造时不起线程，让调用方能
// 先完成其他初始化再开线程，避免线程在对象未就绪时就跑起来。
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
//
// 幂等：靠 stopping_ 判定，重复调（包括显式 Stop 后析构再调）安全。
//
// drain 参数的语义差异很关键：
//   drain=true   保留队列，工作线程把剩余任务跑完才退（WorkerLoop 的
//                退出条件是 stopping_ && tasks_.empty()）。默认行为，不丢任务。
//   drain=false  立即清空队列。用于进程关闭时不想等在途 RPC，
//                代价是丢弃未执行的任务。
//
// 锁与 notify 的顺序：先在锁内置 stopping_，释锁后再 notify_all。
// 若持锁 notify，被唤醒的线程会立即卡在等锁上，白白多一次上下文切换。
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
//
// 返回 bool 而不是 void 是有意设计：调用方必须能知道任务被拒了。
// 比如 Comet 的 OnTextMessage 在 Submit 失败时会直接给客户端回
// 「upstream queue full」，而不是让气泡永远停在「发送中」。
//
// 四个拒绝条件：
//   stopping_          正在关闭
//   !started_          还没 Start（防止向未启动的池丢任务后永远不执行）
//   tasks_.size() >= max_tasks_   条数超限
//   bytes > max_bytes_ - queued_bytes_   字节超限
//
// 字节判定写成 bytes > max_bytes_ - queued_bytes_ 而不是
// queued_bytes_ + bytes > max_bytes_：后者在 queued_bytes_ 很大时
// 可能整数溢出绕过限制。前者因为 queued_bytes_ <= max_bytes_ 恒成立，
// 减法不会下溢。
//
// 双重限额的必要性：只限条数会让少量巨型消息（比如带图 JSON）
// 耗尽内存；只限字节又会让海量小消息卡住队头。
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
//
// 退出条件 stopping_ && tasks_.empty()：两个条件缺一不可。
// 只看 stopping_ 会丢掉 drain 模式下的剩余任务；
// 只看 tasks_.empty() 则会在 Stop 后永久阻塞在 cv_.wait 里。
//
// 字节配额在**取任务时**就扣减（而不是执行完）：否则同一批任务
// 可能被多个线程同时取出，导致 Submit 侧看到的 queued_bytes_ 偏小，
// 实际内存占用超出限额。
//
// 异常必须全部吃掉：std::thread 里未捕获的异常会直接 terminate
// 整个进程。一个任务的 bug 不应该拖垮整个服务，因此这里降级为
// 日志。catch(...) 兜底非 std::exception 的抛出物。
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

