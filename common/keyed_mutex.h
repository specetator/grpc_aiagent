#pragma once
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
namespace sparkpush {

// 按业务键分片的互斥锁表，用于「同一会话串行、不同会话并行」。
//
// 解决的问题：MessageService::Submit 需要把同一 session 的提交串行化
// （保证 Kafka 分区内顺序与 msg_seq 一致）。用一把全局锁会让所有
// 会话互相阻塞，QPS 直接降到单线程；给每个 session 常驻一把锁又会
// 随会话数无限增长。
//
// 关键设计：表里存的是 weak_ptr，锁的**所有权交给调用方**。
// 调用方持着 shared_ptr<mutex> 期间，weak_ptr 有效，后来者拿到同一把锁；
// 所有持有者都释放后 shared_ptr 引用计数归零，weak_ptr 自然 expired，
// 下次清理周期就把表项删掉。因此无需显式释放接口，也不会泄漏。
//
// 为什么返回 shared_ptr 而不是 mutex&：引用无法表达「谁还持有」，
// 也就无法判断何时能安全回收。
class KeyedMutex {
 public:
    // 取得（或创建）指定键的锁。返回的 shared_ptr 就是锁的生命周期凭证：
    // 调用方应把它构造成 unique_lock 后立即使用，不要长期缓存，
    // 否则该键永远不会被回收。
    //
    // 全程持 mutex_：查表与创建必须原子。否则两个线程可能同时
    // 为同一键各建一把锁，串行化就失效了。
    std::shared_ptr<std::mutex> Get(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        // 每 128 次请求才扫一遍过期项：把回收成本摊平。
        // 每次都扫是 O(n)，在高频路径上会反过来成为瓶颈；
        // 完全不扫则表会无限增长。128 是「内存占用」与「扫描开销」
        // 的经验平衡点，并不敏感。
        if (++requests_ % 128 == 0) {
            for (auto it = entries_.begin(); it != entries_.end(); ) {
                if (it->second.expired()) it = entries_.erase(it); else ++it;
            }
        }
        // entries_[key] 会默认构造一个空 weak_ptr；lock() 失败（首次创建
        // 或已被回收）时新建一把并回填。注意必须先 lock() 再判断，
        // 不能用 count()：表项存在但已 expired 的情况同样需要重建。
        auto& weak = entries_[key];
        auto result = weak.lock();
        if (!result) { result = std::make_shared<std::mutex>(); weak = result; }
        return result;
    }
 private:
    std::mutex mutex_;   // 只保护 entries_ 表本身，不是业务锁
    std::unordered_map<std::string, std::weak_ptr<std::mutex>> entries_;
    size_t requests_{0}; // 仅用于驱动周期性回收，溢出不影响正确性
};
}  // namespace sparkpush
