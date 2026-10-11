#include <future>
#include <stdexcept>
#include "thread_pool.h"
#include "keyed_mutex.h"
using namespace sparkpush;
void Require(bool value) { if (!value) throw std::runtime_error("capacity regression"); }
int main() {
    ThreadPool pool(1, "capacity-test", 2, 10);
    pool.Start();
    std::promise<void> entered, release;
    auto unblock = release.get_future();
    Require(pool.Submit([&] { entered.set_value(); unblock.wait(); }));
    entered.get_future().wait();
    Require(pool.Submit([] {}, 6));
    Require(!pool.Submit([] {}, 5));
    Require(pool.Submit([] {}, 1));
    Require(!pool.Submit([] {}, 1));
    release.set_value();
    pool.Stop();
    Require(!pool.Submit([] {}));
    KeyedMutex keys;
    auto a = keys.Get("room-a");
    auto same = keys.Get("room-a");
    auto b = keys.Get("room-b");
    Require(a == same && a != b);
    std::unique_lock<std::mutex> blocked(*a);
    auto independent = std::async(std::launch::async, [&] {
        std::unique_lock<std::mutex> lock(*b, std::try_to_lock);
        return lock.owns_lock();
    });
    Require(independent.get());
}
