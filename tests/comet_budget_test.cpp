#include "connection_budget.h"
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>
using namespace sparkpush;
static void Require(bool result) { if (!result) { std::cerr << "budget assertion failed\n"; std::exit(1); } }
int main() {
    Require(ValidDeviceId("android_01.a-b"));
    Require(!ValidDeviceId("")); Require(!ValidDeviceId("device/../other"));
    Require(!ValidDeviceId(std::string(65, 'a'))); Require(!ValidDeviceId("设备"));
    ConnectionBudget budget(100, 2);
    Require(budget.Reserve(40)); Require(budget.Reserve(40));
    Require(!budget.Reserve(1));
    budget.Complete(40, 40);
    Require(!budget.Reserve(21)); Require(budget.Reserve(20));
    budget.Complete(40, 80); budget.Complete(20, 100);
    Require(!budget.Reserve(1)); budget.BufferDrained();
    Require(!budget.Reserve(static_cast<size_t>(-1)));
    Require(budget.Reserve(100)); budget.Complete(100, 0);
    budget.Close(); Require(!budget.Reserve(1));
    // With all producers held at a barrier, admitted bytes never exceed 1000.
    ConnectionBudget concurrent(1000, 100);
    std::atomic<int> admitted{0}, attempted{0};
    std::atomic<bool> release{false};
    std::vector<std::thread> workers;
    for (int i = 0; i < 32; ++i) workers.emplace_back([&] {
        const bool accepted = concurrent.Reserve(100);
        if (accepted) ++admitted;
        ++attempted;
        while (!release.load()) std::this_thread::yield();
        if (accepted) concurrent.Complete(100, 0);
    });
    while (attempted.load() < 32) std::this_thread::yield();
    Require(admitted.load() == 10); release.store(true);
    for (auto& worker : workers) worker.join();
    Require(concurrent.Reserve(1000)); concurrent.Complete(1000, 0);
    std::cout << "comet budget: bounds, buffer accounting, device validation, concurrency PASS\n";
}
