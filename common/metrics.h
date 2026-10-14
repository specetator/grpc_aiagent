#pragma once

#include <cstdint>
#include <array>
#include <mutex>
#include <string>
#include <unordered_map>

namespace sparkpush {

// 进程内轻量指标注册表。
// 目标是让 demo 在不依赖 Prometheus client 的情况下能够暴露可抓取指标；
// 生产环境可以将实现替换为 prometheus-cpp/OpenTelemetry exporter。
//
// 为何自己写而不用现成库：本项目的指标需求很简单（计数 + 当前值 +
// 少量分位数），而运维脚本、健康检查、E2E 验收都直接读这些名字，
// 引入重型依赖反而让「指标名」这个契约变得不直观。
//
// 全局单例（Instance()）的取舍：指标埋点散在几十个函数里，逐层传递
// registry 指针会污染所有签名。代价是单测里指标会互相干扰，
// 因此测试不依赖指标值做断言。
//
// 重要：这些指标不只是「可观测性」。其中几个是**硬门禁**：
//   spark_push_service_ready              健康检查读它
//   spark_push_comet_logic_stream_ready   启动脚本等它为 1
//   spark_push_comet_push_stream_ready    同上
//   spark_push_delivery_lost_total        E2E 验收要求为 0
// 改名或删除这些会直接让运维脚本和测试失效。
class MetricsRegistry {
 public:
  static MetricsRegistry& Instance();

  // 单调递增计数器（counter）。重复调用累加。
  void Increment(const std::string& name, int64_t value = 1);
  // 瞬时值（gauge），可上可下。用于 ready 标志、当前连接数这类状态。
  void Set(const std::string& name, int64_t value);
  // 读回 gauge；不存在时返回 fallback。供健康检查/就绪探针使用。
  int64_t GaugeValue(const std::string& name, int64_t fallback = 0) const;
  // 记录一个观测值，累入直方图（count/sum/max + 分桶）。
  // 用于延迟类指标，比如 spark_push_delivery_latency_ms。
  void Observe(const std::string& name, int64_t value);

  // 返回 Prometheus text exposition format。
  // 由 MetricsHttpServer 挂在 /metrics 上，三个服务共用同一实现。
  std::string RenderPrometheus() const;

 private:
  MetricsRegistry() = default;

  // 固定 15 个桶的直方图。用数组而不是 vector：桶边界在 .cpp 里
  // 写死，数量编译期已知，避开动态分配（Observe 在热路径上）。
  // max 单独存一份：Prometheus 没这个字段，但排障时「最大延迟」
  // 比均值有用得多（均值会被大量快请求掩盖长尾）。
  struct Histogram {
    int64_t count{0};
    int64_t sum{0};
    int64_t max{0};
    std::array<int64_t, 15> buckets{};
  };

  // 一把全局锁保护三张表。简单但会成为高 QPS 下的争用点；
  // 当前指标点数量有限，尚未成为瓶颈。若要扩容，应改为分片锁
  // 或每线程累加后定期汇聚。
  // mutable 是为了让 const 的 GaugeValue/RenderPrometheus 也能加锁。
  mutable std::mutex mutex_;
  std::unordered_map<std::string, int64_t> counters_;
  std::unordered_map<std::string, int64_t> gauges_;
  std::unordered_map<std::string, Histogram> histograms_;
};

}  // namespace sparkpush
