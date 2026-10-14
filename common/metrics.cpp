#include "metrics.h"

#include <algorithm>
#include <sstream>

namespace sparkpush {

// 读 gauge。健康检查/就绪探针走这里，因此必须是 const 且不报错：
// 指标还没被 Set 过时返回 fallback，而不是让探针失败。
int64_t MetricsRegistry::GaugeValue(const std::string& name, int64_t fallback) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = gauges_.find(name);
    return it == gauges_.end() ? fallback : it->second;
}
namespace {
// 直方图桶边界（毫秒）。分布是有意的：1~100ms 密集（正常延迟区间，
// 需要高分辨率），1s 以上稀疏（长尾只需知道「有多糟」）。
// 上限 120000ms = 2 分钟，足以覆盖 Kafka 重试 + DLQ 的最慢路径。
//
// 数组长度必须与 metrics.h 里 Histogram::buckets 的 15 一致，
// 否则 Observe 会越界写。两处修改必须同步。
constexpr std::array<int64_t, 15> bounds{
    1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
    30000, 60000, 120000};
}

// Meyers 单例：局部 static 的初始化在 C++11 后由标准保证线程安全，
// 不需要自己加双检锁。
MetricsRegistry& MetricsRegistry::Instance() {
  static MetricsRegistry registry;
  return registry;
}

// 下面三个写入方法都先卡 name.empty()：指标名通常由字面量拼接而成
// （比如 "spark_push_rate_limited_total_" + scene），scene 为空时会
// 产生带尾下划线的脏名字。这里只能拦完全空的情况，拼接卫生靠调用方。
void MetricsRegistry::Increment(const std::string& name, int64_t value) {
  if (name.empty()) return;
  std::lock_guard<std::mutex> lock(mutex_);
  counters_[name] += value;
}

void MetricsRegistry::Set(const std::string& name, int64_t value) {
  if (name.empty()) return;
  std::lock_guard<std::mutex> lock(mutex_);
  gauges_[name] = value;
}

// 记录一个观测值。
//
// 注意循环用的是 <= 且**不 break**：一个值会同时计入所有不小于它的桶。
// 这正是 Prometheus histogram 要求的「累计桶（cumulative）」语义：
// le="100" 的桶必须包含 le="10" 的所有样本。若改成 break，导出的
// 数据在 Prometheus 侧会算出错误的分位数。
//
// 固定扫 15 个桶而不是二分：桶数极少，分支预测开销比二分比较更低。
void MetricsRegistry::Observe(const std::string& name, int64_t value) {
  if (name.empty()) return;
  std::lock_guard<std::mutex> lock(mutex_);
  auto& histogram = histograms_[name];
  ++histogram.count;
  histogram.sum += value;
  histogram.max = std::max(histogram.max, value);
  for (size_t i = 0; i < bounds.size(); ++i)
    if (value <= bounds[i]) ++histogram.buckets[i];
}

// 导出 Prometheus text exposition format。
//
// 全程持锁：快照必须一致，否则 counter 与其对应的 histogram 可能
// 来自不同时刻，分位数会算不自洽。
//
// _max 是本项目自行扩展的字段，不属于 Prometheus 标准 histogram。
// 大多数采集器会忽略未知行，不影响兼容；它的价值在于排障时能直接
// 看到最慢请求，而不必从均值里推测长尾。
std::string MetricsRegistry::RenderPrometheus() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::ostringstream out;
  for (const auto& item : counters_) {
    out << item.first << " " << item.second << "\n";
  }
  for (const auto& item : gauges_) {
    out << item.first << " " << item.second << "\n";
  }
  for (const auto& item : histograms_) {
    out << "# TYPE " << item.first << " histogram\n";
    for (size_t i = 0; i < bounds.size(); ++i)
      out << item.first << "_bucket{le=\"" << bounds[i] << "\"} "
          << item.second.buckets[i] << "\n";
    out << item.first << "_bucket{le=\"+Inf\"} " << item.second.count << "\n";
    out << item.first << "_count " << item.second.count << "\n";
    out << item.first << "_sum " << item.second.sum << "\n";
    out << item.first << "_max " << item.second.max << "\n";
  }
  return out.str();
}

}  // namespace sparkpush
