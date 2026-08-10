#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace sparkpush {

// 进程内轻量指标注册表。
// 目标是让 demo 在不依赖 Prometheus client 的情况下能够暴露可抓取指标；
// 生产环境可以将实现替换为 prometheus-cpp/OpenTelemetry exporter。
class MetricsRegistry {
 public:
  static MetricsRegistry& Instance();

  void Increment(const std::string& name, int64_t value = 1);
  void Set(const std::string& name, int64_t value);
  void Observe(const std::string& name, int64_t value);

  // 返回 Prometheus text exposition format。
  std::string RenderPrometheus() const;

 private:
  MetricsRegistry() = default;

  struct Histogram {
    int64_t count{0};
    int64_t sum{0};
    int64_t max{0};
  };

  mutable std::mutex mutex_;
  std::unordered_map<std::string, int64_t> counters_;
  std::unordered_map<std::string, int64_t> gauges_;
  std::unordered_map<std::string, Histogram> histograms_;
};

}  // namespace sparkpush
