#include "metrics.h"

#include <algorithm>
#include <sstream>

namespace sparkpush {

MetricsRegistry& MetricsRegistry::Instance() {
  static MetricsRegistry registry;
  return registry;
}

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

void MetricsRegistry::Observe(const std::string& name, int64_t value) {
  if (name.empty()) return;
  std::lock_guard<std::mutex> lock(mutex_);
  auto& histogram = histograms_[name];
  ++histogram.count;
  histogram.sum += value;
  histogram.max = std::max(histogram.max, value);
}

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
    out << item.first << "_count " << item.second.count << "\n";
    out << item.first << "_sum " << item.second.sum << "\n";
    out << item.first << "_max " << item.second.max << "\n";
  }
  return out.str();
}

}  // namespace sparkpush
