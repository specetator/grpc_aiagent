#include "metrics.h"

#include <algorithm>
#include <sstream>

namespace sparkpush {
namespace {
constexpr std::array<int64_t, 15> bounds{
    1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000,
    30000, 60000, 120000};
}

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
  for (size_t i = 0; i < bounds.size(); ++i)
    if (value <= bounds[i]) ++histogram.buckets[i];
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
