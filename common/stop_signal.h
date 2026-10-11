#pragma once
#include <csignal>
namespace sparkpush {
inline volatile std::sig_atomic_t stop_requested = 0;
inline void StopSignalHandler(int) { stop_requested = 1; }
inline void InstallStopSignals() {
    stop_requested = 0;
    std::signal(SIGINT, StopSignalHandler);
    std::signal(SIGTERM, StopSignalHandler);
}
}  // namespace sparkpush
