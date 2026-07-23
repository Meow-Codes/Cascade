#pragma once
// Cascade :: core::metrics :: sample_system_metrics
//
// Linux-only (consistent with this project's scope from Phase 2 onward):
// reads resident memory from /proc/self/status and CPU time from
// getrusage(), pushing both into gauges. Call periodically (e.g. once per
// /metrics scrape, or from a dedicated timer) rather than per-request --
// this is process-wide state, not per-event.

#include <sys/resource.h>
#include <cstdio>
#include <cstring>
#include "metrics/metrics_registry.hpp"

namespace cascade::core::metrics {

inline void sample_system_metrics(MetricsRegistry& registry) {
    FILE* f = std::fopen("/proc/self/status", "r");
    if (f) {
        char line[256];
        while (std::fgets(line, sizeof(line), f)) {
            if (std::strncmp(line, "VmRSS:", 6) == 0) {
                long kb = 0;
                std::sscanf(line + 6, "%ld", &kb);
                registry.gauge("process_resident_memory_bytes").set(kb * 1024);
                break;
            }
        }
        std::fclose(f);
    }

    struct rusage usage{};
    if (::getrusage(RUSAGE_SELF, &usage) == 0) {
        double user_ms = usage.ru_utime.tv_sec * 1000.0 + usage.ru_utime.tv_usec / 1000.0;
        double sys_ms = usage.ru_stime.tv_sec * 1000.0 + usage.ru_stime.tv_usec / 1000.0;
        // Gauge is int64; stored in milliseconds (not seconds) to avoid
        // truncating to 0 for short-lived processes.
        registry.gauge("process_cpu_user_milliseconds_total").set(static_cast<std::int64_t>(user_ms));
        registry.gauge("process_cpu_system_milliseconds_total").set(static_cast<std::int64_t>(sys_ms));
    }
}

} // namespace cascade::core::metrics