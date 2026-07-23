#pragma once
// Cascade :: core::tracing :: Trace / ScopedSpan
//
// Follows one logical request (e.g. one publish call) as spans across
// networking -> storage -> consumer. In-memory collection only -- not
// wired to a real backend (Jaeger/Zipkin) for v1; a log/println-based
// collector proves the cross-layer propagation concept just as well as a
// real backend would for a single-broker system, and adding OTLP export
// is a clean drop-in later without touching call sites.

#include <chrono>
#include <cstdint>
#include <mutex>
#include <random>
#include <string>
#include <vector>

namespace cascade::core::tracing {

struct SpanRecord {
    std::string name;
    std::uint64_t trace_id;
    std::uint64_t span_id;
    std::uint64_t parent_span_id; // 0 = root
    std::chrono::steady_clock::time_point start;
    std::chrono::steady_clock::time_point end;
    double duration_us() const { return std::chrono::duration<double, std::micro>(end - start).count(); }
};

class Trace {
public:
    explicit Trace(std::string name) : trace_id_(random_id()) {
        start_span(std::move(name), 0);
    }

    std::uint64_t start_span(std::string name, std::uint64_t parent_span_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::uint64_t id = random_id();
        spans_.push_back(SpanRecord{std::move(name), trace_id_, id, parent_span_id,
                                     std::chrono::steady_clock::now(), {}});
        return id;
    }

    void end_span(std::uint64_t span_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto& s : spans_) {
            if (s.span_id == span_id) { s.end = std::chrono::steady_clock::now(); return; }
        }
    }

    std::uint64_t trace_id() const { return trace_id_; }
    std::vector<SpanRecord> spans() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return spans_;
    }

    std::string to_string() const {
        auto snapshot = spans();
        std::string out = "trace " + std::to_string(trace_id_) + ":\n";
        for (auto& s : snapshot) {
            out += "  [" + s.name + "] " + std::to_string(s.duration_us()) + " us (span "
                 + std::to_string(s.span_id) + ", parent " + std::to_string(s.parent_span_id) + ")\n";
        }
        return out;
    }

private:
    static std::uint64_t random_id() {
        static thread_local std::mt19937_64 rng{std::random_device{}()};
        return rng();
    }

    std::uint64_t trace_id_;
    mutable std::mutex mutex_;
    std::vector<SpanRecord> spans_;
};

// RAII: start_span on construction, end_span on destruction.
class ScopedSpan {
public:
    ScopedSpan(Trace& trace, std::string name, std::uint64_t parent_span_id = 0)
        : trace_(trace), span_id_(trace.start_span(std::move(name), parent_span_id)) {}
    ~ScopedSpan() { trace_.end_span(span_id_); }
    std::uint64_t span_id() const { return span_id_; }

    ScopedSpan(const ScopedSpan&) = delete;
    ScopedSpan& operator=(const ScopedSpan&) = delete;
private:
    Trace& trace_;
    std::uint64_t span_id_;
};

} // namespace cascade::core::tracing