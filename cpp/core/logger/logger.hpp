#pragma once
// Cascade :: core::logger
//
// A minimal async, thread-safe structured logger.
//
// Design notes:
//  - Log calls push a formatted record onto a queue and return immediately;
//    a single background thread owns the actual write to stdout/stderr.
//    This keeps hot-path threads (networking, storage) from blocking on I/O.
//  - v1 uses a mutex + condition_variable queue rather than our own lock-free
//    queue, deliberately: the lock-free queue is validated in relative
//    isolation before anything else in the codebase depends on it. Swapping
//    the backing queue for core::queue::MpmcQueue later is a drop-in change.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <format>
#include <mutex>
#include <string>
#include <thread>

namespace cascade::core {

enum class LogLevel { Trace, Debug, Info, Warn, Error, Fatal };

constexpr const char* to_string(LogLevel level) {
    switch (level) {
        case LogLevel::Trace: return "TRACE";
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO";
        case LogLevel::Warn:  return "WARN";
        case LogLevel::Error: return "ERROR";
        case LogLevel::Fatal: return "FATAL";
    }
    return "UNKNOWN";
}

class Logger {
public:
    static Logger& instance() {
        static Logger logger;
        return logger;
    }

    void set_min_level(LogLevel level) { min_level_.store(level, std::memory_order_relaxed); }

    template <typename... Args>
    void log(LogLevel level, std::format_string<Args...> fmt, Args&&... args) {
        if (level < min_level_.load(std::memory_order_relaxed)) return;

        std::string message = std::format(fmt, std::forward<Args>(args)...);
        auto now = std::chrono::system_clock::now();

        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push_back(Record{now, level, std::move(message), std::this_thread::get_id()});
        }
        cv_.notify_one();
    }

    // Blocks until all currently-queued records have been written. Mainly
    // for tests and clean shutdown.
    void flush() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_flushed_.wait(lock, [this] { return queue_.empty(); });
    }

    ~Logger() {
        stop_.store(true, std::memory_order_relaxed);
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
    }

    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

private:
    struct Record {
        std::chrono::system_clock::time_point ts;
        LogLevel level;
        std::string message;
        std::thread::id tid;
    };

    Logger() : worker_([this] { run(); }) {}

    void run() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (true) {
            cv_.wait(lock, [this] { return !queue_.empty() || stop_.load(std::memory_order_relaxed); });

            while (!queue_.empty()) {
                Record rec = std::move(queue_.front());
                queue_.pop_front();
                lock.unlock();
                write(rec);
                lock.lock();
            }
            cv_flushed_.notify_all();

            if (stop_.load(std::memory_order_relaxed) && queue_.empty()) break;
        }
    }

    static void write(const Record& rec) {
        auto t = std::chrono::system_clock::to_time_t(rec.ts);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      rec.ts.time_since_epoch()) % 1000;
        std::tm tm_buf{};

        #ifdef _WIN32
            localtime_s(&tm_buf, &t);
        #else
            localtime_r(&t, &tm_buf);
        #endif

        FILE* out = (rec.level >= LogLevel::Error) ? stderr : stdout;
        std::fprintf(out, "%04d-%02d-%02d %02d:%02d:%02d.%03lld [%-5s] [tid:%zu] %s\n",
                     tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
                     tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
                     static_cast<long long>(ms.count()),
                     to_string(rec.level),
                     std::hash<std::thread::id>{}(rec.tid) % 100000,
                     rec.message.c_str());
    }

    std::atomic<LogLevel> min_level_{LogLevel::Info};
    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable cv_flushed_;
    std::deque<Record> queue_;
    std::atomic<bool> stop_{false};
    std::thread worker_;
};

} // namespace cascade::core

#define CASCADE_LOG_TRACE(...) ::cascade::core::Logger::instance().log(::cascade::core::LogLevel::Trace, __VA_ARGS__)
#define CASCADE_LOG_DEBUG(...) ::cascade::core::Logger::instance().log(::cascade::core::LogLevel::Debug, __VA_ARGS__)
#define CASCADE_LOG_INFO(...)  ::cascade::core::Logger::instance().log(::cascade::core::LogLevel::Info,  __VA_ARGS__)
#define CASCADE_LOG_WARN(...)  ::cascade::core::Logger::instance().log(::cascade::core::LogLevel::Warn,  __VA_ARGS__)
#define CASCADE_LOG_ERROR(...) ::cascade::core::Logger::instance().log(::cascade::core::LogLevel::Error, __VA_ARGS__)
#define CASCADE_LOG_FATAL(...) ::cascade::core::Logger::instance().log(::cascade::core::LogLevel::Fatal, __VA_ARGS__)