#pragma once
// Cascade :: core::threadpool
//
// Fixed-size thread pool for CPU/IO task execution.
//
// Design notes:
//  - Task queue is our own MpmcQueue (bounded, lock-free), dogfooding
//    Phase 1's own components rather than std::queue + mutex.
//  - Bounded queues need a full-queue policy: submit() retries briefly
//    then falls back to running the task synchronously on the caller's
//    thread rather than blocking forever or silently dropping work — a
//    deliberate backpressure choice so overload is felt, not hidden.
//  - Workers sleep/wake via condition_variable rather than spin-waiting:
//    a thread pool's bottleneck is almost never wakeup latency, and a
//    condvar keeps idle CPU usage at zero.

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

#include "queue/mpmc_queue.hpp"

namespace cascade::core {

class ThreadPool {
public:
    explicit ThreadPool(std::size_t thread_count = std::thread::hardware_concurrency(),
                         std::size_t queue_capacity = 4096)
        : queue_(queue_capacity) {
        if (thread_count == 0) thread_count = 1;
        workers_.reserve(thread_count);
        for (std::size_t i = 0; i < thread_count; ++i) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

    ~ThreadPool() {
        stop_.store(true, std::memory_order_relaxed);
        wake_cv_.notify_all();
        for (auto& t : workers_) {
            if (t.joinable()) t.join();
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    template <typename F, typename... Args>
    auto submit(F&& f, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>> {
        using ReturnType = std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        std::future<ReturnType> result = task->get_future();

        Task wrapped = [task] { (*task)(); };

        constexpr int kMaxRetries = 3;
        for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
            if (queue_.try_push(wrapped)) {
                pending_.fetch_add(1, std::memory_order_relaxed);
                wake_cv_.notify_one();
                return result;
            }
        }
        // Queue saturated: apply backpressure by running inline.
        wrapped();
        return result;
    }

    std::size_t thread_count() const { return workers_.size(); }
    std::size_t pending_approx() const { return pending_.load(std::memory_order_relaxed); }

private:
    using Task = std::function<void()>;

    void worker_loop() {
        while (true) {
            auto task = queue_.try_pop();
            if (task) {
                pending_.fetch_sub(1, std::memory_order_relaxed);
                (*task)();
                continue;
            }

            if (stop_.load(std::memory_order_relaxed)) return;

            std::unique_lock<std::mutex> lock(wake_mutex_);
            wake_cv_.wait_for(lock, std::chrono::milliseconds(5), [this] {
                return stop_.load(std::memory_order_relaxed) || pending_.load(std::memory_order_relaxed) > 0;
            });
        }
    }

    MpmcQueue<Task> queue_;
    std::vector<std::thread> workers_;
    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> pending_{0};

    std::mutex wake_mutex_;
    std::condition_variable wake_cv_;
};

} // namespace cascade::core