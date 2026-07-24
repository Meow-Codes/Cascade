#include <cstdio>
#include <random>
#include <vector>
#include "media/jitter_buffer.hpp"

using namespace cascade::core::media;

static void bench_loss_sweep(std::uint32_t target_delay_ms) {
    constexpr std::uint32_t kFrameInterval = 20;
    constexpr int kTotalFrames = 5000;
    std::vector<double> loss_rates = {0.0, 0.02, 0.05, 0.10, 0.20, 0.30};

    std::printf("--- Jitter buffer loss-masking sweep (target_delay=%u ms) ---\n", target_delay_ms);
    for (double loss_rate : loss_rates) {
        JitterBuffer jb(kFrameInterval, target_delay_ms);
        std::mt19937 rng(7);
        std::bernoulli_distribution drop(loss_rate);
        std::uint64_t base_time = 10000;

        for (int i = 0; i < kTotalFrames; ++i) {
            if (drop(rng)) continue;
            std::uint64_t arrival = base_time + static_cast<std::uint64_t>(i) * kFrameInterval;
            jb.push(static_cast<std::uint32_t>(i), {static_cast<std::uint8_t>(i)}, arrival);
        }

        std::uint64_t final_time = base_time + static_cast<std::uint64_t>(kTotalFrames) * kFrameInterval + target_delay_ms + 100;
        for (int i = 0; i < kTotalFrames; ++i) jb.pull(final_time);

        std::printf("  simulated loss %5.1f%%  ->  effective loss %5.1f%%  (added latency: %u ms)\n",
                    loss_rate * 100, jb.effective_loss_rate() * 100, target_delay_ms);
    }
}

int main() {
    bench_loss_sweep(40);   // tight buffer: less latency, less tolerance
    bench_loss_sweep(100);  // moderate
    bench_loss_sweep(200);  // generous buffer: more latency, absorbs more jitter
    return 0;
}