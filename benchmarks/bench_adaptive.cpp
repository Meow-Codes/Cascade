#include <cstdio>
#include "media/adaptive_sender.hpp"
#include "media/bitrate_controller.hpp"
#include "media/network_estimator.hpp"

using namespace cascade::core::media;

int main() {
    NetworkConditionEstimator est(2000);
    std::vector<QualityLevel> ladder = {{"low", 128}, {"medium", 512}, {"high", 2048}};
    BitrateController ctrl(est, ladder, 0.05, 3, 1.3);
    AdaptiveSender sender(ctrl);

    std::printf("--- Adaptive bitrate simulation: bandwidth trace over 20s ---\n");
    std::printf("%6s  %10s  %8s  %8s\n", "t(s)", "bw(kbps)", "quality", "dropped");

    std::uint64_t t = 0;
    for (int sec = 0; sec < 20; ++sec) {
        // Simulate a bandwidth trace: starts generous, craters mid-stream
        // (simulating congestion), then recovers -- exercises both fast
        // downgrade and slow upgrade paths in one run.
        double kbps = (sec < 6) ? 2500.0 : (sec < 12) ? 300.0 : 2500.0;
        double bytes_per_sec = (kbps * 1000.0) / 8.0;

        for (int tick = 0; tick < 10; ++tick) { // 100ms ticks
            t += 100;
            est.record_received_bytes(static_cast<std::uint32_t>(bytes_per_sec / 10.0), t);
            sender.enqueue(PacketPriority::Normal, std::vector<std::uint8_t>(200, 0));
        }
        auto sent = sender.drain(t);
        std::printf("%6d  %10.0f  %8s  %8zu\n", sec + 1, kbps, ctrl.current().name.c_str(), sender.dropped_count());
    }
    return 0;
}