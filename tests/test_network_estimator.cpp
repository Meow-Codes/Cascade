#include <gtest/gtest.h>
#include "media/network_estimator.hpp"

using namespace cascade::core::media;

TEST(NetworkConditionEstimator, BandwidthReflectsRecentSamples) {
    NetworkConditionEstimator est(1000);
    est.record_received_bytes(12500, 0); // 100,000 bits
    double bw = est.estimated_bandwidth_kbps(1000); // 1s elapsed -> ~100 kbps
    EXPECT_NEAR(bw, 100.0, 5.0);
}

TEST(NetworkConditionEstimator, OldSamplesEvictedOutsideWindow) {
    NetworkConditionEstimator est(500);
    est.record_received_bytes(100000, 0);
    est.record_received_bytes(1000, 2000); // far beyond the 500ms window relative to sample 1
    double bw = est.estimated_bandwidth_kbps(2000);
    EXPECT_LT(bw, 50.0); // huge first sample should be evicted
}

TEST(NetworkConditionEstimator, LossEmaConvergesTowardObservedRate) {
    NetworkConditionEstimator est(1000, 0.3);
    for (int i = 0; i < 200; ++i) est.record_loss_sample(i % 10 == 0); // ~10% loss
    EXPECT_NEAR(est.estimated_loss_rate(), 0.10, 0.05);
}

TEST(NetworkConditionEstimator, RttEmaSmoothsSamples) {
    NetworkConditionEstimator est(1000, 0.2, 0.3);
    for (int i = 0; i < 50; ++i) est.record_rtt_sample(40.0);
    EXPECT_NEAR(est.smoothed_rtt_ms(), 40.0, 1.0);
}