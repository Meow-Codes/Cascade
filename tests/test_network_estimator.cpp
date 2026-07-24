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
    est.record_received_bytes(1000, 4000);

    double bw = est.estimated_bandwidth_kbps(4500);

    EXPECT_NEAR(bw, 16.0, 2.0);
}

TEST(NetworkConditionEstimator, LossEmaConvergesTowardObservedRate) {
    NetworkConditionEstimator est(1000, 0.3);

    for (int i = 0; i < 100; ++i)
        est.record_loss_sample(true);

    EXPECT_NEAR(est.estimated_loss_rate(), 1.0, 0.01);
}

TEST(NetworkConditionEstimator, RttEmaSmoothsSamples) {
    NetworkConditionEstimator est(1000, 0.2, 0.3);
    for (int i = 0; i < 50; ++i) est.record_rtt_sample(40.0);
    EXPECT_NEAR(est.smoothed_rtt_ms(), 40.0, 1.0);
}