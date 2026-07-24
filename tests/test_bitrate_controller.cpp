#include <gtest/gtest.h>
#include "media/bitrate_controller.hpp"

using namespace cascade::core::media;

TEST(BitrateController, DowngradesImmediatelyOnHighLoss) {
    NetworkConditionEstimator est(2000);
    std::vector<QualityLevel> ladder = {{"low", 64}, {"medium", 256}, {"high", 1024}};
    BitrateController ctrl(est, ladder, 0.05, 2, 1.0);

    std::uint64_t t = 0;
    for (int i = 0; i < 20 && ctrl.current().name != "high"; ++i) {
        t += 300;
        est.record_received_bytes(300000, t);
        ctrl.evaluate(t);
    }
    ASSERT_EQ(ctrl.current().name, "high") << "failed to reach top quality with abundant bandwidth";

    std::uint32_t before = ctrl.current().bitrate_kbps;
    est.record_loss_sample(true);
    t += 300;
    ctrl.evaluate(t);
    EXPECT_LT(ctrl.current().bitrate_kbps, before);
}

TEST(BitrateController, RequiresSustainedGoodConditionsBeforeUpgrading) {
    NetworkConditionEstimator est(2000);
    std::vector<QualityLevel> ladder = {{"low", 64}, {"medium", 256}};
    BitrateController ctrl(est, ladder, 0.05, /*upgrade_confirmations=*/3, 1.0);

    std::uint64_t t = 0;
    // Alternating good/bad loss samples reset the confirmation counter
    // every other tick, so despite "mostly good" bandwidth, 3 consecutive
    // good evaluations should never accumulate.
    for (int i = 0; i < 30; ++i) {
        t += 200;
        est.record_received_bytes(300000, t);
        est.record_loss_sample(i % 2 == 0);
        ctrl.evaluate(t);
    }
    EXPECT_EQ(ctrl.current().name, "low");
}

TEST(BitrateController, UpgradesAfterSustainedGoodConditions) {
    NetworkConditionEstimator est(2000);
    std::vector<QualityLevel> ladder = {{"low", 64}, {"medium", 256}};
    BitrateController ctrl(est, ladder, 0.05, 3, 1.0);

    std::uint64_t t = 0;
    for (int i = 0; i < 20 && ctrl.current().name != "medium"; ++i) {
        t += 300;
        est.record_received_bytes(300000, t); // consistently abundant, no loss recorded
        ctrl.evaluate(t);
    }
    EXPECT_EQ(ctrl.current().name, "medium");
}