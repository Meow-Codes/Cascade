#include <gtest/gtest.h>
#include "media/adaptive_sender.hpp"

using namespace cascade::core::media;

TEST(AdaptiveSender, HighPriorityAlwaysSentUnderConstrainedBandwidth) {
    NetworkConditionEstimator est(2000);
    std::vector<QualityLevel> ladder = {{"low", 8}}; // 8 kbps = 1000 bytes/sec
    BitrateController ctrl(est, ladder, 0.05, 3, 1.0);
    AdaptiveSender sender(ctrl);

    for (int i = 0; i < 5; ++i) sender.enqueue(PacketPriority::High, std::vector<std::uint8_t>(200, 0xAA));
    for (int i = 0; i < 20; ++i) sender.enqueue(PacketPriority::Normal, std::vector<std::uint8_t>(200, 0xBB));

    auto sent = sender.drain(0);

    int high_sent = 0, normal_sent = 0;
    for (auto& pkt : sent) {
        if (pkt[0] == 0xAA) high_sent++;
        if (pkt[0] == 0xBB) normal_sent++;
    }

    EXPECT_EQ(high_sent, 5); // all High priority sent, unconditionally
    EXPECT_LT(normal_sent, 20); // budget exhausted before all Normal frames could go
    EXPECT_GT(sender.dropped_count(), 0u);
}

TEST(AdaptiveSender, EmptyQueueDrainsCleanly) {
    NetworkConditionEstimator est;
    std::vector<QualityLevel> ladder = {{"low", 64}};
    BitrateController ctrl(est, ladder);
    AdaptiveSender sender(ctrl);
    auto sent = sender.drain(0);
    EXPECT_TRUE(sent.empty());
    EXPECT_EQ(sender.dropped_count(), 0u);
}