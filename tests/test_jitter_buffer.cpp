#include <gtest/gtest.h>
#include <random>
#include "media/jitter_buffer.hpp"

using namespace cascade::core::media;

static std::vector<std::uint8_t> payload_for(std::uint32_t seq) {
    return {static_cast<std::uint8_t>(seq & 0xFF), static_cast<std::uint8_t>((seq >> 8) & 0xFF)};
}

TEST(JitterBuffer, ReordersOutOfOrderPackets) {
    JitterBuffer jb(/*frame_interval_ms=*/20, /*target_delay_ms=*/60);

    // Arrive out of order: 0, then 2, then 1 -- all comfortably inside
    // the jitter window (arrival times close together).
    jb.push(0, payload_for(0), 1000);
    jb.push(2, payload_for(2), 1010);
    jb.push(1, payload_for(1), 1015);

    auto r0 = jb.pull(1000);
    ASSERT_EQ(r0.kind, JitterBuffer::PullResult::Kind::Packet);
    EXPECT_EQ(r0.sequence, 0u);
    EXPECT_EQ(r0.payload, payload_for(0));

    auto r1 = jb.pull(1020); // by now packet 1 has arrived (arrived at 1015)
    ASSERT_EQ(r1.kind, JitterBuffer::PullResult::Kind::Packet);
    EXPECT_EQ(r1.sequence, 1u);
    EXPECT_EQ(r1.payload, payload_for(1));

    auto r2 = jb.pull(1040);
    ASSERT_EQ(r2.kind, JitterBuffer::PullResult::Kind::Packet);
    EXPECT_EQ(r2.sequence, 2u);
    EXPECT_EQ(r2.payload, payload_for(2));
}

TEST(JitterBuffer, NotReadyBeforeDeadlineEvenIfMissing) {
    JitterBuffer jb(20, 60);
    jb.push(0, payload_for(0), 1000);
    // sequence 1 never arrives yet -- but we haven't reached its deadline.
    jb.pull(1000); // consumes seq 0
    auto r = jb.pull(1005); // way before seq 1's deadline (1000+60+20=1080)
    EXPECT_EQ(r.kind, JitterBuffer::PullResult::Kind::NotReady);
}

TEST(JitterBuffer, ConcealsAfterDeadlinePassesUsingLastGoodPayload) {
    JitterBuffer jb(20, 60);
    jb.push(0, payload_for(0), 1000);
    jb.pull(1000); // plays seq 0, becomes "last good"
    // seq 1 never arrives.
    auto r = jb.pull(1000 + 60 + 20 + 1); // just past seq 1's deadline
    ASSERT_EQ(r.kind, JitterBuffer::PullResult::Kind::Concealed);
    EXPECT_EQ(r.sequence, 1u);
    EXPECT_EQ(r.payload, payload_for(0)); // repeated last-good payload (basic PLC)
}

// The core Phase 6 requirement: simulate N% random packet loss over a
// realistic stream and verify the jitter buffer's *effective* loss after
// concealment is bounded and sane -- concealment can't manufacture
// missing data, but it must never crash, never desync sequence order,
// and must conceal exactly the dropped frames (not extra ones).
TEST(JitterBuffer, MasksSimulatedRandomPacketLoss) {
    constexpr std::uint32_t kFrameInterval = 20;
    constexpr std::uint32_t kTargetDelay = 100; // 5 frames of jitter tolerance
    constexpr int kTotalFrames = 1000;
    constexpr double kDropProbability = 0.08; // 8% loss

    JitterBuffer jb(kFrameInterval, kTargetDelay);
    std::mt19937 rng(123); // fixed seed: reproducible
    std::bernoulli_distribution drop(kDropProbability);

    int actually_dropped = 0;
    std::uint64_t base_time = 5000;
    for (int i = 0; i < kTotalFrames; ++i) {
        std::uint64_t arrival = base_time + static_cast<std::uint64_t>(i) * kFrameInterval;
        if (drop(rng)) { actually_dropped++; continue; }
        jb.push(static_cast<std::uint32_t>(i), payload_for(static_cast<std::uint32_t>(i)), arrival);
    }

    // Drain by advancing "now" well past the last possible deadline.
    std::uint64_t final_time = base_time + static_cast<std::uint64_t>(kTotalFrames) * kFrameInterval + kTargetDelay + 100;
    int concealed_seen = 0, played_seen = 0;
    for (int i = 0; i < kTotalFrames; ++i) {
        auto r = jb.pull(final_time);
        ASSERT_NE(r.kind, JitterBuffer::PullResult::Kind::NotReady) << "should be resolved by final_time";
        EXPECT_EQ(r.sequence, static_cast<std::uint32_t>(i)); // strict in-order playout regardless of loss
        if (r.kind == JitterBuffer::PullResult::Kind::Concealed) concealed_seen++;
        else played_seen++;
    }

    EXPECT_EQ(concealed_seen, actually_dropped);
    EXPECT_EQ(played_seen, kTotalFrames - actually_dropped);
    EXPECT_EQ(jb.concealed_count(), static_cast<std::size_t>(actually_dropped));

    double effective = jb.effective_loss_rate();
    std::printf("Simulated loss: %.1f%%, effective (post-concealment) loss: %.1f%%\n",
                kDropProbability * 100, effective * 100);
    // Concealment fills gaps with repeated audio (masks the *dropout*,
    // not the data loss) -- effective_loss_rate still counts them as
    // concealed by design, so this should equal the simulated rate; the
    // "masking" benefit is that playout never stalls/glitches, not that
    // the loss disappears from metrics. This assertion documents that
    // distinction rather than asserting an unrealistic loss reduction.
    EXPECT_NEAR(effective, kDropProbability, 0.03);
}