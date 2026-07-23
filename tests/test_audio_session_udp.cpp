#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "media/audio_session.hpp"

using namespace cascade::core::media;
using namespace std::chrono_literals;

static std::uint64_t now_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

TEST(AudioSessionUdp, EndToEndDeliveryNoLoss) {
    AudioSender sender("127.0.0.1", 31001, /*frame_interval_ms=*/20, /*bind_port=*/31000);
    AudioReceiver receiver("127.0.0.1", 31001, "127.0.0.1", 31000, 20, /*target_delay_ms=*/80);

    for (int i = 0; i < 20; ++i) {
        sender.send_frame({static_cast<std::uint8_t>(i), 0});
        std::this_thread::sleep_for(2ms);
    }
    std::this_thread::sleep_for(50ms);
    receiver.poll_incoming(now_ms());

    std::uint64_t t = now_ms() + 200; // force past every deadline
    int played = 0;
    for (int i = 0; i < 20; ++i) {
        auto r = receiver.pull(t);
        if (r.kind != JitterBuffer::PullResult::Kind::NotReady) played++;
    }
    EXPECT_EQ(played, 20);
}

// The retransmit-lite recovery path: sender skips actually transmitting
// one frame (simulating real network loss), receiver notices the gap via
// NACK, sender re-sends from its RetransmitCache, and the frame is
// eventually played normally rather than concealed -- proving the
// recovery mechanism, not just the concealment fallback.
TEST(AudioSessionUdp, RetransmitLiteRecoversASingleDroppedFrame) {
    AudioSender sender("127.0.0.1", 31011, 20, 31010);
    AudioReceiver receiver("127.0.0.1", 31011, "127.0.0.1", 31010, 20, /*target_delay_ms=*/150);

    for (int i = 0; i < 5; ++i) {
        auto frame = sender.prepare_frame({static_cast<std::uint8_t>(i)});
        if (i == 2) continue; // simulate network loss of frame #2 (don't transmit it)
        sender.transmit(frame);
    }
    std::this_thread::sleep_for(20ms);
    receiver.poll_incoming(now_ms());

    // Receiver notices seq 2 is missing (application-level gap detection
    // would normally trigger this; test drives it directly) and asks for it.
    receiver.request_retransmit(2);
    std::this_thread::sleep_for(20ms);
    sender.poll_and_handle_nacks();
    std::this_thread::sleep_for(20ms);
    receiver.poll_incoming(now_ms());

    std::uint64_t t = now_ms() + 400;
    int recovered_as_real_packet = 0, concealed = 0;
    for (int i = 0; i < 5; ++i) {
        auto r = receiver.pull(t);
        if (r.kind == JitterBuffer::PullResult::Kind::Packet) recovered_as_real_packet++;
        if (r.kind == JitterBuffer::PullResult::Kind::Concealed) concealed++;
    }
    EXPECT_EQ(recovered_as_real_packet, 5); // all 5, including the "lost" one, arrived via retransmit
    EXPECT_EQ(concealed, 0);
}