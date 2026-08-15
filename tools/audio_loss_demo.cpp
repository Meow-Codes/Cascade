// Drives a real AudioSender/AudioReceiver pair over actual loopback UDP
// (not the in-process JitterBuffer unit test from Phase 6) so external
// tc/netem-induced packet loss is genuinely exercised end-to-end, not
// simulated in-process. Prints concealment stats for the fault-injection
// script to capture.
#include <chrono>
#include <cstdio>
#include <thread>
#include "media/audio_session.hpp"

using namespace cascade::core::media;

static std::uint64_t now_ms() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

int main(int argc, char** argv) {
    int num_frames = (argc > 1) ? std::atoi(argv[1]) : 500;
    std::uint32_t target_delay_ms = (argc > 2) ? static_cast<std::uint32_t>(std::atoi(argv[2])) : 100;

    AudioSender sender("127.0.0.1", 31501, /*frame_interval_ms=*/20, /*bind_port=*/31500);
    AudioReceiver receiver("127.0.0.1", 31501, "127.0.0.1", 31500, 20, target_delay_ms);

    for (int i = 0; i < num_frames; ++i) {
        sender.send_frame({static_cast<std::uint8_t>(i & 0xFF), static_cast<std::uint8_t>((i >> 8) & 0xFF)});
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        receiver.poll_incoming(now_ms());
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(target_delay_ms + 200));
    receiver.poll_incoming(now_ms());

    std::uint64_t t = now_ms() + target_delay_ms + 300;
    int played = 0, concealed = 0;
    for (int i = 0; i < num_frames; ++i) {
        auto r = receiver.pull(t);
        if (r.kind == JitterBuffer::PullResult::Kind::Packet) played++;
        if (r.kind == JitterBuffer::PullResult::Kind::Concealed) concealed++;
    }

    std::printf("frames_sent=%d played=%d concealed=%d effective_loss_pct=%.2f\n",
                num_frames, played, concealed, (100.0 * concealed) / num_frames);
    return 0;
}