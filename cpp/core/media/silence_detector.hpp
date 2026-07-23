#pragma once
// Cascade :: core::media :: is_silence
//
// Simple average-absolute-amplitude check on 16-bit PCM samples. This is
// deliberately the simplest correct silence detector, not a spectral or
// VAD-model-based one -- good enough to demonstrate silence-flagging on
// the wire and to unit test; a production system would use a proper
// voice-activity-detection algorithm.

#include <cstdint>
#include <cstdlib>
#include <cstddef>

namespace cascade::core::media {

inline bool is_silence(const std::int16_t* samples, std::size_t sample_count, std::int16_t threshold = 200) {
    if (sample_count == 0) return true;
    std::int64_t sum_abs = 0;
    for (std::size_t i = 0; i < sample_count; ++i) sum_abs += std::abs(static_cast<int>(samples[i]));
    std::int64_t avg = sum_abs / static_cast<std::int64_t>(sample_count);
    return avg < threshold;
}

} // namespace cascade::core::media