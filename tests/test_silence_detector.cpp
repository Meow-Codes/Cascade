#include <gtest/gtest.h>
#include <vector>
#include "media/silence_detector.hpp"

using namespace cascade::core::media;

TEST(SilenceDetector, LoudSamplesAreNotSilence) {
    std::vector<std::int16_t> samples(160, 12000); // loud constant tone
    EXPECT_FALSE(is_silence(samples.data(), samples.size()));
}

TEST(SilenceDetector, NearZeroSamplesAreSilence) {
    std::vector<std::int16_t> samples(160, 5); // near-zero amplitude
    EXPECT_TRUE(is_silence(samples.data(), samples.size()));
}

TEST(SilenceDetector, EmptyIsSilence) {
    EXPECT_TRUE(is_silence(nullptr, 0));
}