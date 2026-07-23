#include <gtest/gtest.h>
#include <thread>
#include "tracing/trace.hpp"

using namespace cascade::core::tracing;

TEST(Tracing, CapturesSpansAcrossSimulatedLayers) {
    Trace trace("publish-request");
    std::uint64_t net_span, storage_span, consumer_span;
    {
        ScopedSpan net(trace, "network::receive");
        net_span = net.span_id();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        {
            ScopedSpan storage(trace, "storage::append", net_span);
            storage_span = storage.span_id();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        {
            ScopedSpan consumer(trace, "consumer::notify", net_span);
            consumer_span = consumer.span_id();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    auto spans = trace.spans();
    ASSERT_EQ(spans.size(), 4u); // root + network + storage + consumer

    for (auto& s : spans) {
        EXPECT_EQ(s.trace_id, trace.trace_id());
        EXPECT_GE(s.duration_us(), 0.0);
    }

    bool found_storage_child = false, found_consumer_child = false;
    for (auto& s : spans) {
        if (s.span_id == storage_span) { EXPECT_EQ(s.parent_span_id, net_span); found_storage_child = true; }
        if (s.span_id == consumer_span) { EXPECT_EQ(s.parent_span_id, net_span); found_consumer_child = true; }
    }
    EXPECT_TRUE(found_storage_child);
    EXPECT_TRUE(found_consumer_child);
}