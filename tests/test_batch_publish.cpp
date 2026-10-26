#include <gtest/gtest.h>
#include <atomic>
#include <filesystem>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "broker/broker.hpp"
#include "broker/consumer.hpp"
#include "broker/producer.hpp"
#include "storage/log.hpp"

using namespace cascade::core;

namespace {

std::string fresh_dir(const char* name) {
    auto dir = std::string("/tmp/cascade_batch_test_") + name + "_" + std::to_string(::getpid());
    std::filesystem::remove_all(dir);
    return dir;
}

std::vector<std::string> make_records(int n, const std::string& prefix) {
    std::vector<std::string> out;
    for (int i = 0; i < n; ++i) out.push_back(prefix + std::to_string(i) + "-" + std::string(40, 'x'));
    return out;
}

std::vector<storage::PayloadView> views_of(const std::vector<std::string>& recs) {
    std::vector<storage::PayloadView> v;
    v.reserve(recs.size());
    for (auto& r : recs) v.emplace_back(reinterpret_cast<const std::uint8_t*>(r.data()), r.size());
    return v;
}

std::string text_of(const storage::ReadResult& r) { return std::string(r.payload.begin(), r.payload.end()); }

} // namespace

TEST(BatchPublish, ContiguousOffsetsAndReadBack) {
    auto dir = fresh_dir("contiguous");
    storage::Log log(dir, 1 << 20);

    auto a = make_records(100, "a");
    EXPECT_EQ(log.append_batch(views_of(a), true), 0u);
    EXPECT_EQ(log.next_offset(), 100u);

    auto b = make_records(10, "b");
    EXPECT_EQ(log.append_batch(views_of(b), true), 100u);

    for (int i = 0; i < 100; ++i) {
        auto r = log.read(static_cast<std::uint64_t>(i));
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(text_of(*r), a[i]);
    }
    for (int i = 0; i < 10; ++i) {
        auto r = log.read(100 + static_cast<std::uint64_t>(i));
        ASSERT_TRUE(r.has_value());
        EXPECT_EQ(text_of(*r), b[i]);
    }
    std::filesystem::remove_all(dir);
}

// The core claim: N records, one flush -- asserted by counting, not timing.
TEST(BatchPublish, OneFlushPerBatchVersusOnePerRecord) {
    auto dir = fresh_dir("flushcount");
    storage::Log log(dir, 1 << 20);

    auto recs = make_records(100, "f");
    log.append_batch(views_of(recs), true);
    EXPECT_EQ(log.flush_calls(), 1u);

    log.append_batch(views_of(recs), true);
    log.append_batch(views_of(recs), true);
    EXPECT_EQ(log.flush_calls(), 3u);

    for (auto& r : recs) {
        log.append(reinterpret_cast<const std::uint8_t*>(r.data()), static_cast<std::uint32_t>(r.size()));
        log.flush();
    }
    EXPECT_EQ(log.flush_calls(), 103u);

    // flush_after=false must not flush at all
    log.append_batch(views_of(recs), false);
    EXPECT_EQ(log.flush_calls(), 103u);
    std::filesystem::remove_all(dir);
}

TEST(BatchPublish, BatchSpanningSegmentRolloverSurvivesReopen) {
    auto dir = fresh_dir("rollover");
    constexpr int kRecords = 200;
    auto recs = make_records(kRecords, "r");
    {
        storage::Log log(dir, 4096); // ~57 B records => several segments inside ONE batch
        EXPECT_EQ(log.append_batch(views_of(recs), true), 0u);
        EXPECT_GT(log.segment_count(), 2u);
    }
    storage::Log reopened(dir, 4096);
    EXPECT_EQ(reopened.next_offset(), static_cast<std::uint64_t>(kRecords));
    for (int i = 0; i < kRecords; ++i) {
        auto r = reopened.read(static_cast<std::uint64_t>(i));
        ASSERT_TRUE(r.has_value()) << "missing offset " << i;
        EXPECT_EQ(text_of(*r), recs[i]);
    }
    std::filesystem::remove_all(dir);
}

TEST(BatchPublish, OversizedRecordRejectedBeforeAnythingIsWritten) {
    auto dir = fresh_dir("oversized");
    storage::Log log(dir, 4096);

    std::vector<std::string> recs = {"small-1", "small-2", std::string(5000, 'Z')};
    EXPECT_THROW(log.append_batch(views_of(recs), true), std::length_error);
    EXPECT_EQ(log.next_offset(), 0u);   // none of the batch was appended
    EXPECT_EQ(log.flush_calls(), 0u);

    std::vector<std::string> ok = {"fine-1", "fine-2"};
    EXPECT_EQ(log.append_batch(views_of(ok), true), 0u);
    std::filesystem::remove_all(dir);
}

TEST(BatchPublish, EmptyBatchIsANoOp) {
    auto dir = fresh_dir("empty");
    storage::Log log(dir, 1 << 20);
    std::vector<storage::PayloadView> none;
    EXPECT_EQ(log.append_batch(none, true), 0u);
    EXPECT_EQ(log.next_offset(), 0u);
    EXPECT_EQ(log.flush_calls(), 0u);
    std::filesystem::remove_all(dir);
}

// Each batch must occupy consecutive offsets with its own records in order,
// even with several threads batching into the same log across rollovers.
TEST(BatchPublish, ConcurrentBatchesAreNeverInterleaved) {
    auto dir = fresh_dir("concurrent");
    storage::Log log(dir, 1 << 16);

    constexpr int kThreads = 4, kBatches = 50, kBatchSize = 20;
    struct Ack { int thread, batch; std::uint64_t first; };
    std::vector<std::vector<Ack>> acks(kThreads);

    auto record = [](int t, int b, int i) {
        return "t" + std::to_string(t) + "-b" + std::to_string(b) + "-i" + std::to_string(i) + "-" + std::string(30, 'p');
    };

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            for (int b = 0; b < kBatches; ++b) {
                std::vector<std::string> recs;
                for (int i = 0; i < kBatchSize; ++i) recs.push_back(record(t, b, i));
                auto first = log.append_batch(views_of(recs), /*flush_after=*/false);
                acks[t].push_back({t, b, first});
            }
        });
    }
    for (auto& th : threads) th.join();

    EXPECT_EQ(log.next_offset(), static_cast<std::uint64_t>(kThreads * kBatches * kBatchSize));
    for (auto& per_thread : acks) {
        for (auto& a : per_thread) {
            for (int i = 0; i < kBatchSize; ++i) {
                auto r = log.read(a.first + static_cast<std::uint64_t>(i));
                ASSERT_TRUE(r.has_value());
                EXPECT_EQ(text_of(*r), record(a.thread, a.batch, i)) << "batch interleaved at offset " << a.first + i;
            }
        }
    }
    std::filesystem::remove_all(dir);
}

TEST(BatchPublish, BackpressureLimitIsSoftForBatchesThenRecovers) {
    auto dir = fresh_dir("backpressure");
    broker::Broker broker(dir);
    auto topic = broker.create_topic("bp", 1, 1 << 20, /*max_lag_records=*/50);
    broker::Producer producer(topic);
    broker::Consumer consumer(topic, "g", 1, 0, broker.offset_store()); // registering makes lag tracked

    auto recs = make_records(40, "r");
    auto views = views_of(recs);

    auto a = producer.try_publish_batch(0, views);
    ASSERT_TRUE(a.has_value());
    EXPECT_EQ(a->first_offset, 0u);
    EXPECT_EQ(a->count, 40u);

    auto b = producer.try_publish_batch(0, views); // lag 40 < 50: admitted; lag becomes 80 (soft limit)
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(b->first_offset, 40u);

    EXPECT_FALSE(producer.try_publish_batch(0, views).has_value()); // lag 80 >= 50

    auto polled = consumer.poll(1000);
    EXPECT_EQ(polled.size(), 80u);
    consumer.commit();
    EXPECT_TRUE(producer.try_publish_batch(0, views).has_value());
    std::filesystem::remove_all(dir);
}

TEST(BatchPublish, KeyedBatchLandsOnSamePartitionAsKeyedSingle) {
    auto dir = fresh_dir("keyed");
    broker::Broker broker(dir);
    auto topic = broker.create_topic("keyed", 4);
    broker::Producer producer(topic);

    std::string one = "single";
    auto single = producer.publish_keyed("session-7", reinterpret_cast<const std::uint8_t*>(one.data()),
                                          static_cast<std::uint32_t>(one.size()));

    auto recs = make_records(5, "k");
    auto batch = producer.publish_batch_keyed("session-7", views_of(recs));

    EXPECT_EQ(batch.partition, single.partition);
    EXPECT_EQ(batch.first_offset, single.offset + 1);
    EXPECT_EQ(batch.count, 5u);
    std::filesystem::remove_all(dir);
}