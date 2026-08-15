#include <gtest/gtest.h>
#include <unordered_map>
#include <unordered_set>
#include "broker/consistent_hash.hpp"

using namespace cascade::core::broker;

TEST(ConsistentHash, SameKeyAlwaysMapsToSamePartition) {
    ConsistentHashRing ring;
    for (int p = 0; p < 4; ++p) ring.add_partition(p);

    int first = ring.partition_for("session-abc");
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(ring.partition_for("session-abc"), first);
    }
}

TEST(ConsistentHash, DistributesKeysAcrossAllPartitionsReasonablyEvenly) {
    ConsistentHashRing ring;
    for (int p = 0; p < 4; ++p) ring.add_partition(p);

    std::unordered_map<int, int> counts;
    constexpr int kKeys = 10000;
    for (int i = 0; i < kKeys; ++i) counts[ring.partition_for("key-" + std::to_string(i))]++;

    ASSERT_EQ(counts.size(), 4u);
    for (auto& [p, count] : counts) {
        double fraction = static_cast<double>(count) / kKeys;
        EXPECT_NEAR(fraction, 0.25, 0.05) << "partition " << p << " got " << count << " keys (expected ~25%)";
    }
}

// The core property consistent hashing exists for: adding a partition
// should remap only a SMALL fraction of keys, not most/all of them --
// directly contrasted against what plain modulo hashing would do.
TEST(ConsistentHash, AddingPartitionRemapsOnlyAFraction) {
    constexpr int kKeys = 10000;
    std::vector<std::string> keys;
    for (int i = 0; i < kKeys; ++i) keys.push_back("key-" + std::to_string(i));

    ConsistentHashRing ring;
    for (int p = 0; p < 4; ++p) ring.add_partition(p);

    std::unordered_map<std::string, int> before;
    for (auto& k : keys) before[k] = ring.partition_for(k);

    ring.add_partition(4); // 4 -> 5 partitions

    int remapped = 0;
    for (auto& k : keys) {
        if (ring.partition_for(k) != before[k]) remapped++;
    }
    double remapped_fraction = static_cast<double>(remapped) / kKeys;

    // Expected theoretical remap fraction going from N to N+1 partitions
    // is roughly 1/(N+1) (~20% here) -- generously bounded at 40% to
    // avoid test flakiness from hash distribution variance, while still
    // being a MUCH tighter bound than modulo's ~80% remap rate for the
    // same transition (asserted in the next test for direct comparison).
    EXPECT_LT(remapped_fraction, 0.40)
        << "remapped " << remapped << "/" << kKeys << " (" << (remapped_fraction * 100) << "%) -- too high for consistent hashing";
    std::printf("Consistent hash: adding 1 partition (4->5) remapped %.1f%% of keys\n", remapped_fraction * 100);
}

TEST(ConsistentHash, ModuloRemapsFarMoreKeysThanConsistentHashOnTheSameTransition) {
    constexpr int kKeys = 10000;
    int remapped_modulo = 0;
    for (int i = 0; i < kKeys; ++i) {
        std::size_t h = std::hash<std::string>{}("key-" + std::to_string(i));
        if (h % 4 != h % 5) remapped_modulo++;
    }
    double fraction = static_cast<double>(remapped_modulo) / kKeys;
    std::printf("Modulo hash: adding 1 partition (4->5) remapped %.1f%% of keys\n", fraction * 100);
    EXPECT_GT(fraction, 0.60); // modulo genuinely remaps most keys on this transition
}

TEST(ConsistentHash, RemovingPartitionRedistributesOnlyItsKeys) {
    ConsistentHashRing ring;
    for (int p = 0; p < 4; ++p) ring.add_partition(p);

    std::unordered_map<std::string, int> before;
    constexpr int kKeys = 5000;
    for (int i = 0; i < kKeys; ++i) {
        std::string k = "key-" + std::to_string(i);
        before[k] = ring.partition_for(k);
    }

    ring.remove_partition(2);

    for (auto& [k, old_p] : before) {
        int new_p = ring.partition_for(k);
        if (old_p != 2) {
            // Keys that weren't on partition 2 should be COMPLETELY
            // unaffected by its removal -- this is the other half of
            // consistent hashing's guarantee.
            EXPECT_EQ(new_p, old_p) << "key " << k << " moved despite not being on the removed partition";
        }
    }
}