#pragma once
// Cascade :: core::broker :: ConsistentHashRing
//
// Virtual-node consistent hashing: each partition gets `virtual_nodes`
// points scattered around a hash ring (not just one point per partition)
// so that when a partition is added/removed, only that partition's
// virtual-node ranges move -- keys elsewhere on the ring are unaffected.
// More virtual nodes = smoother load distribution but more memory/lookup
// cost; 100-150 per partition is a common real-world default (what
// Cassandra/DynamoDB-style systems typically use), balancing both.
//
// This does NOT replace Producer's round-robin publish() -- it's
// specifically for publish_keyed(), where minimizing key remapping on
// partition-count change is the actual goal round-robin never had.

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace cascade::core::broker {

class ConsistentHashRing {
public:
    explicit ConsistentHashRing(int virtual_nodes_per_partition = 128)
        : virtual_nodes_(virtual_nodes_per_partition) {}

    void add_partition(int partition_id) {
        for (int v = 0; v < virtual_nodes_; ++v) {
            std::uint64_t h = hash_virtual_node(partition_id, v);
            ring_[h] = partition_id;
        }
    }

    void remove_partition(int partition_id) {
        for (int v = 0; v < virtual_nodes_; ++v) {
            ring_.erase(hash_virtual_node(partition_id, v));
        }
    }

    // Returns the partition owning `key` -- the first ring point at or
    // after hash(key), wrapping around to the smallest point if key's
    // hash exceeds every point on the ring.
    int partition_for(const std::string& key) const {
        if (ring_.empty()) throw std::runtime_error("consistent hash ring has no partitions");
        std::uint64_t h = std::hash<std::string>{}(key);
        auto it = ring_.lower_bound(h);
        if (it == ring_.end()) it = ring_.begin(); // wrap around
        return it->second;
    }

    std::size_t ring_size() const { return ring_.size(); }

private:
    static std::uint64_t hash_virtual_node(int partition_id, int virtual_index) {
        std::string composite = std::to_string(partition_id) + "#vnode#" + std::to_string(virtual_index);
        return std::hash<std::string>{}(composite);
    }

    int virtual_nodes_;
    std::map<std::uint64_t, int> ring_; // sorted by hash point -> partition id
};

} // namespace cascade::core::broker