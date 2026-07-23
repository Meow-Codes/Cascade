#pragma once
// Cascade :: core::broker :: OffsetStore
//
// Tracks, per (topic, partition, consumer group), the next offset that
// group should read on its next poll -- "committed offset = next offset
// to consume" semantics, the same convention Kafka uses. Also the source
// of truth Partition reads from to decide whether a producer should be
// backpressured.
//
// In-memory only for v1: a broker restart resets all consumer group
// progress to the beginning of each partition. Persisting this (e.g. as
// its own compacted topic -- literally how Kafka does it internally with
// __consumer_offsets) is a natural stretch goal once Phase 3's deferred
// log-compaction groundwork exists.

#include <cstdint>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>

namespace cascade::core::broker {

class OffsetStore {
public:
    void register_group(const std::string& topic, int partition, const std::string& group) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto pkey = partition_key(topic, partition);
        groups_by_partition_[pkey].insert(group);
        entries_.try_emplace(entry_key(topic, partition, group), 0); // "earliest": new groups start at offset 0
    }

    void commit(const std::string& topic, int partition, const std::string& group, std::uint64_t next_offset) {
        std::lock_guard<std::mutex> lock(mutex_);
        entries_[entry_key(topic, partition, group)] = next_offset;
    }

    std::optional<std::uint64_t> get(const std::string& topic, int partition, const std::string& group) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(entry_key(topic, partition, group));
        if (it == entries_.end()) return std::nullopt;
        return it->second;
    }

    // Minimum committed offset across all groups REGISTERED for this
    // partition. nullopt means no group has ever registered -- callers
    // (Partition's backpressure check) treat that as "no limit."
    std::optional<std::uint64_t> min_committed_offset_for(const std::string& topic, int partition) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = groups_by_partition_.find(partition_key(topic, partition));
        if (it == groups_by_partition_.end() || it->second.empty()) return std::nullopt;

        std::uint64_t min_val = UINT64_MAX;
        for (auto& group : it->second) {
            auto eit = entries_.find(entry_key(topic, partition, group));
            std::uint64_t v = (eit != entries_.end()) ? eit->second : 0;
            if (v < min_val) min_val = v;
        }
        return min_val;
    }

private:
    static std::string partition_key(const std::string& topic, int partition) {
        return topic + "#" + std::to_string(partition);
    }
    static std::string entry_key(const std::string& topic, int partition, const std::string& group) {
        return partition_key(topic, partition) + "#" + group;
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::uint64_t> entries_;
    std::unordered_map<std::string, std::set<std::string>> groups_by_partition_;
};

} // namespace cascade::core::broker