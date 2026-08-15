#pragma once
// Cascade :: core::broker :: Producer
//
// Thin client-side handle around a Topic.
//   - publish(): round-robins across partitions.
//   - publish_keyed(): hashes the key to a fixed partition, so all
//     messages sharing a key land in the same partition and are
//     therefore strictly ordered relative to each other -- the standard
//     Kafka partitioning contract.

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "broker/topic.hpp"
#include "broker/consistent_hash.hpp"

namespace cascade::core::broker {

struct PublishResult {
    int partition;
    std::uint64_t offset;
};

class Producer {
public:
    explicit Producer(std::shared_ptr<Topic> topic) : topic_(std::move(topic)) {}

    PublishResult publish(const std::uint8_t* payload, std::uint32_t len) {
        int p = static_cast<int>(round_robin_.fetch_add(1, std::memory_order_relaxed) %
                                  static_cast<std::uint64_t>(topic_->num_partitions()));
        auto offset = topic_->partition(p)->publish(payload, len);
        return {p, offset};
    }

    PublishResult publish_keyed(const std::string& key, const std::uint8_t* payload, std::uint32_t len) {
        std::size_t h = std::hash<std::string>{}(key);
        int p = static_cast<int>(h % static_cast<std::size_t>(topic_->num_partitions()));
        auto offset = topic_->partition(p)->publish(payload, len);
        return {p, offset};
    }

    // NEW: consistent-hash-based keyed publish. publish_keyed() (modulo-based)
    // is UNCHANGED and still the default -- this is an explicit opt-in for
    // callers who care about minimal key remapping across partition-count
    // changes, since it costs a bit more (ring lookup vs a modulo) and most
    // single-broker v1 usage never resizes partition count anyway.
    PublishResult publish_keyed_consistent(const std::string& key, const std::uint8_t* payload, std::uint32_t len) {
        if (!hash_ring_) {
            hash_ring_ = std::make_unique<ConsistentHashRing>();
            for (int p = 0; p < topic_->num_partitions(); ++p) hash_ring_->add_partition(p);
        }
        int p = hash_ring_->partition_for(key);
        auto offset = topic_->partition(p)->publish(payload, len);
        return {p, offset};
    }

    // Non-blocking variant so callers (and tests) can observe backpressure
    // explicitly rather than block on it.
    std::optional<PublishResult> try_publish(int partition, const std::uint8_t* payload, std::uint32_t len) {
        auto offset = topic_->partition(partition)->try_publish(payload, len);
        if (!offset.has_value()) return std::nullopt;
        return PublishResult{partition, *offset};
    }

private:
    std::shared_ptr<Topic> topic_;
    std::atomic<std::uint64_t> round_robin_{0};
    std::unique_ptr<ConsistentHashRing> hash_ring_; // lazy init on first publish_keyed_consistent()
};

} // namespace cascade::core::broker