#pragma once
// Cascade :: core::broker :: Consumer
//
// One consumer within a consumer group. Partition assignment for v1 is
// static: partitions go to group members by
// (partition_index % group_size == consumer_index). Deliberately simpler
// than Kafka's rebalance protocol (range/sticky/cooperative assignors
// reacting to members joining/leaving) -- rebalancing is a distributed-
// coordination problem that belongs with Phase 8's cluster membership,
// not a single-broker v1. The guarantee that matters for THIS phase --
// "no double delivery within a group" -- holds regardless: static modulo
// assignment still ensures each partition has exactly one owning
// consumer in the group.
//
// poll()/commit() are separate calls (no auto-commit) so a consumer can
// poll, process, and only commit after processing succeeds -- standard
// at-least-once semantics. This is also what makes the backpressure test
// deterministic: a consumer that polls but never commits looks exactly
// like a stalled/slow consumer to the broker.

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "broker/offset_store.hpp"
#include "broker/topic.hpp"
#include "metrics/metrics_registry.hpp"


namespace cascade::core::broker {

struct PolledRecord {
    int partition;
    std::uint64_t offset;
    std::vector<std::uint8_t> payload;
};

class Consumer {
public:
    Consumer(std::shared_ptr<Topic> topic, std::string group, int group_size, int consumer_index,
             OffsetStore& offset_store)
        : topic_(std::move(topic)), group_(std::move(group)), offset_store_(offset_store) {
        for (int p = 0; p < topic_->num_partitions(); ++p) {
            if (p % group_size == consumer_index) {
                offset_store_.register_group(topic_->name(), p, group_);
                auto committed = offset_store_.get(topic_->name(), p, group_);
                cursors_[p] = committed.value_or(0);
            }
        }
    }
    
    void attach_metrics(metrics::MetricsRegistry& registry) { metrics_ = &registry; }

    // Reads up to max_records total across all assigned partitions,
    // advancing in-memory cursors (not yet persisted -- call commit()
    // for that). Returns fewer than max_records if partitions run dry.
    std::vector<PolledRecord> poll(std::size_t max_records = 100) {
        std::vector<PolledRecord> results;
        for (auto& [partition_index, cursor] : cursors_) {
            auto partition = topic_->partition(partition_index);
            while (results.size() < max_records) {
                auto rec = partition->read(cursor);
                if (!rec.has_value()) break;
                results.push_back({partition_index, rec->offset, std::move(rec->payload)});
                cursor++;
            }
            if (results.size() >= max_records) break;
        }
        if (metrics_ && !results.empty()) metrics_->counter("cascade_consume_total").inc(results.size());
        return results;
    }

    void commit() {
        for (auto& [partition_index, cursor] : cursors_) {
            offset_store_.commit(topic_->name(), partition_index, group_, cursor);
        }
    }

    const std::unordered_map<int, std::uint64_t>& cursors() const { return cursors_; }

private:
    std::shared_ptr<Topic> topic_;
    std::string group_;
    OffsetStore& offset_store_;
    std::unordered_map<int, std::uint64_t> cursors_;
    metrics::MetricsRegistry* metrics_ = nullptr;
};

} // namespace cascade::core::broker