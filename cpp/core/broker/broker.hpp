#pragma once
// Cascade :: core::broker :: Broker
//
// Single-node broker: owns the topic registry and the one OffsetStore
// shared by every topic's partitions. Deliberately the smallest possible
// "broker" -- no networking wiring yet (Phase 2's TcpServer plugs in once
// a wire protocol/schema is defined), just the in-process pub/sub
// semantics this phase calls for.

#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "broker/offset_store.hpp"
#include "broker/topic.hpp"

namespace cascade::core::broker {

class Broker {
public:
    explicit Broker(std::string data_dir) : data_dir_(std::move(data_dir)) {}

    std::shared_ptr<Topic> create_topic(const std::string& name, int num_partitions,
                                     std::size_t max_segment_bytes = 64 * 1024 * 1024,
                                     std::uint64_t max_lag_records = 0,
                                     FlushPolicy flush_policy = FlushPolicy::PerMessage) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (topics_.count(name)) throw std::runtime_error("topic already exists: " + name);

        auto topic = std::make_shared<Topic>(name, num_partitions, data_dir_, max_segment_bytes,
                                            offset_store_, max_lag_records, flush_policy);
        topics_[name] = topic;
        return topic;
    }

    std::shared_ptr<Topic> get_topic(const std::string& name) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = topics_.find(name);
        if (it == topics_.end()) throw std::runtime_error("no such topic: " + name);
        return it->second;
    }

    OffsetStore& offset_store() { return offset_store_; }

private:
    std::string data_dir_;
    OffsetStore offset_store_;
    std::unordered_map<std::string, std::shared_ptr<Topic>> topics_;
    mutable std::mutex mutex_;
};

} // namespace cascade::core::broker