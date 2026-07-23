#pragma once
// Cascade :: core::broker :: Topic
//
// A named collection of Partitions. Partition count is fixed at topic
// creation for v1 -- no dynamic repartitioning, since existing keyed
// messages would need to migrate to new partition assignments, which
// only makes sense once Phase 8+'s cluster coordination exists. Each
// partition gets its own subdirectory so Phase 3's per-partition Log
// layout stays clean.

#include <memory>
#include <string>
#include <vector>

#include "broker/offset_store.hpp"
#include "broker/partition.hpp"

namespace cascade::core::broker {

class Topic {
public:
    Topic(std::string name, int num_partitions, std::string base_dir, std::size_t max_segment_bytes,
          OffsetStore& offset_store, std::uint64_t max_lag_records)
        : name_(std::move(name)) {
        partitions_.reserve(static_cast<std::size_t>(num_partitions));
        for (int i = 0; i < num_partitions; ++i) {
            std::string dir = base_dir + "/" + name_ + "/partition-" + std::to_string(i);
            partitions_.push_back(std::make_shared<Partition>(name_, i, dir, max_segment_bytes,
                                                                offset_store, max_lag_records));
        }
    }

    int num_partitions() const { return static_cast<int>(partitions_.size()); }
    std::shared_ptr<Partition> partition(int index) const { return partitions_.at(static_cast<std::size_t>(index)); }
    const std::string& name() const { return name_; }

private:
    std::string name_;
    std::vector<std::shared_ptr<Partition>> partitions_;
};

} // namespace cascade::core::broker