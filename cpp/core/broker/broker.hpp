#pragma once
// Cascade :: core::broker :: Broker
//
// Single-node broker: owns the topic registry and the one OffsetStore
// shared by every topic's partitions. Deliberately the smallest possible
// "broker" -- no networking wiring yet (Phase 2's TcpServer plugs in once
// a wire protocol/schema is defined), just the in-process pub/sub
// semantics this phase calls for.
//
// When connected to the Go control plane, topic creation is coordinated
// through the control plane before being materialized locally. Broker
// registration and heartbeats are driven using the project's own
// ThreadPool + TimerWheel infrastructure.

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

#include "broker/offset_store.hpp"
#include "broker/topic.hpp"
#include "controlplane/control_plane_client.hpp"
#include "threadpool/thread_pool.hpp"
#include "timer/timer_wheel.hpp"

namespace cascade::core::broker {

class Broker {
public:
    explicit Broker(std::string data_dir)
        : data_dir_(std::move(data_dir)) {}

    // Optional control-plane integration.
    //
    // Existing single-node users/tests don't need to call this.
    // Once connected, the broker registers itself and periodically
    // heartbeats to the Go control plane.
    void connect_to_control_plane(
        const std::string& control_plane_address,
        const std::string& broker_id,
        const std::string& broker_data_plane_address,
        std::uint64_t heartbeat_interval_ms = 2000)
    {
        if (cp_client_) {
            throw std::runtime_error("broker already connected to control plane");
        }

        cp_pool_ = std::make_unique<ThreadPool>(1);
        cp_wheel_ = std::make_unique<TimerWheel>(64, /*tick_ms=*/100);

        cp_client_ = std::make_unique<controlplane::ControlPlaneClient>(
            control_plane_address,
            broker_id,
            broker_data_plane_address);

        cp_client_->start_heartbeating(
            *cp_pool_,
            *cp_wheel_,
            heartbeat_interval_ms);

        cp_ticker_stop_.store(false, std::memory_order_relaxed);

        cp_ticker_ = std::thread([this] {
            while (!cp_ticker_stop_.load(std::memory_order_relaxed)) {
                cp_wheel_->tick();
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }

    bool is_connected_to_control_plane() const {
        return cp_client_ != nullptr;
    }

    std::size_t missed_heartbeats() const {
        return cp_client_
            ? cp_client_->missed_heartbeats()
            : 0;
    }

    std::shared_ptr<Topic> create_topic(
        const std::string& name,
        int num_partitions,
        std::size_t max_segment_bytes = 64 * 1024 * 1024,
        std::uint64_t max_lag_records = 0,
        FlushPolicy flush_policy = FlushPolicy::PerMessage)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (topics_.count(name)) {
            throw std::runtime_error("topic already exists: " + name);
        }

        // In clustered mode, the control plane is the source of truth
        // for metadata.
        if (cp_client_) {
            auto result = cp_client_->create_topic(name, num_partitions);

            if (!result.created) {
                throw std::runtime_error(
                    "control plane rejected topic creation: " +
                    result.error);
            }
        }

        auto topic = std::make_shared<Topic>(
            name,
            num_partitions,
            data_dir_,
            max_segment_bytes,
            offset_store_,
            max_lag_records,
            flush_policy);

        topics_[name] = topic;
        return topic;
    }

    std::shared_ptr<Topic> get_topic(const std::string& name) const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = topics_.find(name);

        if (it == topics_.end()) {
            throw std::runtime_error("no such topic: " + name);
        }

        return it->second;
    }

    OffsetStore& offset_store() {
        return offset_store_;
    }

    ~Broker() {
        if (cp_client_) {
            cp_client_->stop_heartbeating();
        }

        cp_ticker_stop_.store(true, std::memory_order_relaxed);

        if (cp_ticker_.joinable()) {
            cp_ticker_.join();
        }
    }

    Broker(const Broker&) = delete;
    Broker& operator=(const Broker&) = delete;

private:
    std::string data_dir_;
    OffsetStore offset_store_;
    std::unordered_map<std::string, std::shared_ptr<Topic>> topics_;
    mutable std::mutex mutex_;

    std::unique_ptr<ThreadPool> cp_pool_;
    std::unique_ptr<TimerWheel> cp_wheel_;
    std::unique_ptr<controlplane::ControlPlaneClient> cp_client_;

    std::thread cp_ticker_;
    std::atomic<bool> cp_ticker_stop_{false};
};

} // namespace cascade::core::broker