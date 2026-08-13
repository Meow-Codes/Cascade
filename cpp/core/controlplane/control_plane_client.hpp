#pragma once
// Cascade :: core::controlplane :: ControlPlaneClient
//
// Thin C++ gRPC client for the Go control plane's CascadeControlPlane
// service (Phase 8). This is the missing half of the architecture claim
// from the roadmap: "C++23 owns the hot path... Go owns the control
// plane." Everything here is registration/heartbeat/admin traffic --
// low-frequency, latency-insensitive -- which is exactly why it's
// acceptable for this to go through gRPC/protobuf serialization overhead
// that would never be tolerated on the Phase 2-6 hot path (TCP/UDP
// framing, storage, media).
//
// Heartbeating is driven by Phase 1's ThreadPool + TimerWheel rather
// than a dedicated sleep-loop thread -- reuses infrastructure this
// project already built and tested, instead of introducing a third way
// to schedule periodic work.

#include <grpcpp/grpcpp.h>
#include "cascade.grpc.pb.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>

#include "threadpool/thread_pool.hpp"
#include "timer/timer_wheel.hpp"

namespace cascade::core::controlplane {

struct BrokerInfo {
    std::string broker_id;
    std::string address;
    bool alive;
    std::int64_t last_heartbeat_unix_ms;
};

class ControlPlaneClient {
public:
    ControlPlaneClient(const std::string& control_plane_address, std::string broker_id, std::string broker_address)
        : broker_id_(std::move(broker_id)), broker_address_(std::move(broker_address)) {
        auto channel = grpc::CreateChannel(control_plane_address, grpc::InsecureChannelCredentials());
        stub_ = cascade::v1::CascadeControlPlane::NewStub(channel);
    }

    // Blocking. Throws on transport failure or explicit rejection --
    // registration failing is not a condition callers should silently
    // continue past, unlike a single missed heartbeat.
    void register_broker() {
        cascade::v1::RegisterBrokerRequest req;
        req.set_broker_id(broker_id_);
        req.set_address(broker_address_);

        cascade::v1::RegisterBrokerResponse resp;
        grpc::ClientContext ctx;
        auto status = stub_->RegisterBroker(&ctx, req, &resp);
        if (!status.ok()) {
            throw std::runtime_error("RegisterBroker RPC failed: " + status.error_message());
        }
        if (!resp.registered()) {
            throw std::runtime_error("control plane rejected broker registration");
        }
        registered_ = true;
    }

    // Non-throwing: a single missed heartbeat is expected to happen
    // occasionally (transient network blip) and shouldn't crash the
    // broker. Returns false on failure so callers can log/count it.
    bool heartbeat_once() {
        cascade::v1::HeartbeatRequest req;
        req.set_broker_id(broker_id_);
        cascade::v1::HeartbeatResponse resp;
        grpc::ClientContext ctx;
        auto status = stub_->Heartbeat(&ctx, req, &resp);
        return status.ok() && resp.acknowledged();
    }

    std::vector<BrokerInfo> list_brokers() {
        cascade::v1::Empty req;
        cascade::v1::ListBrokersResponse resp;
        grpc::ClientContext ctx;
        auto status = stub_->ListBrokers(&ctx, req, &resp);
        if (!status.ok()) throw std::runtime_error("ListBrokers RPC failed: " + status.error_message());

        std::vector<BrokerInfo> out;
        out.reserve(static_cast<std::size_t>(resp.brokers_size()));
        for (auto& b : resp.brokers()) {
            out.push_back({b.broker_id(), b.address(), b.alive(), b.last_heartbeat_unix_ms()});
        }
        return out;
    }

    struct CreateTopicResult { bool created; std::string error; };
    CreateTopicResult create_topic(const std::string& name, int num_partitions) {
        cascade::v1::CreateTopicRequest req;
        req.set_name(name);
        req.set_num_partitions(num_partitions);
        cascade::v1::CreateTopicResponse resp;
        grpc::ClientContext ctx;
        auto status = stub_->CreateTopic(&ctx, req, &resp);
        if (!status.ok()) throw std::runtime_error("CreateTopic RPC failed: " + status.error_message());
        return {resp.created(), resp.error()};
    }

    // Starts periodic heartbeating via the given ThreadPool/TimerWheel,
    // re-arming itself after every heartbeat_interval_ms. Caller owns the
    // pool/wheel lifetime and must keep ticking the wheel (see Phase 2's
    // ConnectionManager for the same pattern) for heartbeats to actually
    // fire. Automatically registers first if not already registered.
    void start_heartbeating(cascade::core::ThreadPool& pool, cascade::core::TimerWheel& wheel,
                             std::uint64_t heartbeat_interval_ms) {
        if (!registered_) register_broker();
        schedule_next_heartbeat(pool, wheel, heartbeat_interval_ms);
    }

    void stop_heartbeating() { stop_.store(true, std::memory_order_relaxed); }

    std::size_t missed_heartbeats() const { return missed_heartbeats_.load(std::memory_order_relaxed); }
    bool is_registered() const { return registered_; }

private:
    void schedule_next_heartbeat(cascade::core::ThreadPool& pool, cascade::core::TimerWheel& wheel,
                                  std::uint64_t interval_ms) {
        if (stop_.load(std::memory_order_relaxed)) return;
        wheel.add_timer(interval_ms, [this, &pool, &wheel, interval_ms] {
            pool.submit([this] {
                if (!heartbeat_once()) missed_heartbeats_.fetch_add(1, std::memory_order_relaxed);
            });
            schedule_next_heartbeat(pool, wheel, interval_ms);
        });
    }

    std::string broker_id_;
    std::string broker_address_;
    std::unique_ptr<cascade::v1::CascadeControlPlane::Stub> stub_;
    bool registered_ = false;
    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> missed_heartbeats_{0};
};

} // namespace cascade::core::controlplane