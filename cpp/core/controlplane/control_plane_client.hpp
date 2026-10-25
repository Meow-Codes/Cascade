#pragma once
// Cascade :: core::controlplane :: ControlPlaneClient
//
// gRPC client for the Go control plane. `control_plane_addresses` may be a
// comma-separated list ("h1:50051,h2:50052,h3:50053") for a raftd cluster:
//  - FAILED_PRECONDITION + "x-cascade-leader" trailer => switch to that
//    leader address and retry (leader-aware routing);
//  - UNAVAILABLE / DEADLINE_EXCEEDED => rotate to the next endpoint and retry
//    (leader crashed / election in progress);
//  - any other status is an application-level answer and is returned as-is.
// With a single address and a plain controlplaned this reduces to the
// original behaviour.

#include <grpcpp/grpcpp.h>
#include "cascade.grpc.pb.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iterator>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

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
    ControlPlaneClient(const std::string& control_plane_addresses, std::string broker_id,
                        std::string broker_address, const std::string& ca_cert_path = "")
        : broker_id_(std::move(broker_id)), broker_address_(std::move(broker_address)) {
        endpoints_ = split_addresses(control_plane_addresses);
        if (endpoints_.empty()) throw std::runtime_error("no control plane address given");

        if (!ca_cert_path.empty()) {
            std::ifstream file(ca_cert_path);
            if (!file.is_open()) throw std::runtime_error("failed to open CA cert file: " + ca_cert_path);
            std::string ca_cert((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
            grpc::SslCredentialsOptions ssl_opts;
            ssl_opts.pem_root_certs = ca_cert;
            creds_ = grpc::SslCredentials(ssl_opts);
        } else {
            creds_ = grpc::InsecureChannelCredentials();
        }
        connect_locked();
    }

    // Blocking; throws on failure (registration failing is not something
    // callers should silently continue past).
    void register_broker() {
        cascade::v1::RegisterBrokerRequest req;
        req.set_broker_id(broker_id_);
        req.set_address(broker_address_);
        cascade::v1::RegisterBrokerResponse resp;

        auto status = call_with_failover([&](Stub& stub, grpc::ClientContext& ctx) {
            return stub.RegisterBroker(&ctx, req, &resp);
        });
        if (!status.ok()) throw std::runtime_error("RegisterBroker RPC failed: " + status.error_message());
        if (!resp.registered()) throw std::runtime_error("control plane rejected broker registration");
        registered_ = true;
    }

    // Non-throwing: one missed heartbeat must not crash the broker.
    bool heartbeat_once() {
        cascade::v1::HeartbeatRequest req;
        req.set_broker_id(broker_id_);
        cascade::v1::HeartbeatResponse resp;

        auto status = call_with_failover([&](Stub& stub, grpc::ClientContext& ctx) {
            return stub.Heartbeat(&ctx, req, &resp);
        });
        if (!status.ok()) return false;
        if (!resp.acknowledged()) {
            // The control plane doesn't know us (state wiped / raced a
            // registration): re-register so we don't heartbeat into the void.
            try { register_broker(); } catch (...) {}
            return false;
        }
        return true;
    }

    std::vector<BrokerInfo> list_brokers() {
        cascade::v1::Empty req;
        cascade::v1::ListBrokersResponse resp;
        auto status = call_with_failover([&](Stub& stub, grpc::ClientContext& ctx) {
            return stub.ListBrokers(&ctx, req, &resp);
        });
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
        auto status = call_with_failover([&](Stub& stub, grpc::ClientContext& ctx) {
            return stub.CreateTopic(&ctx, req, &resp);
        });
        if (!status.ok()) throw std::runtime_error("CreateTopic RPC failed: " + status.error_message());
        return {resp.created(), resp.error()};
    }

    void start_heartbeating(ThreadPool& pool, TimerWheel& wheel,
                        std::uint64_t heartbeat_interval_ms) {
        if (!registered_) register_broker();
        schedule_next_heartbeat(pool, wheel, heartbeat_interval_ms);
    }

    void stop_heartbeating() { stop_.store(true, std::memory_order_relaxed); }

    std::size_t missed_heartbeats() const { return missed_heartbeats_.load(std::memory_order_relaxed); }
    bool is_registered() const { return registered_.load(); }

    std::string current_endpoint() {
        std::lock_guard<std::mutex> lock(ep_mutex_);
        return endpoints_[current_idx_];
    }

private:
    using Stub = cascade::v1::CascadeControlPlane::Stub;

    static std::vector<std::string> split_addresses(const std::string& s) {
        std::vector<std::string> out;
        std::stringstream ss(s);
        std::string item;
        while (std::getline(ss, item, ',')) {
            auto b = item.find_first_not_of(" \t");
            auto e = item.find_last_not_of(" \t");
            if (b != std::string::npos) out.push_back(item.substr(b, e - b + 1));
        }
        return out;
    }

    
    void connect_locked() {
        stub_ = cascade::v1::CascadeControlPlane::NewStub(
            grpc::CreateChannel(endpoints_[current_idx_], creds_));
    }

    std::shared_ptr<Stub> current_stub() {
        std::lock_guard<std::mutex> lock(ep_mutex_);
        return stub_;
    }

    void rotate_endpoint() {
        std::lock_guard<std::mutex> lock(ep_mutex_);
        current_idx_ = (current_idx_ + 1) % endpoints_.size();
        connect_locked();
    }

    void follow_leader(const std::string& leader_addr) {
        std::lock_guard<std::mutex> lock(ep_mutex_);
        auto it = std::find(endpoints_.begin(), endpoints_.end(), leader_addr);
        std::size_t idx;
        if (it == endpoints_.end()) {
            endpoints_.push_back(leader_addr);
            idx = endpoints_.size() - 1;
        } else {
            idx = static_cast<std::size_t>(it - endpoints_.begin());
        }
        if (idx != current_idx_) {
            current_idx_ = idx;
            connect_locked();
        }
    }

    template <typename Fn>
    grpc::Status call_with_failover(Fn&& rpc) {
        constexpr int kMaxAttempts = 12;
        grpc::Status status;
        for (int attempt = 0; attempt < kMaxAttempts; ++attempt) {
            auto stub = current_stub();
            grpc::ClientContext ctx;
            ctx.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);
            status = rpc(*stub, ctx);
            if (status.ok()) return status;

            const auto code = status.error_code();
            if (code == grpc::StatusCode::FAILED_PRECONDITION) {
                const auto& trailers = ctx.GetServerTrailingMetadata();
                auto it = trailers.find("x-cascade-leader");
                if (it != trailers.end()) {
                    follow_leader(std::string(it->second.data(), it->second.size()));
                    continue; // straight to the leader, no sleep
                }
                rotate_endpoint(); // not leader and no hint known yet
            } else if (code == grpc::StatusCode::UNAVAILABLE ||
                       code == grpc::StatusCode::DEADLINE_EXCEEDED) {
                rotate_endpoint();
            } else {
                return status; // application-level error: don't retry
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return status;
    }

    void schedule_next_heartbeat(ThreadPool& pool, TimerWheel& wheel,
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

    std::mutex ep_mutex_;
    std::vector<std::string> endpoints_;
    std::size_t current_idx_ = 0;
    std::shared_ptr<grpc::ChannelCredentials> creds_;
    std::shared_ptr<Stub> stub_;

    std::chrono::milliseconds rpc_timeout_{1500};
    std::atomic<bool> registered_{false};
    std::atomic<bool> stop_{false};
    std::atomic<std::size_t> missed_heartbeats_{0};
};

} // namespace cascade::core::controlplane