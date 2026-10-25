// Long-running demo for the dashboard: a real Broker with metrics attached,
// a real HttpMetricsServer, and a synthetic workload whose consumer stalls
// for 5 s of every 20 s so backpressure is visible.
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <thread>
#include <unistd.h>

#include "broker/broker.hpp"
#include "broker/consumer.hpp"
#include "broker/producer.hpp"
#include "metrics/http_metrics_server.hpp"
#include "metrics/metrics_registry.hpp"
#include "metrics/system_metrics.hpp"
#include "net/epoll_loop.hpp"

using namespace cascade::core;
using namespace std::chrono_literals;

static std::atomic<bool> g_stop{false};

int main(int argc, char** argv) {
        auto port = static_cast<std::uint16_t>(argc > 1 ? std::atoi(argv[1]) : 9100);
    std::string cp_addrs = argc > 2 ? argv[2] : "";             // e.g. "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053"
    std::string broker_id = argc > 3 ? argv[3] : "cpp-broker-demo";
    std::signal(SIGINT, [](int) { g_stop.store(true); });

    auto dir = "/tmp/cascade_broker_demo_" + std::to_string(::getpid());
    std::filesystem::remove_all(dir);

    metrics::MetricsRegistry registry;
    broker::Broker broker(dir);
    if (!cp_addrs.empty()) {
        broker.connect_to_control_plane(cp_addrs, broker_id, "127.0.0.1:" + std::to_string(port),
                                         /*heartbeat_interval_ms=*/1000);
        std::printf("registered as '%s' with control plane %s\n", broker_id.c_str(), cp_addrs.c_str());
    }
    // Unique name: a cluster remembers topics across demo restarts.
    auto topic = broker.create_topic("demo-" + std::to_string(::getpid()), 1, 64 * 1024 * 1024,
                                      /*max_lag_records=*/2000);
    topic->partition(0)->attach_metrics(registry);
    broker::Producer producer(topic);
    broker::Consumer consumer(topic, "demo-group", 1, 0, broker.offset_store());
    consumer.attach_metrics(registry);

    net::EpollLoop loop;
    metrics::HttpMetricsServer server(loop, "127.0.0.1", port, registry);
    std::printf("metrics on http://127.0.0.1:%u/metrics (Ctrl-C to stop)\n", port);

    std::thread producer_thread([&] {
        std::vector<std::uint8_t> payload(128, 0x42);
        while (!g_stop.load()) {
            if (!producer.try_publish(0, payload.data(), static_cast<std::uint32_t>(payload.size()))) {
                std::this_thread::sleep_for(1ms); // backpressured
            }
        }
    });
    std::thread consumer_thread([&] {
        auto start = std::chrono::steady_clock::now();
        while (!g_stop.load()) {
            auto secs = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::steady_clock::now() - start).count();
            if (secs % 20 < 15) { // stalled for the last 5 s of each 20 s window
                if (!consumer.poll(500).empty()) consumer.commit();
            }
            std::this_thread::sleep_for(5ms);
        }
    });

    while (!g_stop.load()) {
        loop.poll_once(250);
        metrics::sample_system_metrics(registry);
    }
    producer_thread.join();
    consumer_thread.join();
    std::filesystem::remove_all(dir);
    return 0;
}