// benchmarks/bench_tcp.cpp
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>
#include <algorithm>
#include <atomic>
#include "net/epoll_loop.hpp"
#include "net/tcp_server.hpp"
#include "net/tcp_client.hpp"

using namespace cascade::core::net;
using Clock = std::chrono::steady_clock;

static void bench_connections_per_sec(int total_connections) {
    EpollLoop loop;
    TcpServer server(loop, "127.0.0.1", 28080);
    std::atomic<int> accepted{0};
    server.set_on_connect([&](TcpServer::ConnectionId, const std::string&) {
        accepted.fetch_add(1, std::memory_order_relaxed);
    });

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(5); });

    auto start = Clock::now();
    std::vector<std::thread> clients;
    for (int i = 0; i < total_connections; ++i) {
        clients.emplace_back([] {
            TcpClient c;
            c.connect("127.0.0.1", 28080);
        });
    }
    for (auto& t : clients) t.join();

    while (accepted.load() < total_connections) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    auto end = Clock::now();

    stop.store(true);
    loop_thread.join();

    double secs = std::chrono::duration<double>(end - start).count();
    std::printf("TCP connections: %d in %.4f s -> %.0f conn/sec\n",
                total_connections, secs, total_connections / secs);
}

static void bench_rtt_latency(int iterations) {
    EpollLoop loop;
    TcpServer server(loop, "127.0.0.1", 28081);
    server.set_on_message([&](TcpServer::ConnectionId id, const std::vector<std::uint8_t>& payload) {
        server.send(id, payload.data(), static_cast<std::uint32_t>(payload.size())); // echo
    });

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(5); });

    TcpClient client;
    client.connect("127.0.0.1", 28081);

    std::vector<double> latencies_us;
    latencies_us.reserve(iterations);
    std::string ping = "ping";

    for (int i = 0; i < iterations; ++i) {
        auto t0 = Clock::now();
        client.send(ping);
        auto reply = client.receive_one();
        auto t1 = Clock::now();
        (void)reply;
        latencies_us.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }

    stop.store(true);
    loop_thread.join();

    std::sort(latencies_us.begin(), latencies_us.end());
    double p50 = latencies_us[latencies_us.size() * 50 / 100];
    double p99 = latencies_us[latencies_us.size() * 99 / 100];
    std::printf("TCP loopback RTT: p50 = %.1f us, p99 = %.1f us (n=%d)\n", p50, p99, iterations);
}

int main() {
    std::printf("--- TCP networking benchmark ---\n");
    bench_connections_per_sec(2000);
    bench_rtt_latency(5000);
    return 0;
}