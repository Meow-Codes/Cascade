#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>
#include "net/epoll_loop.hpp"
#include "net/io_uring_loop.hpp"
#include "net/tcp_server.hpp"
#include "net/tcp_client.hpp"

using namespace cascade::core::net;
using Clock = std::chrono::steady_clock;

template <typename LoopT>
static double bench_connections_per_sec(LoopT& loop, std::uint16_t port, int total_connections) {
    TcpServer<LoopT> server(loop, "127.0.0.1", port);
    std::atomic<int> accepted{0};
    server.set_on_connect([&](typename TcpServer<LoopT>::ConnectionId, const std::string&) {
        accepted.fetch_add(1, std::memory_order_relaxed);
    });

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(5); });

    auto start = Clock::now();
    std::vector<std::thread> clients;
    for (int i = 0; i < total_connections; ++i) {
        clients.emplace_back([port] { TcpClient c; c.connect("127.0.0.1", port); });
    }
    for (auto& t : clients) t.join();
    while (accepted.load() < total_connections) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    auto end = Clock::now();

    stop.store(true);
    loop_thread.join();

    double secs = std::chrono::duration<double>(end - start).count();
    return total_connections / secs;
}

template <typename LoopT>
static void bench_rtt_latency(LoopT& loop, std::uint16_t port, int iterations, double& p50, double& p99) {
    TcpServer<LoopT> server(loop, "127.0.0.1", port);
    server.set_on_message([&](typename TcpServer<LoopT>::ConnectionId id, const std::vector<std::uint8_t>& payload) {
        server.send(id, payload.data(), static_cast<std::uint32_t>(payload.size()));
    });

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(5); });

    TcpClient client;
    client.connect("127.0.0.1", port);

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
    p50 = latencies_us[latencies_us.size() * 50 / 100];
    p99 = latencies_us[latencies_us.size() * 99 / 100];
}

int main() {
    std::printf("--- epoll vs io_uring (POLL_ADD mode) comparison ---\n\n");

    {
        EpollLoop loop;
        double cps = bench_connections_per_sec(loop, 28090, 2000);
        std::printf("epoll:     %.0f connections/sec\n", cps);
    }
    {
        IoUringLoop loop;
        double cps = bench_connections_per_sec(loop, 28091, 2000);
        std::printf("io_uring:  %.0f connections/sec\n", cps);
    }

    std::printf("\n");

    {
        EpollLoop loop;
        double p50, p99;
        bench_rtt_latency(loop, 28092, 5000, p50, p99);
        std::printf("epoll:     RTT p50=%.1fus p99=%.1fus\n", p50, p99);
    }
    {
        IoUringLoop loop;
        double p50, p99;
        bench_rtt_latency(loop, 28093, 5000, p50, p99);
        std::printf("io_uring:  RTT p50=%.1fus p99=%.1fus\n", p50, p99);
    }

    std::printf("\nNote: io_uring here uses IORING_OP_POLL_ADD (readiness notification,\n");
    std::printf("same model as epoll) -- NOT direct READ/WRITE SQEs. This measures\n");
    std::printf("notification-mechanism overhead only, not io_uring's deeper zero-\n");
    std::printf("syscall-per-op potential.\n");
    return 0;
}