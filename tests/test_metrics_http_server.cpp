#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <chrono>
#include <thread>
#include "metrics/http_metrics_server.hpp"
#include "metrics/metrics_registry.hpp"
#include "net/epoll_loop.hpp"

using namespace cascade::core;
using namespace std::chrono_literals;

// Raw socket GET, deliberately NOT using Phase 2's TcpClient -- that
// client speaks the length-prefix framed protocol, which is incompatible
// with plain HTTP (see design note in this phase).
static std::string raw_http_get(const std::string& path, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<std::uint16_t>(port));
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) { ::close(fd); return ""; }

    std::string req = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
    ::send(fd, req.data(), req.size(), 0);

    std::string response;
    char buf[4096];
    ssize_t n;
    while ((n = ::recv(fd, buf, sizeof(buf), 0)) > 0) response.append(buf, static_cast<std::size_t>(n));
    ::close(fd);
    return response;
}

TEST(HttpMetricsServer, ServesMetricsOnGetMetricsPath) {
    net::EpollLoop loop;
    metrics::MetricsRegistry registry;
    registry.counter("cascade_test_metric").inc(7);
    metrics::HttpMetricsServer server(loop, "127.0.0.1", 29100, registry);

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(10); });
    std::this_thread::sleep_for(50ms);

    auto response = raw_http_get("/metrics", 29100);
    stop.store(true);
    loop_thread.join();

    EXPECT_NE(response.find("200 OK"), std::string::npos);
    EXPECT_NE(response.find("cascade_test_metric 7"), std::string::npos);
}

TEST(HttpMetricsServer, ReturnsNotFoundForOtherPaths) {
    net::EpollLoop loop;
    metrics::MetricsRegistry registry;
    metrics::HttpMetricsServer server(loop, "127.0.0.1", 29101, registry);

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(10); });
    std::this_thread::sleep_for(50ms);

    auto response = raw_http_get("/", 29101);
    stop.store(true);
    loop_thread.join();

    EXPECT_NE(response.find("404"), std::string::npos);
}