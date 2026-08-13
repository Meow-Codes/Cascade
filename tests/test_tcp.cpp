#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include "net/epoll_loop.hpp"
#include "net/tcp_server.hpp"
#include "net/tcp_client.hpp"

using namespace cascade::core::net;
using namespace std::chrono_literals;

// Drives an EpollLoop on a background thread for the duration of a test.
class LoopRunner {
public:
    explicit LoopRunner(EpollLoop& loop) : loop_(loop), thread_([this] { run(); }) {}
    ~LoopRunner() {
        stop_.store(true);
        thread_.join();
    }
private:
    void run() {
        while (!stop_.load()) loop_.poll_once(10);
    }
    EpollLoop& loop_;
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

TEST(TcpLoopback, SendReceiveCorrectness) {
    EpollLoop loop;
    TcpServer<EpollLoop> server(loop, "127.0.0.1", 18080);

    std::atomic<bool> got_message{false};
    std::vector<std::uint8_t> received;
    server.set_on_message([&](TcpServer<EpollLoop>::ConnectionId, const std::vector<std::uint8_t>& payload) {
        received = payload;
        got_message.store(true);
    });

    LoopRunner runner(loop);

    TcpClient client;
    client.connect("127.0.0.1", 18080);
    client.send(std::string("integration test payload"));

    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!got_message.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(5ms);
    }

    ASSERT_TRUE(got_message.load());
    EXPECT_EQ(std::string(received.begin(), received.end()), "integration test payload");
}

TEST(TcpLoopback, ServerDetectsAbruptDisconnect) {
    EpollLoop loop;
    TcpServer<EpollLoop> server(loop, "127.0.0.1", 18081);

    std::atomic<bool> disconnected{false};
    server.set_on_disconnect([&](TcpServer<EpollLoop>::ConnectionId) { disconnected.store(true); });

    LoopRunner runner(loop);

    {
        TcpClient client;
        client.connect("127.0.0.1", 18081);
        client.send(std::string("hi"));
        std::this_thread::sleep_for(50ms);
    } // client destructs here -> fd closes -> server should see EOF

    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!disconnected.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_TRUE(disconnected.load());
}