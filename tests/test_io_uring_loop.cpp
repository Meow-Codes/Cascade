#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <poll.h>
#include "net/io_uring_loop.hpp"
#include "net/tcp_server.hpp"
#include "net/tcp_client.hpp"

using namespace cascade::core::net;
using namespace std::chrono_literals;

class IoUringLoopRunner {
public:
    explicit IoUringLoopRunner(IoUringLoop& loop) : loop_(loop), thread_([this] { run(); }) {}
    ~IoUringLoopRunner() { stop_.store(true); thread_.join(); }
private:
    void run() { while (!stop_.load()) loop_.poll_once(10); }
    IoUringLoop& loop_;
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

TEST(IoUringLoopTcp, SendReceiveCorrectness) {
    IoUringLoop loop;
    TcpServer<IoUringLoop> server(loop, "127.0.0.1", 18090);

    std::atomic<bool> got_message{false};
    std::vector<std::uint8_t> received;
    server.set_on_message([&](TcpServer<IoUringLoop>::ConnectionId, const std::vector<std::uint8_t>& payload) {
        received = payload;
        got_message.store(true);
    });

    IoUringLoopRunner runner(loop);

    TcpClient client;
    client.connect("127.0.0.1", 18090);
    client.send(std::string("io_uring integration test"));

    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!got_message.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(5ms);
    }

    ASSERT_TRUE(got_message.load());
    EXPECT_EQ(std::string(received.begin(), received.end()), "io_uring integration test");
}

TEST(IoUringLoopTcp, ServerDetectsAbruptDisconnect) {
    IoUringLoop loop;
    TcpServer<IoUringLoop> server(loop, "127.0.0.1", 18091);

    std::atomic<bool> disconnected{false};
    server.set_on_disconnect([&](TcpServer<IoUringLoop>::ConnectionId) { disconnected.store(true); });

    IoUringLoopRunner runner(loop);
    {
        TcpClient client;
        client.connect("127.0.0.1", 18091);
        client.send(std::string("hi"));
        std::this_thread::sleep_for(50ms);
    }

    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!disconnected.load() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_TRUE(disconnected.load());
}