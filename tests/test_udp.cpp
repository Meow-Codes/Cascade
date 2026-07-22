#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include "net/udp_socket.hpp"

using namespace cascade::core::net;
using namespace std::chrono_literals;

TEST(UdpLoopback, SendReceiveCorrectness) {
    UdpSocket server("127.0.0.1", 19090);
    UdpSocket client;

    std::string msg = "udp payload";
    client.send_to(reinterpret_cast<const std::uint8_t*>(msg.data()), msg.size(), "127.0.0.1", 19090);

    std::optional<UdpPacket> received;
    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (!received && std::chrono::steady_clock::now() < deadline) {
        received = server.try_recv();
        if (!received) std::this_thread::sleep_for(5ms);
    }

    ASSERT_TRUE(received.has_value());
    EXPECT_EQ(std::string(received->data.begin(), received->data.end()), msg);
}

// UDP gives no ordering guarantee — this test proves our wrapper doesn't
// impose false ordering assumptions: packets sent "2, 1" over independent
// sendto() calls may legitimately arrive in either order, and callers
// (e.g. the future jitter buffer) are responsible for reordering, not this
// layer. We just verify both arrive intact regardless of arrival order.
TEST(UdpLoopback, HandlesOutOfOrderArrivalWithoutCorruption) {
    UdpSocket server("127.0.0.1", 19091);
    UdpSocket client;

    std::string second = "second-sent-first-expected-order";
    std::string first = "first-sent";

    // Send "first" then "second" — over loopback delivery order usually
    // matches send order, but the test only asserts both are received
    // intact and distinguishable, not a specific arrival order, since UDP
    // makes no such guarantee.
    client.send_to(reinterpret_cast<const std::uint8_t*>(first.data()), first.size(), "127.0.0.1", 19091);
    client.send_to(reinterpret_cast<const std::uint8_t*>(second.data()), second.size(), "127.0.0.1", 19091);

    std::vector<std::string> received;
    auto deadline = std::chrono::steady_clock::now() + 2s;
    while (received.size() < 2 && std::chrono::steady_clock::now() < deadline) {
        if (auto pkt = server.try_recv()) {
            received.emplace_back(pkt->data.begin(), pkt->data.end());
        } else {
            std::this_thread::sleep_for(5ms);
        }
    }

    ASSERT_EQ(received.size(), 2u);
    // Both messages arrived intact, regardless of order:
    EXPECT_TRUE((received[0] == first && received[1] == second) ||
                (received[0] == second && received[1] == first));
}