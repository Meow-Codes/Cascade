#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>
#include "broker/broker.hpp"

using namespace cascade::core::broker;

// Requires a real `go run ./cmd/controlplaned` running on localhost:50051
// -- this is an integration test, not a unit test, and is deliberately
// NOT part of the default `ctest` run for that reason (see CMake note
// below: registered as a separate optional target).
TEST(BrokerControlPlaneIntegration, CreateTopicPropagatesToControlPlane) {
    auto dir = std::string("/tmp/cascade_cp_integration_") + std::to_string(::getpid());
    std::filesystem::remove_all(dir);

    Broker broker(dir);
    broker.connect_to_control_plane("localhost:50051", "test-broker-" + std::to_string(::getpid()),
                                     "127.0.0.1:9300");

    std::this_thread::sleep_for(std::chrono::milliseconds(200)); // let registration complete
    EXPECT_TRUE(broker.is_connected_to_control_plane());

    ASSERT_NO_THROW(broker.create_topic("integration-topic", 2));

    std::this_thread::sleep_for(std::chrono::milliseconds(600)); // allow at least one heartbeat cycle
    EXPECT_EQ(broker.missed_heartbeats(), 0u);

    std::filesystem::remove_all(dir);
}