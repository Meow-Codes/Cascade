// Manual/CI integration check: registers a fake C++ broker with the Go
// control plane, heartbeats a few times via the real ThreadPool+TimerWheel
// path, then lists brokers and prints what the control plane sees.
// Requires `go run ./cmd/controlplaned` running separately on :50051.
#include <chrono>
#include <cstdio>
#include <thread>

#include "controlplane/control_plane_client.hpp"
#include "threadpool/thread_pool.hpp"
#include "timer/timer_wheel.hpp"

using namespace cascade::core;

int main(int argc, char** argv) {
    std::string address = (argc > 1) ? argv[1] : "localhost:50051";
    std::string broker_id = (argc > 2) ? argv[2] : "cpp-broker-1";

    controlplane::ControlPlaneClient client(address, broker_id, "127.0.0.1:9200");

    cascade::core::ThreadPool pool(2);
    cascade::core::TimerWheel wheel(64, /*tick_ms=*/50);

    std::printf("Registering broker '%s' with control plane at %s...\n", broker_id.c_str(), address.c_str());
    client.start_heartbeating(pool, wheel, /*heartbeat_interval_ms=*/500);
    std::printf("Registered. Heartbeating every 500ms.\n");

    // Drive the timer wheel from this thread -- same pattern as
    // ConnectionManager's tests (Phase 2).
    std::thread ticker([&] {
        for (int i = 0; i < 100; ++i) { // ~5 seconds at 50ms/tick
            wheel.tick();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });
    ticker.join();

    client.stop_heartbeating();
    std::printf("Missed heartbeats: %zu\n", client.missed_heartbeats());

    auto brokers = client.list_brokers();
    std::printf("Control plane sees %zu broker(s):\n", brokers.size());
    for (auto& b : brokers) {
        std::printf("  id=%s address=%s alive=%s last_heartbeat_unix_ms=%lld\n",
                    b.broker_id.c_str(), b.address.c_str(), b.alive ? "true" : "false",
                    static_cast<long long>(b.last_heartbeat_unix_ms));
    }

    bool ok = !brokers.empty() && brokers[0].broker_id == broker_id && brokers[0].alive;
    std::printf(ok ? "OK: broker registered and alive per control plane.\n"
                    : "FAILED: broker not found or not alive.\n");
    return ok ? 0 : 1;
}