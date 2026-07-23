// Writes sequential records forever (until killed) so an external script
// can kill -9 this process mid-write and hand the directory to
// wal_verifier for crash-recovery validation.
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include "storage/log.hpp"

using namespace cascade::core::storage;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <log_dir>\n", argv[0]);
        return 1;
    }
    std::string dir = argv[1];
    Log log(dir, 1 << 20);

    std::uint64_t i = 0;
    while (true) {
        std::string payload = "kill-test-record-" + std::to_string(i) + "-padding-to-make-it-realistic-sized";
        log.append(reinterpret_cast<const std::uint8_t*>(payload.data()), payload.size());
        if (i % 100 == 0) log.flush();
        ++i;
        // Deliberately no sleep -- we want the writer moving fast so the
        // kill script has a good chance of landing mid-memcpy at least
        // occasionally across repeated runs.
    }
    return 0;
}