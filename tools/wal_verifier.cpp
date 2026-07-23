// Reopens a Log directory (triggering recovery) and validates every
// record from 0 to next_offset() is intact and matches the expected
// pattern written by wal_kill_writer. Prints a summary and exits non-zero
// on any inconsistency.
#include <cstdio>
#include <string>
#include "storage/log.hpp"

using namespace cascade::core::storage;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <log_dir>\n", argv[0]);
        return 1;
    }
    std::string dir = argv[1];
    Log log(dir, 1 << 20); // recovery happens here, in the constructor

    std::uint64_t recovered = log.next_offset();
    std::printf("Recovered next_offset = %llu\n", static_cast<unsigned long long>(recovered));

    int mismatches = 0;
    for (std::uint64_t i = 0; i < recovered; ++i) {
        auto result = log.read(i);
        if (!result.has_value()) {
            std::printf("MISSING offset %llu (should be present, recovery reported it as valid)\n",
                        static_cast<unsigned long long>(i));
            mismatches++;
            continue;
        }
        std::string expected_prefix = "kill-test-record-" + std::to_string(i) + "-";
        std::string actual(result->payload.begin(), result->payload.end());
        if (actual.rfind(expected_prefix, 0) != 0) {
            std::printf("MISMATCH at offset %llu\n", static_cast<unsigned long long>(i));
            mismatches++;
        }
    }

    if (mismatches == 0) {
        std::printf("OK: all %llu recovered records verified intact.\n",
                    static_cast<unsigned long long>(recovered));
        return 0;
    }
    std::printf("FAILED: %d mismatches out of %llu recovered records.\n",
                mismatches, static_cast<unsigned long long>(recovered));
    return 1;
}