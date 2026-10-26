// Reopens a Log directory (triggering recovery) and validates every record
// from 0 to next_offset() is intact and matches the pattern written by
// wal_kill_writer / wal_batch_writer.
//   wal_verifier <log_dir> [ack_file [batch_size]]
// With ack_file: also requires recovered >= acknowledged (no acked record lost).
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <string>
#include <unistd.h>
#include "storage/log.hpp"

using namespace cascade::core::storage;

static std::uint64_t read_acked(const char* path) {
    int fd = ::open(path, O_RDONLY);
    if (fd < 0) return 0;
    std::uint64_t v = 0;
    if (::pread(fd, &v, sizeof(v), 0) != static_cast<ssize_t>(sizeof(v))) v = 0;
    ::close(fd);
    return v;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <log_dir> [ack_file [batch_size]]\n", argv[0]);
        return 1;
    }
    Log log(argv[1], 1 << 20); // recovery happens here

    const std::uint64_t recovered = log.next_offset();
    std::printf("Recovered next_offset = %llu\n", static_cast<unsigned long long>(recovered));

    int mismatches = 0;
    for (std::uint64_t i = 0; i < recovered; ++i) {
        auto result = log.read(i);
        if (!result.has_value()) {
            std::printf("MISSING offset %llu\n", static_cast<unsigned long long>(i));
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

    if (argc > 2) {
        const std::uint64_t acked = read_acked(argv[2]);
        std::printf("Acknowledged before kill = %llu\n", static_cast<unsigned long long>(acked));
        if (recovered < acked) {
            std::printf("FAILED: %llu acknowledged records missing after recovery\n",
                        static_cast<unsigned long long>(acked - recovered));
            mismatches++;
        }
        if (argc > 3) {
            const std::uint64_t batch = std::strtoull(argv[3], nullptr, 10);
            if (batch > 0) {
                std::printf("Recovered tail is %llu records into a batch (non-zero => a batch was partially "
                            "durable at the kill; expected, batches are not atomic)\n",
                            static_cast<unsigned long long>(recovered % batch));
            }
        }
    }

    if (mismatches == 0) {
        std::printf("OK: all %llu recovered records verified intact.\n", static_cast<unsigned long long>(recovered));
        return 0;
    }
    std::printf("FAILED: %d problems out of %llu recovered records.\n", mismatches,
                static_cast<unsigned long long>(recovered));
    return 1;
}