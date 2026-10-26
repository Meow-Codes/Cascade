// Appends batches forever, flushing once per batch. After each batch is
// ACKNOWLEDGED (append_batch returned, i.e. flushed) it overwrites ack_file
// with the number of records acknowledged so far. Run it, kill -9 it, then
// check with wal_verifier <dir> <ack_file> <batch_size>.
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <string>
#include <unistd.h>
#include <vector>
#include "storage/log.hpp"

using namespace cascade::core::storage;

int main(int argc, char** argv) {
    if (argc < 4) {
        std::fprintf(stderr, "usage: %s <log_dir> <batch_size> <ack_file>\n", argv[0]);
        return 1;
    }
    const std::string dir = argv[1];
    const std::size_t batch = std::strtoull(argv[2], nullptr, 10);
    int ack_fd = ::open(argv[3], O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (batch == 0 || ack_fd < 0) return 1;

    Log log(dir, 1 << 20); // small segments: kills also land during rollovers
    std::uint64_t next = 0;
    std::vector<std::string> bufs(batch);
    std::vector<PayloadView> views(batch);

    while (true) {
        for (std::size_t i = 0; i < batch; ++i) {
            bufs[i] = "kill-test-record-" + std::to_string(next + i) + "-padding-to-make-it-realistic-sized";
            views[i] = PayloadView(reinterpret_cast<const std::uint8_t*>(bufs[i].data()), bufs[i].size());
        }
        const std::uint64_t first = log.append_batch(views, true);
        if (first != next) {
            std::fprintf(stderr, "offset mismatch: expected %llu got %llu\n",
                         static_cast<unsigned long long>(next), static_cast<unsigned long long>(first));
            return 2;
        }
        next += batch;
        const std::uint64_t acked = next;
        if (::pwrite(ack_fd, &acked, sizeof(acked), 0) != static_cast<ssize_t>(sizeof(acked))) return 3;
    }
}