#pragma once
// Cascade :: core::net :: zero_copy_transfer
//
// Wraps sendfile(2): kernel-space copy directly from a file's page cache
// to a socket buffer, skipping the userspace round-trip a normal
// read()+send() pair requires. Natural fit for consumers catching up on
// already-durable Segment data (Phase 3) -- exactly the "bulk historical
// read" case sendfile is designed for, as opposed to the low-latency
// small-message hot path (framing/broker/media), which stays on the
// existing recv()/send() path unchanged.
//
// HONEST SCOPE NOTE: sendfile() requires a real file descriptor as the
// input -- it cannot zero-copy from an arbitrary in-memory buffer. This
// is why it's scoped specifically to Segment file transfer, not a
// general replacement for TcpServer::send(); those two remain separate,
// intentionally, rather than trying to force one code path to serve
// both cases.

#include <sys/sendfile.h>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <stdexcept>

namespace cascade::core::net {

// Sends exactly `count` bytes from in_fd starting at `offset` to out_fd
// (a socket), looping over partial sendfile() calls as needed. offset is
// updated in place, matching sendfile(2)'s own semantics, so callers can
// tell how far a transfer got if it's interrupted partway (e.g. by the
// socket becoming non-blocking-would-block on a non-blocking fd).
inline std::size_t send_file_range(int out_fd, int in_fd, off_t* offset, std::size_t count) {
    std::size_t total_sent = 0;
    while (total_sent < count) {
        std::size_t remaining = count - total_sent;
        ssize_t n = ::sendfile(out_fd, in_fd, offset, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break; // non-blocking socket: caller retries later
            throw std::runtime_error(std::string("sendfile failed: ") + std::strerror(errno));
        }
        if (n == 0) break; // in_fd exhausted before count bytes were available
        total_sent += static_cast<std::size_t>(n);
    }
    return total_sent;
}

} // namespace cascade::core::net