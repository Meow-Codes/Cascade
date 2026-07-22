#pragma once
// Cascade :: core::net :: EpollLoop
//
// Single-threaded epoll reactor. Deliberately single-threaded: multiple
// threads calling epoll_wait() on the same instance is legal but adds
// synchronization complexity for callback dispatch that isn't worth it at
// this stage — the roadmap's concurrency budget goes to the thread pool
// (Phase 1) processing work *handed off* by this loop, not the loop itself.
// This mirrors how nginx/Redis/most reactors scale: N single-threaded
// reactors (one per core) rather than one multi-threaded reactor.
//
// Level-triggered mode (EPOLLET not set) deliberately for v1 — edge-
// triggered requires every callback to loop until EAGAIN or risk starving
// events, which is a correctness footgun worth deferring until the rest of
// the networking stack is proven. Documented as a Phase 12 stretch item.

#include <sys/epoll.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "net/socket_fd.hpp"

namespace cascade::core::net {

class EpollLoop {
public:
    using Callback = std::function<void(std::uint32_t events)>;

    EpollLoop() {
        int fd = ::epoll_create1(0);
        if (fd < 0) throw std::runtime_error(std::string("epoll_create1 failed: ") + std::strerror(errno));
        epoll_fd_ = FileDescriptor(fd);
    }

    // events: bitmask of EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP etc.
    void add(int fd, std::uint32_t events, Callback cb) {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_ADD, fd, &ev) < 0) {
            throw std::runtime_error(std::string("epoll_ctl(ADD) failed: ") + std::strerror(errno));
        }
        callbacks_[fd] = std::move(cb);
    }

    void modify(int fd, std::uint32_t events) {
        epoll_event ev{};
        ev.events = events;
        ev.data.fd = fd;
        if (::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_MOD, fd, &ev) < 0) {
            throw std::runtime_error(std::string("epoll_ctl(MOD) failed: ") + std::strerror(errno));
        }
    }

    void remove(int fd) {
        ::epoll_ctl(epoll_fd_.get(), EPOLL_CTL_DEL, fd, nullptr); // ignore ENOENT etc: idempotent remove
        callbacks_.erase(fd);
    }

    // Blocks up to timeout_ms waiting for events, dispatches callbacks for
    // whatever fired, then returns. Call this in a loop from your own
    // thread — kept as a single step (not an internal run loop) so callers
    // can interleave other periodic work (e.g. TimerWheel::tick()) between
    // iterations without needing a second thread.
    void poll_once(int timeout_ms = -1) {
        constexpr int kMaxEvents = 256;
        epoll_event events[kMaxEvents];

        int n = ::epoll_wait(epoll_fd_.get(), events, kMaxEvents, timeout_ms);
        if (n < 0) {
            if (errno == EINTR) return; // interrupted by signal, not an error
            throw std::runtime_error(std::string("epoll_wait failed: ") + std::strerror(errno));
        }

        for (int i = 0; i < n; ++i) {
            auto it = callbacks_.find(events[i].data.fd);
            if (it != callbacks_.end()) {
                it->second(events[i].events);
            }
        }
    }

    std::size_t registered_count() const { return callbacks_.size(); }

private:
    FileDescriptor epoll_fd_;
    std::unordered_map<int, Callback> callbacks_;
};

} // namespace cascade::core::net