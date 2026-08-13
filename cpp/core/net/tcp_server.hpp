#pragma once
// Cascade :: core::net :: TcpServer
//
// Non-blocking TCP server built on EpollLoop. Owns accepted connections,
// handles partial reads via FrameDecoder, and hands complete frames to a
// user-supplied callback. Deliberately does NOT own a thread — caller
// drives it via poll_once() in their own event loop thread, consistent
// with EpollLoop's design.

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <functional>
#include <stdexcept>
#include <unordered_map>

#include "net/epoll_loop.hpp"
#include "net/framing.hpp"
#include "net/socket_fd.hpp"
#include "net/socket_utils.hpp"

namespace cascade::core::net {

template <typename LoopT>
class TcpServer {
public:
    using ConnectionId = int; // fd doubles as connection id for v1 simplicity
    using OnConnect = std::function<void(ConnectionId, const std::string& peer_addr)>;
    using OnMessage = std::function<void(ConnectionId, const std::vector<std::uint8_t>& payload)>;
    using OnDisconnect = std::function<void(ConnectionId)>;

    TcpServer(LoopT& loop, const std::string& bind_ip, std::uint16_t port)
        : loop_(loop) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error(std::string("socket() failed: ") + std::strerror(errno));
        listen_fd_ = FileDescriptor(fd);

        set_reuseaddr(listen_fd_.get());
        auto addr = make_ipv4_addr(bind_ip, port);
        if (::bind(listen_fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            throw std::runtime_error(std::string("bind() failed: ") + std::strerror(errno));
        }
        if (::listen(listen_fd_.get(), SOMAXCONN) < 0) {
            throw std::runtime_error(std::string("listen() failed: ") + std::strerror(errno));
        }
        set_nonblocking(listen_fd_.get());

        loop_.add(listen_fd_.get(), EPOLLIN, [this](std::uint32_t) { accept_loop(); });
    }

    ~TcpServer() {
        for (auto& [fd, conn] : connections_) {
            loop_.remove(fd);
            ::close(fd);
        }
        loop_.remove(listen_fd_.get());
    }

    void set_on_connect(OnConnect cb) { on_connect_ = std::move(cb); }
    void set_on_message(OnMessage cb) { on_message_ = std::move(cb); }
    void set_on_disconnect(OnDisconnect cb) { on_disconnect_ = std::move(cb); }

    void send(ConnectionId id, const std::uint8_t* payload, std::uint32_t len) {
        auto it = connections_.find(id);
        if (it == connections_.end()) return; // silently drop: connection already gone

        auto frame = encode_frame(payload, len);
        // v1: blocking-ish write via non-blocking fd + retry loop. Good
        // enough for control-plane message sizes; a real backpressure-aware
        // write buffer is a Phase 12 optimization, not a v1 requirement.
        std::size_t sent = 0;
        while (sent < frame.size()) {
            ssize_t n = ::send(id, frame.data() + sent, frame.size() - sent, 0);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                disconnect(id);
                return;
            }
            sent += static_cast<std::size_t>(n);
        }
    }

    std::size_t connection_count() const { return connections_.size(); }

private:
    struct Connection {
        FrameDecoder decoder;
        sockaddr_in peer_addr{};
    };

    void accept_loop() {
        while (true) {
            sockaddr_in peer{};
            socklen_t peer_len = sizeof(peer);
            int fd = ::accept(listen_fd_.get(), reinterpret_cast<sockaddr*>(&peer), &peer_len);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return; // no more pending connections
                if (errno == EINTR) continue;
                return; // other errors: give up this pass, try again next epoll wakeup
            }

            set_nonblocking(fd);
            set_tcp_nodelay(fd);

            connections_[fd] = Connection{FrameDecoder{}, peer};
            loop_.add(fd, EPOLLIN, [this, fd](std::uint32_t events) { handle_readable(fd, events); });

            if (on_connect_) on_connect_(fd, addr_to_string(peer));
        }
    }

    void handle_readable(int fd, std::uint32_t events) {
        if (events & (EPOLLERR | EPOLLHUP)) {
            disconnect(fd);
            return;
        }

        std::uint8_t buf[65536];
        while (true) {
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                auto it = connections_.find(fd);
                if (it == connections_.end()) return;
                it->second.decoder.feed(buf, static_cast<std::size_t>(n));

                try {
                    while (auto frame = it->second.decoder.try_extract()) {
                        if (on_message_) on_message_(fd, *frame);
                    }
                } catch (const std::exception&) {
                    disconnect(fd); // malformed frame: drop the connection, don't crash
                    return;
                }
            } else if (n == 0) {
                disconnect(fd); // peer closed cleanly
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return; // drained for now
                if (errno == EINTR) continue;
                disconnect(fd);
                return;
            }
        }
    }

    void disconnect(int fd) {
        if (connections_.erase(fd) == 0) return; // already disconnected
        loop_.remove(fd);
        ::close(fd);
        if (on_disconnect_) on_disconnect_(fd);
    }

    LoopT& loop_;
    FileDescriptor listen_fd_;
    std::unordered_map<int, Connection> connections_;

    OnConnect on_connect_;
    OnMessage on_message_;
    OnDisconnect on_disconnect_;
};

} // namespace cascade::core::net