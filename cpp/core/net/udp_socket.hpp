#pragma once
// Cascade :: core::net :: UdpSocket
//
// UDP is connectionless and message-boundary-preserving by nature (one
// recvfrom() == one sent datagram, unlike TCP's byte stream), so no
// framing layer is needed here. This wrapper is deliberately thin —
// jitter buffering, reordering, and loss recovery are Phase 6/7 concerns
// layered on top, not part of the raw socket wrapper.

#include <netinet/in.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <vector>

#include "net/socket_fd.hpp"
#include "net/socket_utils.hpp"

namespace cascade::core::net {

struct UdpPacket {
    std::vector<std::uint8_t> data;
    sockaddr_in from{};
};

class UdpSocket {
public:
    // bind_ip/port empty+0 => ephemeral client socket (not bound to a fixed port)
    UdpSocket(const std::string& bind_ip = "", std::uint16_t port = 0) {
        int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (fd < 0) throw std::runtime_error(std::string("socket() failed: ") + std::strerror(errno));
        fd_ = FileDescriptor(fd);

        if (port != 0 || !bind_ip.empty()) {
            set_reuseaddr(fd_.get());
            auto addr = make_ipv4_addr(bind_ip, port);
            if (::bind(fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
                throw std::runtime_error(std::string("bind() failed: ") + std::strerror(errno));
            }
        }
        set_nonblocking(fd_.get());
    }

    void send_to(const std::uint8_t* data, std::size_t len, const std::string& ip, std::uint16_t port) {
        auto addr = make_ipv4_addr(ip, port);
        ssize_t n = ::sendto(fd_.get(), data, len, 0, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            throw std::runtime_error(std::string("sendto() failed: ") + std::strerror(errno));
        }
    }

    // Non-blocking receive. Returns nullopt if no datagram is currently
    // available (caller should be driven by an EpollLoop EPOLLIN callback
    // in real usage, or poll in a loop in tests).
    std::optional<UdpPacket> try_recv(std::size_t max_size = 65536) {
        UdpPacket pkt;
        pkt.data.resize(max_size);
        socklen_t from_len = sizeof(pkt.from);

        ssize_t n = ::recvfrom(fd_.get(), pkt.data.data(), max_size, 0,
                                reinterpret_cast<sockaddr*>(&pkt.from), &from_len);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return std::nullopt;
            if (errno == EINTR) return std::nullopt;
            throw std::runtime_error(std::string("recvfrom() failed: ") + std::strerror(errno));
        }
        pkt.data.resize(static_cast<std::size_t>(n));
        return pkt;
    }

    int fd() const { return fd_.get(); }

private:
    FileDescriptor fd_;
};

} // namespace cascade::core::net