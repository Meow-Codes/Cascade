#pragma once
// Cascade :: core::net :: TcpClient
//
// Simple TCP client. Offers both a blocking connect (fine for control-
// plane setup, which happens once per session) and non-blocking
// send/receive suitable for driving from an EpollLoop.

#include <netinet/in.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include "net/framing.hpp"
#include "net/socket_fd.hpp"
#include "net/socket_utils.hpp"

namespace cascade::core::net {

class TcpClient {
public:
    TcpClient() = default;

    void connect(const std::string& ip, std::uint16_t port) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error(std::string("socket() failed: ") + std::strerror(errno));
        fd_ = FileDescriptor(fd);

        auto addr = make_ipv4_addr(ip, port);
        if (::connect(fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            throw std::runtime_error(std::string("connect() failed: ") + std::strerror(errno));
        }
        set_tcp_nodelay(fd_.get());
    }

    void send(const std::uint8_t* payload, std::uint32_t len) {
        auto frame = encode_frame(payload, len);
        std::size_t sent = 0;
        while (sent < frame.size()) {
            ssize_t n = ::send(fd_.get(), frame.data() + sent, frame.size() - sent, 0);
            if (n < 0) {
                if (errno == EINTR) continue;
                throw std::runtime_error(std::string("send() failed: ") + std::strerror(errno));
            }
            sent += static_cast<std::size_t>(n);
        }
    }

    void send(const std::string& payload) {
        send(reinterpret_cast<const std::uint8_t*>(payload.data()),
             static_cast<std::uint32_t>(payload.size()));
    }

    // Blocking receive of exactly one framed message. Fine for tests and
    // simple request/response flows; a production client driven by
    // EpollLoop would instead feed recv() bytes into a FrameDecoder
    // asynchronously the same way TcpServer does.
    std::vector<std::uint8_t> receive_one() {
        FrameDecoder decoder;
        std::uint8_t buf[65536];
        while (true) {
            if (auto frame = decoder.try_extract()) return *frame;

            ssize_t n = ::recv(fd_.get(), buf, sizeof(buf), 0);
            if (n > 0) {
                decoder.feed(buf, static_cast<std::size_t>(n));
            } else if (n == 0) {
                throw std::runtime_error("peer closed connection before a full frame arrived");
            } else {
                if (errno == EINTR) continue;
                throw std::runtime_error(std::string("recv() failed: ") + std::strerror(errno));
            }
        }
    }

    int fd() const { return fd_.get(); }

private:
    FileDescriptor fd_;
};

} // namespace cascade::core::net