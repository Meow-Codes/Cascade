#pragma once
// Cascade :: core::net :: socket_utils
//
// Small free functions shared by TCP/UDP code: non-blocking mode,
// common socket options, and address formatting. Kept out of the
// server/client classes so those classes stay focused on protocol logic.

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>

namespace cascade::core::net {

inline void set_nonblocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags == -1) throw std::runtime_error(std::string("fcntl(F_GETFL) failed: ") + std::strerror(errno));
    if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        throw std::runtime_error(std::string("fcntl(F_SETFL) failed: ") + std::strerror(errno));
    }
}

inline void set_reuseaddr(int fd) {
    int opt = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
}

inline void set_tcp_nodelay(int fd) {
    int opt = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt)); // disable Nagle: we care about latency, not bandwidth
}

inline sockaddr_in make_ipv4_addr(const std::string& ip, std::uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (ip.empty() || ip == "0.0.0.0") {
        addr.sin_addr.s_addr = INADDR_ANY;
    } else if (::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) != 1) {
        throw std::runtime_error("invalid IPv4 address: " + ip);
    }
    return addr;
}

inline std::string addr_to_string(const sockaddr_in& addr) {
    char buf[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
    return std::string(buf) + ":" + std::to_string(ntohs(addr.sin_port));
}

} // namespace cascade::core::net