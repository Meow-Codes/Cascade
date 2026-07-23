#pragma once
// Cascade :: core::metrics :: HttpMetricsServer
//
// Minimal raw-HTTP/1.1 listener serving Prometheus text exposition format
// on GET /metrics. Deliberately NOT built on Phase 2's TcpServer -- see
// the design note in this phase's chat message on why the framed
// protocol and raw HTTP are incompatible on the same listener. Built
// directly on EpollLoop using the same socket_fd/socket_utils primitives
// TcpServer itself uses, just without the length-prefix framing layer.
// Good enough for `curl` and Prometheus's scraper; not a general HTTP
// server (no keep-alive, no chunked encoding, one request per connection).

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unordered_map>

#include "metrics/metrics_registry.hpp"
#include "net/epoll_loop.hpp"
#include "net/socket_fd.hpp"
#include "net/socket_utils.hpp"

namespace cascade::core::metrics {

class HttpMetricsServer {
public:
    HttpMetricsServer(net::EpollLoop& loop, const std::string& bind_ip, std::uint16_t port,
                       MetricsRegistry& registry)
        : loop_(loop), registry_(registry) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) throw std::runtime_error("socket() failed for metrics server");
        listen_fd_ = net::FileDescriptor(fd);

        net::set_reuseaddr(listen_fd_.get());
        auto addr = net::make_ipv4_addr(bind_ip, port);
        if (::bind(listen_fd_.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            throw std::runtime_error("bind() failed for metrics server");
        }
        if (::listen(listen_fd_.get(), 16) < 0) {
            throw std::runtime_error("listen() failed for metrics server");
        }
        net::set_nonblocking(listen_fd_.get());

        loop_.add(listen_fd_.get(), EPOLLIN, [this](std::uint32_t) { accept_loop(); });
    }

    ~HttpMetricsServer() {
        for (auto& [fd, buf] : buffers_) { loop_.remove(fd); ::close(fd); }
        loop_.remove(listen_fd_.get());
    }

    HttpMetricsServer(const HttpMetricsServer&) = delete;
    HttpMetricsServer& operator=(const HttpMetricsServer&) = delete;

private:
    void accept_loop() {
        while (true) {
            int fd = ::accept(listen_fd_.get(), nullptr, nullptr);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                if (errno == EINTR) continue;
                return;
            }
            net::set_nonblocking(fd);
            buffers_[fd] = "";
            loop_.add(fd, EPOLLIN, [this, fd](std::uint32_t events) { handle_readable(fd, events); });
        }
    }

    void handle_readable(int fd, std::uint32_t events) {
        if (events & (EPOLLERR | EPOLLHUP)) { close_conn(fd); return; }

        char buf[4096];
        while (true) {
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                buffers_[fd].append(buf, static_cast<std::size_t>(n));
                if (buffers_[fd].find("\r\n\r\n") != std::string::npos) {
                    respond(fd, buffers_[fd]);
                    close_conn(fd);
                    return;
                }
            } else if (n == 0) {
                close_conn(fd);
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                if (errno == EINTR) continue;
                close_conn(fd);
                return;
            }
        }
    }

    void respond(int fd, const std::string& request) {
        std::string body, status;
        if (request.rfind("GET /metrics", 0) == 0) {
            body = registry_.render_prometheus();
            status = "200 OK";
        } else {
            body = "not found\n";
            status = "404 Not Found";
        }
        std::string response = "HTTP/1.1 " + status +
                                "\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: " +
                                std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;

        std::size_t sent = 0;
        while (sent < response.size()) {
            ssize_t n = ::send(fd, response.data() + sent, response.size() - sent, 0);
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                break;
            }
            sent += static_cast<std::size_t>(n);
        }
    }

    void close_conn(int fd) {
        loop_.remove(fd);
        ::close(fd);
        buffers_.erase(fd);
    }

    net::EpollLoop& loop_;
    MetricsRegistry& registry_;
    net::FileDescriptor listen_fd_;
    std::unordered_map<int, std::string> buffers_;
};

} // namespace cascade::core::metrics