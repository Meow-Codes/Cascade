#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <thread>
#include <atomic>
#include <cassert>
#include "net/zero_copy_transfer.hpp"
#include "net/tcp_server.hpp"
#include "net/tcp_client.hpp"
#include "net/epoll_loop.hpp"

using namespace cascade::core::net;
using Clock = std::chrono::steady_clock;

static std::string make_test_file(std::size_t size) {
    std::string path = "/tmp/cascade_bench_zerocopy_" + std::to_string(::getpid());
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    std::vector<char> chunk(65536, 'x');
    std::size_t total_written = 0;

    while (total_written < size) {
        std::size_t n = std::min(chunk.size(), size - total_written);

        ssize_t bytes = ::write(fd, chunk.data(), n);
        if (bytes != static_cast<ssize_t>(n)) {
            throw std::runtime_error("failed to write test file");
        }

        total_written += n;
    }
    ::close(fd);
    return path;
}

// Traditional path: read() into a userspace buffer, then send() it back
// out -- two copies (page cache -> userspace, userspace -> socket buffer)
// plus two syscalls per chunk, the baseline sendfile() avoids.
static double bench_traditional(const std::string& path, std::size_t file_size, std::uint16_t port) {
    EpollLoop loop;
    TcpServer<EpollLoop> server(loop, "127.0.0.1", port);
    int in_fd = ::open(path.c_str(), O_RDONLY);

    server.set_on_connect([&](TcpServer<EpollLoop>::ConnectionId id, const std::string&) {
        std::vector<char> buf(65536);
        ::lseek(in_fd, 0, SEEK_SET);
        ssize_t n;
        while ((n = ::read(in_fd, buf.data(), buf.size())) > 0) {
            std::size_t sent = 0;
            while (sent < static_cast<std::size_t>(n)) {
                ssize_t s = ::send(id,
                                buf.data() + sent,
                                static_cast<std::size_t>(n) - sent,
                                0);

                if (s > 0) {
                    sent += static_cast<std::size_t>(s);
                } else if (s < 0 && (errno == EINTR)) {
                    continue;
                } else if (s < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    continue; // simplistic benchmark; production code would poll for writability
                } else {
                    break;
                }
            }
        }
    });

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(5); });

    auto start = Clock::now();
    int cfd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr = make_ipv4_addr("127.0.0.1", port);
    ::connect(cfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    std::size_t received = 0;
    std::vector<char> rbuf(65536);
    while (received < file_size) {
        ssize_t n = ::recv(cfd, rbuf.data(), rbuf.size(), 0);
        if (n <= 0) break;
        received += static_cast<std::size_t>(n);
    }
    auto end = Clock::now();
    ::close(cfd);
    stop.store(true);
    loop_thread.join();
    ::close(in_fd);

    return std::chrono::duration<double>(end - start).count();
}

static double bench_sendfile(const std::string& path, std::size_t file_size, std::uint16_t port) {
    EpollLoop loop;
    TcpServer<EpollLoop> server(loop, "127.0.0.1", port);
    int in_fd = ::open(path.c_str(), O_RDONLY);

    server.set_on_connect([&](TcpServer<EpollLoop>::ConnectionId id, const std::string&) {
        int flags = fcntl(id, F_GETFL, 0);
        fcntl(id, F_SETFL, flags & ~O_NONBLOCK);
        off_t offset = 0;
        std::size_t sent = send_file_range(id, in_fd, &offset, file_size);

        if (sent != file_size) {
            std::fprintf(stderr, "Only sent %zu/%zu bytes\n", sent, file_size);
        }
    });

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(5); });

    auto start = Clock::now();
    int cfd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr = make_ipv4_addr("127.0.0.1", port);
    ::connect(cfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    std::size_t received = 0;
    std::vector<char> rbuf(65536);
    while (received < file_size) {
        ssize_t n = ::recv(cfd, rbuf.data(), rbuf.size(), 0);
        if (n <= 0) break;
        received += static_cast<std::size_t>(n);
    }
    auto end = Clock::now();
    ::close(cfd);
    stop.store(true);
    loop_thread.join();
    ::close(in_fd);

    return std::chrono::duration<double>(end - start).count();
}

int main() {
    constexpr std::size_t kFileSize = 200 * 1024 * 1024; // 200 MB
    std::printf("--- read()+send() vs sendfile() : %.0f MB transfer ---\n", kFileSize / (1024.0 * 1024.0));

    std::string path = make_test_file(kFileSize);

    double t1 = bench_traditional(path, kFileSize, 28100);
    std::printf("read+send:  %.4f s -> %.1f MB/s\n", t1, (kFileSize / (1024.0 * 1024.0)) / t1);

    double t2 = bench_sendfile(path, kFileSize, 28101);
    std::printf("sendfile:   %.4f s -> %.1f MB/s\n", t2, (kFileSize / (1024.0 * 1024.0)) / t2);

    std::printf("speedup: %.2fx\n", t1 / t2);

    ::remove(path.c_str());
    return 0;
}