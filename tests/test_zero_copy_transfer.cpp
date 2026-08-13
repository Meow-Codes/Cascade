#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include "net/zero_copy_transfer.hpp"
#include "net/tcp_server.hpp"
#include "net/tcp_client.hpp"
#include "net/epoll_loop.hpp"

using namespace cascade::core::net;
using namespace std::chrono_literals;

TEST(ZeroCopyTransfer, SendFileRangeTransfersExactBytes) {
    // Write a known file, then sendfile() it over a real loopback socket
    // and verify the receiver gets exactly the same bytes.
    std::string path = "/tmp/cascade_zerocopy_test_" + std::to_string(::getpid());
    std::string content(50000, 'Z');
    for (std::size_t i = 0; i < content.size(); ++i) content[i] = static_cast<char>('A' + (i % 26));

    int wfd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    ASSERT_GE(wfd, 0);
    ::write(wfd, content.data(), content.size());
    ::close(wfd);

    EpollLoop loop;
    TcpServer<EpollLoop> server(loop, "127.0.0.1", 18200);

    int in_fd = ::open(path.c_str(), O_RDONLY);
    ASSERT_GE(in_fd, 0);

    server.set_on_connect([&](TcpServer<EpollLoop>::ConnectionId id, const std::string&) {
        off_t offset = 0;
        std::size_t sent = send_file_range(id, in_fd, &offset, content.size());
        EXPECT_EQ(sent, content.size());
    });

    std::atomic<bool> stop{false};
    std::thread loop_thread([&] { while (!stop.load()) loop.poll_once(5); });

    // Raw client (not TcpServer's TcpClient, since sendfile output has no
    // length-prefix framing wrapped around it -- it's raw bytes).
    int cfd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr = make_ipv4_addr("127.0.0.1", 18200);
    ASSERT_EQ(::connect(cfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);

    std::string received;
    received.reserve(content.size());
    char buf[4096];
    auto deadline = std::chrono::steady_clock::now() + 3s;
    while (received.size() < content.size() && std::chrono::steady_clock::now() < deadline) {
        ssize_t n = ::recv(cfd, buf, sizeof(buf), 0);
        if (n > 0) received.append(buf, static_cast<std::size_t>(n));
        else if (n == 0) break;
    }
    ::close(cfd);
    stop.store(true);
    loop_thread.join();
    ::close(in_fd);
    ::remove(path.c_str());

    ASSERT_EQ(received.size(), content.size());
    EXPECT_EQ(received, content);
}