#pragma once
// Cascade :: core::net :: FileDescriptor
//
// Move-only RAII wrapper around a raw POSIX fd (socket, epoll instance,
// etc). Exists so every net/ class gets automatic close() on destruction
// and scope-exit paths (including exceptions) without hand-written cleanup.

#include <unistd.h>
#include <utility>

namespace cascade::core::net {

class FileDescriptor {
public:
    FileDescriptor() = default;
    explicit FileDescriptor(int fd) : fd_(fd) {}

    ~FileDescriptor() { reset(); }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this != &other) {
            reset();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    int get() const { return fd_; }
    bool valid() const { return fd_ >= 0; }

    int release() {
        int f = fd_;
        fd_ = -1;
        return f;
    }

    void reset(int new_fd = -1) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = new_fd;
    }

private:
    int fd_ = -1;
};

} // namespace cascade::core::net