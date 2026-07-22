![CI](https://github.com/Meow-Codes/Cascade/actions/workflows/ci.yml/badge.svg)
![License](...)

# Cascade

A high-performance distributed media streaming infrastructure built from scratch in modern C++23 and Go.

## Project Goals

Cascade is an educational systems project focused on learning the internals of large-scale distributed systems by implementing them from first principles.

The project emphasizes:

- Modern C++23
- Lock-free concurrency
- Custom memory management
- High-performance networking
- Distributed messaging
- Media streaming
- Replication and fault tolerance

---

## Current Progress

### Phase 1 — Core Infrastructure

- [x] Lock-free SPSC queue
- [x] Lock-free MPMC queue
- [x] Thread pool
- [x] Memory pool
- [x] Timer wheel
- [x] Configuration manager
- [x] Asynchronous logger

---

## Testing

Cascade uses GoogleTest for unit and concurrency testing.

Current coverage:

- SPSC queue
- MPMC queue
- Memory pool
- Thread pool
- Timer wheel
- Configuration manager
- Logger

Run:

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build

---

## Technologies

- C++23
- CMake
- Ninja
- GoogleTest
- GCC 16 (MSYS2 UCRT64)

---

# Building

## Linux

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Enable AddressSanitizer:

```bash
cmake -S . -B build-asan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCASCADE_ENABLE_ASAN=ON \
    -DCASCADE_ENABLE_UBSAN=ON
```

Enable ThreadSanitizer (Linux only):

```bash
cmake -S . -B build-tsan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCASCADE_ENABLE_TSAN=ON
```

---

## Windows (MSYS2 UCRT64)

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

Networking tests requiring POSIX sockets are automatically skipped on Windows.

---

## Performance

Benchmarks were run on:

- Windows 11
- GCC 16
- Release build

Queue throughput (Ryzen ...)

SPSC
313 M ops/sec

MPMC

1P/1C 36.4 M ops/sec
2P/2C 11.8 M ops/sec
4P/4C 8.7 M ops/sec
8P/8C 5.3 M ops/sec

## Benchmarks

```bash
cmake --build build --target bench_queue bench_memory_pool

./build/benchmarks/bench_queue
./build/benchmarks/bench_memory_pool
```

### Queue throughput

| Benchmark | Throughput |
|-----------|-----------:|
| SPSC (1P/1C) | 313M ops/sec |
| MPMC (1P/1C) | 36M ops/sec |
| MPMC (2P/2C) | 11.8M ops/sec |
| MPMC (4P/4C) | 8.7M ops/sec |
| MPMC (8P/8C) | 5.2M ops/sec |

Run locally:

```bash
cmake -S . -B build
cmake --build build --config Release

./build/benchmarks/bench_queue
./build/benchmarks/bench_memory_pool
```

---

## Roadmap

- Phase 1 — Core concurrency primitives
- Phase 2 — Storage engine
- Phase 3 — Broker
- Phase 4 — Replication
- Phase 5 — Networking
- Phase 6 — Streaming services
- Phase 7 — Production hardening

---

## License

MIT