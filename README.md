[![CI](https://github.com/Meow-Codes/Cascade/actions/workflows/ci.yml/badge.svg)](https://github.com/Meow-Codes/Cascade/actions/workflows/ci.yml)

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)

![Tests](https://img.shields.io/badge/tests-85%20passing-brightgreen)

# Cascade

Cascade is a distributed messaging broker and real-time media transport stack implemented entirely in **modern C++23**.

The project explores the engineering tradeoffs behind systems such as **Apache Kafka, Discord, Zoom, and modern streaming servers** by implementing storage engines, messaging infrastructure, networking, observability, and real-time media transport from first principles instead of relying on existing frameworks.

Rather than focusing only on functionality, Cascade emphasizes:

- crash-safe storage and recovery
- lock-free concurrent data structures
- deterministic testing using simulated time
- benchmark-driven development
- documented architectural tradeoffs
- separation of measurement from decision making

The current implementation consists of approximately **85 automated unit and concurrency tests**, covering every major subsystem of the project.

---

# Why Cascade?

Modern distributed systems combine many independent components:

- high-performance concurrent data structures
- durable storage engines
- publish/subscribe messaging
- network protocols
- observability
- real-time media delivery

Most projects implement only one of these areas.

Cascade combines all of them into a single codebase to understand how production messaging systems and streaming platforms are designed internally.

The goal is educational engineering—not cloning an existing product—but building the underlying infrastructure from scratch while documenting the reasoning behind every major design decision.

---

# Architecture

> **Architecture diagram**

```
                   +----------------------+
                   |      Producers       |
                   +----------+-----------+
                              |
                              |
                       TCP / UDP Transport
                              |
                              v
                 +--------------------------+
                 | Publish / Subscribe      |
                 | Broker                   |
                 +------------+-------------+
                              |
              +---------------+---------------+
              |                               |
              |                               |
              v                               v
      Storage Engine                 Media Pipeline
   (WAL + mmap Segments)     (Jitter Buffer + ABR +
                                 Priority Queues)
              |
              |
              v
       Consumer Groups
```

A complete architecture diagram is available in

```
docs/architecture/
```

---

# Features

## Concurrency

- Lock-free Single Producer Single Consumer queue
- Lock-free Multi Producer Multi Consumer queue
- Thread pool
- Timer wheel
- Fixed-size memory pool
- Async logger
- Configuration manager

---

## Storage Engine

- mmap-backed append-only log
- Write-Ahead Logging (WAL)
- CRC validation
- Segment rotation
- Crash recovery
- Persistent offsets
- Recovery benchmark

---

## Messaging

- Publish / Subscribe broker
- Ordered partitions
- Consumer groups
- Producer backpressure
- Offset tracking
- Topic abstraction

---

## Networking

- TCP framing
- UDP transport
- Connection manager
- Token bucket rate limiter
- Timeout handling

---

## Observability

- Prometheus metrics
- HTTP metrics endpoint
- Distributed tracing

---

## Real-Time Media

- Audio packetization
- Silence detection
- Retransmit cache
- Priority packet queue
- Jitter buffer
- Retransmit-lite recovery
- Network condition estimation
- Adaptive bitrate controller

---

# Engineering Highlights

Several design decisions intentionally mirror techniques used in production systems.

### Crash-safe storage

Storage uses an append-only Write-Ahead Log backed by memory-mapped files. Recovery reconstructs offsets directly from disk without requiring external metadata.

### Deterministic testing

Beginning with the media pipeline, components operate entirely on explicitly supplied timestamps rather than reading the system clock. This allows unit tests and benchmarks to advance simulated time deterministically without relying on sleeps or wall-clock timing.

### Separation of measurement and policy

The adaptive bitrate subsystem separates network measurement (`NetworkConditionEstimator`) from bitrate selection (`BitrateController`). This keeps measurement logic independent from adaptation policy and makes both components easier to test.

### Asymmetric bitrate adaptation

The bitrate controller immediately reduces quality during congestion but upgrades only after several consecutive good network measurements, reducing oscillation while maintaining stable playback.

### Priority-aware bandwidth shaping

Control traffic such as retransmission requests bypasses bandwidth shaping entirely while expendable media frames are throttled under constrained bandwidth, ensuring prioritization remains meaningful during congestion.

---

# Project Layout

```text
├── 📁 .github
│   └── 📁 workflows
│       └── ⚙️ ci.yaml
├── 📁 benchmarks
│   ├── 📄 CMakeLists.txt
│   ├── ⚡ bench_adaptive.cpp
│   ├── ⚡ bench_broker.cpp
│   ├── ⚡ bench_media.cpp
│   ├── ⚡ bench_memory_pool.cpp
│   ├── ⚡ bench_queue.cpp
│   ├── ⚡ bench_storage.cpp
│   └── ⚡ bench_tcp.cpp
├── 📁 cpp
│   ├── 📁 core
│   │   ├── 📁 broker
│   │   │   ├── ⚡ broker.hpp
│   │   │   ├── ⚡ consumer.hpp
│   │   │   ├── ⚡ offset_store.hpp
│   │   │   ├── ⚡ partition.hpp
│   │   │   ├── ⚡ producer.hpp
│   │   │   └── ⚡ topic.hpp
│   │   ├── 📁 config
│   │   │   └── ⚡ config.hpp
│   │   ├── 📁 logger
│   │   │   └── ⚡ logger.hpp
│   │   ├── 📁 media
│   │   │   ├── ⚡ adaptive_sender.hpp
│   │   │   ├── ⚡ audio_packet.hpp
│   │   │   ├── ⚡ audio_session.hpp
│   │   │   ├── ⚡ bitrate_controller.hpp
│   │   │   ├── ⚡ jitter_buffer.hpp
│   │   │   ├── ⚡ network_estimator.hpp
│   │   │   ├── ⚡ priority_queue.hpp
│   │   │   ├── ⚡ retransmit_cache.hpp
│   │   │   └── ⚡ silence_detector.hpp
│   │   ├── 📁 memory
│   │   │   └── ⚡ memory_pool.hpp
│   │   ├── 📁 metrics
│   │   │   ├── ⚡ http_metrics_server.hpp
│   │   │   ├── ⚡ metrics_registry.hpp
│   │   │   └── ⚡ system_metrics.hpp
│   │   ├── 📁 net
│   │   │   ├── ⚡ connection_manager.hpp
│   │   │   ├── ⚡ epoll_loop.hpp
│   │   │   ├── ⚡ framing.hpp
│   │   │   ├── ⚡ rate_limiter.hpp
│   │   │   ├── ⚡ socket_fd.hpp
│   │   │   ├── ⚡ socket_utils.hpp
│   │   │   ├── ⚡ tcp_client.hpp
│   │   │   ├── ⚡ tcp_server.hpp
│   │   │   └── ⚡ udp_socket.hpp
│   │   ├── 📁 queue
│   │   │   ├── ⚡ mpmc_queue.hpp
│   │   │   └── ⚡ spsc_queue.hpp
│   │   ├── 📁 storage
│   │   │   ├── ⚡ crc32.hpp
│   │   │   ├── ⚡ log.hpp
│   │   │   └── ⚡ segment.hpp
│   │   ├── 📁 threadpool
│   │   │   └── ⚡ thread_pool.hpp
│   │   ├── 📁 timer
│   │   │   └── ⚡ timer_wheel.hpp
│   │   └── 📁 tracing
│   │       └── ⚡ trace.hpp
│   └── 📄 CMakeLists.txt
├── 📁 fault-injection
│   └── 📄 kill_wal_test.sh
├── 📁 tests
│   ├── 📄 CMakeLists.txt
│   ├── ⚡ test_adaptive_sender.cpp
│   ├── ⚡ test_audio_packet_codec.cpp
│   ├── ⚡ test_audio_session_udp.cpp
│   ├── ⚡ test_bitrate_controller.cpp
│   ├── ⚡ test_broker_backpressure.cpp
│   ├── ⚡ test_broker_consumer_group.cpp
│   ├── ⚡ test_broker_ordering.cpp
│   ├── ⚡ test_config.cpp
│   ├── ⚡ test_connection_manager.cpp
│   ├── ⚡ test_framing.cpp
│   ├── ⚡ test_jitter_buffer.cpp
│   ├── ⚡ test_log_recovery.cpp
│   ├── ⚡ test_logger.cpp
│   ├── ⚡ test_memory_pool.cpp
│   ├── ⚡ test_metrics_http_server.cpp
│   ├── ⚡ test_metrics_overhead.cpp
│   ├── ⚡ test_metrics_registry.cpp
│   ├── ⚡ test_mpmc_queue.cpp
│   ├── ⚡ test_network_estimator.cpp
│   ├── ⚡ test_priority_queue.cpp
│   ├── ⚡ test_rate_limiter.cpp
│   ├── ⚡ test_retransmit_cache.cpp
│   ├── ⚡ test_segment.cpp
│   ├── ⚡ test_silence_detector.cpp
│   ├── ⚡ test_spsc_queue.cpp
│   ├── ⚡ test_storage_concurrent_readers.cpp
│   ├── ⚡ test_storage_property.cpp
│   ├── ⚡ test_tcp.cpp
│   ├── ⚡ test_thread_pool.cpp
│   ├── ⚡ test_timer_wheel.cpp
│   ├── ⚡ test_tracing.cpp
│   └── ⚡ test_udp.cpp
├── 📁 third_party
├── 📁 tools
│   ├── 📄 CMakeLists.txt
│   ├── ⚡ wal_kill_writer.cpp
│   └── ⚡ wal_verifier.cpp
├── ⚙️ .gitignore
├── 📄 CMakeLists.txt
└── 📝 README.md
```

---

# Performance Summary

The following measurements were obtained from Release builds using the project's benchmark suite.

| Component | Result |
|-----------|---------:|
| Automated tests | **85 / 85 passing** |
| Storage write throughput | **185.7 MB/s** |
| Random read latency (p50 / p99) | **0.48 μs / 1.13 μs** |
| WAL recovery (500k records) | **334.68 ms** |
| Broker publish throughput | **1,256 msgs/sec** |
| Broker consume throughput | **4.40 million msgs/sec** |
| Publish → Consume latency (p50 / p99) | **720 μs / 1.94 ms** |
| TCP loopback RTT (p50 / p99) | **91.5 μs / 146.8 μs** |
| TCP connection establishment | **3,564 connections/sec** |
| Queue throughput (SPSC) | **187 million ops/sec** |
| Adaptive bitrate | Immediate downgrade, delayed upgrade |
| Jitter buffer | Configurable 40–200 ms target delay |

> Detailed benchmark outputs are available in the benchmark executables and accompanying documentation.

---

# Testing

Cascade uses **GoogleTest** for unit, integration, and concurrency testing.

Current test coverage includes:

## Concurrency

- SPSC queue
- MPMC queue
- Thread pool
- Memory pool
- Timer wheel

## Storage

- WAL append
- CRC validation
- Crash recovery
- Segment rotation
- Persistent offsets

## Messaging

- Publish / Subscribe broker
- Consumer groups
- Ordered delivery
- Producer backpressure

## Networking

- TCP framing
- UDP transport
- Rate limiter
- Connection manager
- Timeout handling

## Media

- Audio packetization
- Priority packet queue
- Retransmit cache
- Jitter buffer
- Network condition estimator
- Adaptive bitrate controller

Every subsystem is accompanied by deterministic unit tests, allowing failures to be reproduced without relying on wall-clock timing.

Run the complete test suite:

```bash
ctest --test-dir build --output-on-failure
```

---

# Building

## Requirements

- C++23 compatible compiler
- CMake 3.20+
- Ninja or Make
- GoogleTest

---

## Linux

Configure:

```bash
cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release
```

Build:

```bash
cmake --build build -j
```

Run tests:

```bash
ctest --test-dir build --output-on-failure
```

---

### AddressSanitizer

```bash
cmake -S . -B build-asan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCASCADE_ENABLE_ASAN=ON \
    -DCASCADE_ENABLE_UBSAN=ON
```

---

### ThreadSanitizer

```bash
cmake -S . -B build-tsan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCASCADE_ENABLE_TSAN=ON
```

---

## Windows (MSYS2 UCRT64)

Configure:

```bash
cmake -S . -B build
```

Build:

```bash
cmake --build build
```

Run tests:

```bash
ctest --test-dir build --output-on-failure
```

Networking tests that require POSIX sockets are skipped automatically on Windows.

---

# Running Benchmarks

Build benchmark targets:

```bash
cmake --build build --target \
    bench_queue \
    bench_memory_pool \
    bench_storage \
    bench_broker \
    bench_tcp \
    bench_media \
    bench_adaptive
```

Run benchmarks:

```bash
./build/benchmarks/bench_queue

./build/benchmarks/bench_memory_pool

./build/benchmarks/bench_storage

./build/benchmarks/bench_broker

./build/benchmarks/bench_tcp

./build/benchmarks/bench_media

./build/benchmarks/bench_adaptive
```

Each benchmark focuses on a different subsystem:

| Benchmark | Measures |
|-----------|----------|
| bench_queue | Queue throughput |
| bench_memory_pool | Allocation performance |
| bench_storage | Storage throughput and recovery |
| bench_broker | Publish / consume throughput |
| bench_tcp | TCP latency and connection rate |
| bench_media | Jitter buffer behaviour |
| bench_adaptive | Adaptive bitrate response |

---

# Benchmark Highlights

## Storage Engine

- 185.7 MB/s sequential write throughput
- Sub-microsecond median random reads
- Crash recovery scales linearly with log size

---

## Broker

- Ordered publish / subscribe messaging
- Producer backpressure
- Multi-million message per second consumer throughput

---

## Networking

- Low-latency TCP framing
- Token bucket rate limiting
- Efficient connection management

---

## Media

The media subsystem introduces several components commonly found in real-time communication systems.

- Packetization
- Retransmit cache
- Jitter buffer
- Packet prioritization
- Adaptive bitrate control

Unlike the storage engine, correctness in the media pipeline is defined by **low latency rather than perfect durability**. Expired media frames are intentionally discarded when necessary to preserve real-time playback.

---

# Documentation

Project documentation is located under the `docs/` directory.

```
docs/
├── architecture/
│   └── architecture.png
│
├── notes/
│   ├── phase1.md
│   ├── phase2.md
│   ├── phase3.md
│   ├── phase4.md
│   ├── phase5.md
│   ├── phase6.md
│   └── phase7.md
│
└── design.md
```

The phase notes document the evolution of the project, benchmark methodology, design tradeoffs, and implementation decisions made throughout development.

---

## Benchmarks

Build benchmark targets:

```bash
cmake --build build --target \
    bench_queue \
    bench_memory_pool \
    bench_storage \
    bench_broker \
    bench_tcp \
    bench_media \
    bench_adaptive
```

Run:

```bash
./build/benchmarks/bench_queue
./build/benchmarks/bench_memory_pool
./build/benchmarks/bench_storage
./build/benchmarks/bench_broker
./build/benchmarks/bench_tcp
./build/benchmarks/bench_media
./build/benchmarks/bench_adaptive
```

---

## Benchmark Summary

The project includes microbenchmarks and system-level benchmarks covering
core concurrency primitives, persistent storage, networking, messaging,
and real-time media components.

| Component | Result |
|-----------|--------|
| **SPSC Queue** | **187M ops/sec** |
| **MPMC Queue (1P/1C)** | **41M ops/sec** |
| **Storage Write Throughput** | **185.7 MB/s** |
| **Random Read Latency** | **p50: 0.48 μs, p99: 1.13 μs** |
| **WAL Recovery (500k records)** | **334.68 ms** |
| **TCP Loopback RTT** | **p50: 91.5 μs, p99: 146.8 μs** |
| **Broker Consume Throughput** | **4.40M msgs/sec** |
| **Broker Publish Throughput*** | **1256 msgs/sec** |
| **Media Benchmark** | configurable latency / packet-loss evaluation |
| **Adaptive Bitrate** | immediate downgrade, delayed upgrade policy |

> **Note**
>
> The broker publish benchmark currently prioritizes correctness and
> durability over maximum throughput. Future work will focus on batching,
> asynchronous disk flushing and replication, which are expected to
> substantially improve publish performance.

---

## Running Tests

Cascade uses **GoogleTest** for unit, integration and concurrency testing.

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

Current status:

```
85 / 85 tests passing
```

Tests cover:

- Lock-free queues
- Thread pool
- Timer wheel
- Memory pool
- Storage engine
- WAL recovery
- CRC validation
- Broker
- Consumer groups
- Backpressure
- TCP framing
- UDP transport
- Connection management
- Prometheus metrics
- Distributed tracing
- Jitter buffer
- Packet retransmission
- Network condition estimator
- Adaptive bitrate controller

---

## Documentation

Additional design documentation is available in the `docs/` directory.

```
docs/
├── architecture/
│   └── architecture.png
├── design.md
├── notes/
│   ├── phase1.md
│   ├── phase2.md
│   ├── phase3.md
│   ├── phase4.md
│   ├── phase5.md
│   ├── phase6.md
│   └── phase7.md
```

The phase notes document the evolution of the project, implementation
decisions, benchmarks, trade-offs, and lessons learned throughout the
development process.

---

## Build

### Linux

```bash
cmake -S . -B build \
    -DCMAKE_BUILD_TYPE=Release

cmake --build build -j
ctest --test-dir build
```

### AddressSanitizer

```bash
cmake -S . -B build-asan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCASCADE_ENABLE_ASAN=ON \
    -DCASCADE_ENABLE_UBSAN=ON
```

### ThreadSanitizer (Linux)

```bash
cmake -S . -B build-tsan \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCASCADE_ENABLE_TSAN=ON
```

---

### Windows (MSYS2 UCRT64)

```bash
cmake -S . -B build

cmake --build build

ctest --test-dir build
```

Networking tests that depend on POSIX sockets are skipped automatically on
Windows.

---

## Project Status

Cascade v1 is feature complete.

Implemented:

- High-performance concurrency primitives
- Persistent storage engine
- Publish/Subscribe broker
- TCP networking
- UDP media transport
- Metrics & tracing
- Real-time media pipeline
- Adaptive bitrate streaming
- Extensive automated testing
- Benchmark suite

Future work (v2):

- Raft replication
- Multi-broker clustering
- Partition rebalancing
- Leader election
- Snapshotting
- Authentication & authorization
- TLS transport
- Web dashboard
- io_uring networking backend
- Video streaming support

---

## Key Design Principles

Throughout the project, several architectural principles remained
consistent.

- **Correctness before optimization.**
  Every subsystem is implemented with deterministic behavior and extensive
  automated testing before performance tuning.

- **Measure before optimizing.**
  Every benchmark exists to validate a specific design decision rather than
  to produce impressive numbers.

- **Separate measurement from policy.**
  Components such as `NetworkConditionEstimator` provide observations,
  while policy decisions belong to higher-level controllers.

- **Deterministic testing.**
  Media components use explicit timestamps instead of wall-clock time,
  making simulations and unit tests repeatable.

- **Different workloads require different trade-offs.**
  Durable storage never silently loses data, whereas real-time media
  intentionally sacrifices stale packets to maintain low latency.

---

## Technologies

- C++23
- CMake
- GoogleTest
- Prometheus
- POSIX sockets
- mmap
- epoll
- UDP
- TCP
- GitHub Actions

---

## License

Released under the **MIT License**.

See the [LICENSE](LICENSE) file for details.