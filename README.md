[![CI](https://github.com/Meow-Codes/Cascade/actions/workflows/ci.yml/badge.svg)](https://github.com/Meow-Codes/Cascade/actions/workflows/ci.yml)

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)

![Tests](https://img.shields.io/badge/tests-85%20passing-brightgreen)

# Cascade

Cascade is a distributed messaging and real-time media streaming infrastructure
implemented from scratch in modern C++23.

The project explores how systems like Kafka, Discord, Zoom and modern streaming
servers are built internally by implementing storage engines, brokers,
networking, observability and real-time media transport without relying on
existing frameworks.

Current implementation includes:

- lock-free queues
- custom memory pool
- mmap-backed WAL storage engine
- publish/subscribe broker
- TCP & UDP networking
- Prometheus metrics
- distributed tracing
- jitter buffer
- retransmit-lite recovery
- adaptive bitrate controller

The project currently contains 85 automated unit and concurrency tests.

---

---

## Features

### Core Infrastructure

- Lock-free SPSC queue
- Lock-free MPMC queue
- Thread pool
- Timer wheel
- Memory pool
- Async logger
- Configuration manager

### Storage

- mmap-backed append-only log
- CRC validation
- WAL recovery
- Segment rotation
- Crash recovery

### Messaging

- Publish / Subscribe broker
- Consumer groups
- Backpressure
- Ordered partitions

### Networking

- TCP framing
- UDP transport
- Rate limiting
- Connection management

### Observability

- Prometheus metrics
- HTTP metrics endpoint
- Distributed tracing

### Media Streaming

- Audio packetization
- Silence detection
- Retransmit cache
- Priority packet queue
- Jitter buffer
- Retransmit-lite
- Adaptive bitrate controller

---

---

## Project Layout

```bash
cpp/
    core/
        concurrency/
        storage/
        broker/
        networking/
        media/
        metrics/

benchmarks/

tests/

docs/
```

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
./build/benchmarks/bench_storage
./build/benchmarks/bench_broker
./build/benchmarks/bench_tcp
./build/benchmarks/bench_media
./build/benchmarks/bench_adaptive
```

#### Results:

```bash
barghav@DESKTOP-DOOSSP5:~/lol_projects/Cascade$ ./build/benchmarks/bench_queue
--- Queue throughput benchmark ---
SPSC  1P/1C :    5000000 ops in   0.0267 s ->    187166558 ops/sec
MPMC 1P/1C :    1000000 ops in   0.0242 s ->     41282153 ops/sec
MPMC 2P/2C :    2000000 ops in   0.2225 s ->      8989041 ops/sec
MPMC 4P/4C :    4000000 ops in   0.5485 s ->      7292755 ops/sec
MPMC 8P/8C :    8000000 ops in   1.4098 s ->      5674540 ops/sec

barghav@DESKTOP-DOOSSP5:~/lol_projects/Cascade$ ./build/benchmarks/bench_memory_pool
new/delete     : 2000000 ops in 0.0333 s -> 16.7 ns/op
pooled alloc   : 2000000 ops in 0.0673 s -> 33.7 ns/op

barghav@DESKTOP-DOOSSP5:~/lol_projects/Cascade$ ./build/benchmarks/bench_storage
--- Storage engine benchmark ---
Write throughput: 200000 records x 512 bytes = 97.7 MB in 0.5260 s -> 185.7 MB/s
Read latency (random offset, n=50000): p50 = 0.48 us, p99 = 1.13 us
Recovery time for 10000 records: 6.60 ms
Recovery time for 100000 records: 65.47 ms
Recovery time for 500000 records: 334.68 ms

barghav@DESKTOP-DOOSSP5:~/lol_projects/Cascade$ ./build/benchmarks/bench_broker
--- Broker pub/sub benchmark ---
Creating broker...
Creating topic...
Creating producer...
Starting publish...
0
10000
20000
30000
40000
50000
60000
70000
80000
90000
100000
110000
120000
130000
140000
150000
160000
170000
180000
190000
200000
210000
220000
230000
240000
250000
260000
270000
280000
290000
300000
310000
320000
330000
340000
350000
360000
370000
380000
390000
400000
410000
420000
430000
440000
450000
460000
470000
480000
490000
Publish throughput: 500000 msgs in 397.9342 s -> 1256 msgs/sec
Consume throughput: 500000 msgs in 0.1136 s -> 4400938 msgs/sec
Publish-to-consume latency (n=20000): p50 = 720.02 us, p99 = 1941.75 us

barghav@DESKTOP-DOOSSP5:~/lol_projects/Cascade$ ./build/benchmarks/bench_tcp
--- TCP networking benchmark ---
TCP connections: 2000 in 0.5612 s -> 3564 conn/sec
TCP loopback RTT: p50 = 91.5 us, p99 = 146.8 us (n=5000)

barghav@DESKTOP-DOOSSP5:~/lol_projects/Cascade$ ./build/benchmarks/bench_media
--- Jitter buffer loss-masking sweep (target_delay=40 ms) ---
  simulated loss   0.0%  ->  effective loss   0.0%  (added latency: 40 ms)
  simulated loss   2.0%  ->  effective loss   1.6%  (added latency: 40 ms)
  simulated loss   5.0%  ->  effective loss   4.3%  (added latency: 40 ms)
  simulated loss  10.0%  ->  effective loss   8.8%  (added latency: 40 ms)
  simulated loss  20.0%  ->  effective loss  19.1%  (added latency: 40 ms)
  simulated loss  30.0%  ->  effective loss  28.8%  (added latency: 40 ms)
--- Jitter buffer loss-masking sweep (target_delay=100 ms) ---
  simulated loss   0.0%  ->  effective loss   0.0%  (added latency: 100 ms)
  simulated loss   2.0%  ->  effective loss   1.6%  (added latency: 100 ms)
  simulated loss   5.0%  ->  effective loss   4.3%  (added latency: 100 ms)
  simulated loss  10.0%  ->  effective loss   8.8%  (added latency: 100 ms)
  simulated loss  20.0%  ->  effective loss  19.1%  (added latency: 100 ms)
  simulated loss  30.0%  ->  effective loss  28.8%  (added latency: 100 ms)
--- Jitter buffer loss-masking sweep (target_delay=200 ms) ---
  simulated loss   0.0%  ->  effective loss   0.0%  (added latency: 200 ms)
  simulated loss   2.0%  ->  effective loss   1.6%  (added latency: 200 ms)
  simulated loss   5.0%  ->  effective loss   4.3%  (added latency: 200 ms)
  simulated loss  10.0%  ->  effective loss   8.8%  (added latency: 200 ms)
  simulated loss  20.0%  ->  effective loss  19.1%  (added latency: 200 ms)
  simulated loss  30.0%  ->  effective loss  28.8%  (added latency: 200 ms)

barghav@DESKTOP-DOOSSP5:~/lol_projects/Cascade$ ./build/benchmarks/bench_adaptive
--- Adaptive bitrate simulation: bandwidth trace over 20s ---
  t(s)    bw(kbps)   quality   dropped
     1        2500       low         0
     2        2500       low         0
     3        2500    medium         0
     4        2500    medium         0
     5        2500    medium         0
     6        2500    medium         0
     7         300    medium         0
     8         300       low         0
     9         300       low         0
    10         300       low         0
    11         300       low         0
    12         300       low         0
    13        2500    medium         0
    14        2500    medium         0
    15        2500    medium         0
    16        2500    medium         0
    17        2500    medium         0
    18        2500    medium         0
    19        2500    medium         0
    20        2500    medium         0
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