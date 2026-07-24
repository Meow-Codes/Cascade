# Cascade Design Document

## Overview

Cascade is a distributed messaging and real-time media streaming project implemented from scratch in modern C++23. Rather than reproducing the APIs of existing systems, the project focuses on understanding the engineering trade-offs behind storage engines, message brokers, networking, and low-latency media transport.

Many implementation decisions throughout the project involve balancing competing goals such as throughput versus durability, latency versus reliability, and implementation complexity versus long-term scalability.

This document summarizes three of the most significant architectural trade-offs made during the first version of Cascade.

---

# 1. Storage Engine

## mmap-backed Segments vs. write() + fsync()

The storage engine is responsible for durable persistence of broker messages through an append-only write-ahead log (WAL).

Two implementation approaches were considered.

### Option 1: write() + fsync()

Traditional database engines often append data using `write()` system calls and periodically invoke `fsync()` to force modified pages to permanent storage.

Advantages:

- Explicit control over durability
- Predictable persistence semantics
- Widely used by production databases

Disadvantages:

- Every write requires an additional kernel transition
- Frequent `fsync()` calls can significantly reduce throughput
- Requires manual buffer management

---

### Option 2: Memory-Mapped Files (Chosen)

Cascade stores each log segment using memory-mapped files (`mmap`).

Instead of copying application buffers into kernel buffers through repeated write system calls, the operating system exposes the file as virtual memory. The storage engine appends records by writing directly into the mapped region.

Advantages:

- Fewer explicit system calls during normal operation
- Simpler append logic
- Operating system performs page caching automatically
- Excellent random read performance
- Efficient recovery by remapping existing segments

Disadvantages:

- Less explicit control over page flushing
- Requires careful handling of segment rotation
- Page faults can introduce latency spikes

---

### Design Decision

For Cascade v1, memory-mapped storage provides the best balance between simplicity and performance.

The goal of the project is to study storage engine internals rather than maximize write throughput at all costs. Using `mmap` significantly reduced implementation complexity while still supporting:

- append-only writes
- CRC validation
- crash recovery
- segment rotation
- deterministic recovery after restart

Benchmarks show:

| Metric | Result |
|---------|--------|
| Sequential write throughput | **185.7 MB/s** |
| Random read latency (p50) | **0.48 μs** |
| Recovery (500k records) | **334.68 ms** |

---

# 2. Real-Time Media

## Jitter Buffer Size:
### Latency vs. Loss Masking

Packet arrival times on real networks are rarely uniform.

Packets may arrive:

- early
- late
- out of order
- with temporary bursts of delay

A jitter buffer intentionally delays playback so that late packets still have an opportunity to arrive before they are needed.

The fundamental trade-off is simple.

Increasing the buffer size:

✔ reduces effective packet loss

✔ improves playback stability

✘ increases end-to-end latency

Reducing the buffer size:

✔ minimizes playback delay

✘ increases audible packet loss

✘ increases sensitivity to network jitter

---

Cascade implements a configurable jitter buffer whose playback delay can be adjusted depending on network conditions.

Example benchmark configurations:

| Target Delay | Effective Behavior |
|--------------|-------------------|
| 40 ms | Lowest latency, less tolerance to delayed packets |
| 100 ms | Balanced latency and stability |
| 200 ms | Highest loss masking, increased playback delay |

The benchmark intentionally sweeps multiple packet-loss scenarios to demonstrate how target delay affects media behavior rather than attempting to optimize for a single workload.

---

### Design Decision

Cascade does not attempt to eliminate network latency.

Instead, it exposes latency as a configurable parameter whose value depends on the application's requirements.

Interactive voice communication typically favors lower latency, while streamed media may tolerate larger playback buffers to achieve smoother output.

Beginning in Phase 7, adaptive bitrate control complements the jitter buffer by reducing transmission bitrate when network conditions deteriorate, decreasing the probability of future packet loss instead of relying solely on buffering.

---

# 3. Event Notification

## epoll (Level Triggered) vs. io_uring

Cascade currently uses Linux's `epoll` interface for scalable network event notification.

`epoll` allows a single thread to monitor thousands of sockets without blocking on individual connections.

Two major alternatives were considered.

---

### epoll (Current Implementation)

Advantages:

- Mature Linux interface
- Stable across kernel versions
- Well understood
- Simple integration with existing socket APIs
- Excellent performance for moderate to high connection counts

Disadvantages:

- Read/write operations still require explicit system calls
- More user/kernel transitions than newer asynchronous interfaces
- Additional bookkeeping for readiness notifications

---

### io_uring (Future Work)

Linux's `io_uring` provides an asynchronous submission/completion queue model capable of reducing system call overhead.

Potential advantages include:

- Fewer kernel transitions
- Reduced syscall overhead
- Better batching opportunities
- Higher throughput for networking and storage workloads
- Unified asynchronous interface

Potential disadvantages include:

- Increased implementation complexity
- Larger API surface
- Kernel-version dependencies
- More difficult debugging
- Additional portability concerns

---

### Design Decision

Cascade intentionally uses `epoll` for Version 1.

The networking layer focuses on correctness, deterministic behavior, and clear architecture before introducing more advanced Linux-specific optimizations.

The current implementation already demonstrates scalable non-blocking networking while remaining significantly easier to reason about during development and testing.

Evaluation of an `io_uring` backend is explicitly deferred to a future project milestone (planned Phase 12), where performance gains can be measured against the existing `epoll` implementation rather than assumed.

---

# Summary

The primary design philosophy throughout Cascade is to optimize only after correctness has been established.

Several recurring principles guided architectural decisions:

- deterministic behavior improves testing reliability
- benchmark-driven development is preferred over premature optimization
- simpler implementations are favored unless measurable performance requires additional complexity
- different workloads require different trade-offs instead of a single universally optimal solution

These principles influenced the storage engine, networking stack, and real-time media pipeline, resulting in a system whose individual components remain relatively small, testable, and independently benchmarked while still demonstrating many of the core techniques used by modern distributed systems.