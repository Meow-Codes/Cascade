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

Current status:

```
22 / 22 tests passing
```

Tests include:

- Queue correctness
- Concurrent producer/consumer stress tests
- Memory pool validation
- Thread pool execution
- Timer wheel scheduling
- Configuration loading
- Logger concurrency

---

## Technologies

- C++23
- CMake
- Ninja
- GoogleTest
- GCC 16 (MSYS2 UCRT64)

---

## Build

```bash
cmake -S . -B build -G Ninja
cmake --build build
ctest --test-dir build --output-on-failure
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