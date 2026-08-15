| Scenario | Result | Detail |
|---|---|---|
| Broker crash (kill -9 mid-write, WAL) | PASS | 10/10 runs recovered with zero corruption |
| Replica crash (Raft leader kill) | PASS | new leader elected, zero committed data loss (see raftnode_test) |
| Network partition (split-brain) | PASS | isolated leader rejected write; majority made progress; converged after heal |
| Consumer failure (stalled, never commits) | PASS | producer stalled by backpressure after 50 messages (max_lag=50); producer resumed after commit: YES |
| Packet loss (10% injected) | PASS | effective_loss=9.20%, playout never stalled |
| Latency injection (50ms +/-20ms) | PASS |  |
| Memory pressure (cgroup limit) | SKIPPED | cgroup v2 unavailable in this environment |
