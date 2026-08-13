package raftnode_test

import (
	"sync"
	"testing"
	"time"

	"cascade/raftnode"
)

// Simplified linearizability check for a single register (the FSM's
// counter). Real linearizability checkers (Jepsen/Porcupine) validate
// arbitrary histories against ALL possible sequential orderings, which
// is a much bigger undertaking. This test validates the specific,
// checkable property that matters for a replicated counter: N concurrent
// increments from multiple goroutines against the SAME leader must
// produce exactly the values {1, 2, ..., N} -- no two operations ever
// observe/return the same "new value" (which would mean a lost update),
// and no gaps appear (which would mean a phantom/duplicated update).
// Because raft.Apply() calls are strictly serialized through the FSM,
// this is precisely what "linearizable" means for a counter: every
// operation appears to take effect atomically at a single point,
// consistent with real-time ordering.
func TestLinearizability_ConcurrentIncrementsProduceExactSequentialValues(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	leaderID, err := raftnode.WaitForLeader(nodes, 2*time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}
	leader := nodeByIDHelper(t, nodes, leaderID)

	const kGoroutines = 20
	const kIncrementsPerGoroutine = 25
	const kTotal = kGoroutines * kIncrementsPerGoroutine

	var wg sync.WaitGroup
	valuesCh := make(chan int64, kTotal)
	errCh := make(chan error, kTotal)

	for g := 0; g < kGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < kIncrementsPerGoroutine; i++ {
				result, err := leader.Propose(raftnode.Command{Kind: raftnode.CmdIncrementCounter}, 2*time.Second)
				if err != nil {
					errCh <- err
					continue
				}
				if result.Err != nil {
					errCh <- result.Err
					continue
				}
				valuesCh <- result.Value
			}
		}()
	}
	wg.Wait()
	close(valuesCh)
	close(errCh)

	for err := range errCh {
		t.Fatalf("increment failed: %v", err)
	}

	seen := make(map[int64]int)
	for v := range valuesCh {
		seen[v]++
	}

	if len(seen) != kTotal {
		t.Fatalf("expected %d distinct values, got %d distinct values (lost or duplicated updates)", kTotal, len(seen))
	}
	for v, count := range seen {
		if count != 1 {
			t.Fatalf("value %d returned by %d operations -- lost update (two ops observed the same 'new value')", v, count)
		}
	}
	for v := int64(1); v <= int64(kTotal); v++ {
		if seen[v] != 1 {
			t.Fatalf("gap detected: value %d never observed -- phantom or skipped update", v)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		allMatch := true
		for _, n := range nodes {
			if n.FSM.CurrentCounter() != int64(kTotal) {
				allMatch = false
			}
		}
		if allMatch {
			break
		}
		if time.Now().After(deadline) {
			for _, n := range nodes {
				t.Logf("node %s counter = %d", n.ID, n.FSM.CurrentCounter())
			}
			t.Fatal("followers did not converge to the same final counter value")
		}
		time.Sleep(10 * time.Millisecond)
	}
}