package raftnode_test

import (
	"os"
	"testing"
	"time"

	"cascade/raftnode"
)

// The actual Phase 10 "automatic recovery" claim: kill the process
// (simulated here by Close()-ing the Node, which is what a real process
// exit/crash would leave behind on disk), start a brand NEW Node backed
// by the SAME data directory, and confirm it recovers all previously
// committed state with zero peer involvement -- not via AddNodeToCluster
// rejoining as an empty node (that's Phase 10's snapshot-sync test,
// a different mechanism for a different scenario: a node that never had
// local state at all, vs. one recovering ITS OWN prior state).
func TestRecovery_NodeRecoversStateAfterRestartFromDisk(t *testing.T) {
	dataDir := t.TempDir()
	bindAddr := "127.0.0.1:19301"

	node1, err := raftnode.BootstrapSingleFileNode("solo", bindAddr, dataDir)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := raftnode.WaitForLeader([]*raftnode.Node{node1}, 2*time.Second); err != nil {
		t.Fatalf("single node never became leader: %v", err)
	}

	const kCommands = 30
	for i := 0; i < kCommands; i++ {
		if _, err := node1.Propose(raftnode.Command{Kind: raftnode.CmdIncrementCounter}, 1*time.Second); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	if got := node1.FSM.CurrentCounter(); got != kCommands {
		t.Fatalf("pre-restart sanity check failed: got %d, want %d", got, kCommands)
	}

	// Simulate process death: Close() releases the port and files
	// cleanly, standing in for the process exiting (normally or via
	// crash -- raft's on-disk WAL is what makes either case recoverable).
	if err := node1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Brand new Node, same dataDir, same bind address, zero shared
	// in-process state with node1 -- this is what a genuinely restarted
	// process looks like.
	node2, err := raftnode.NewFileNode("solo", bindAddr, dataDir)
	if err != nil {
		t.Fatalf("recreate after restart: %v", err)
	}
	// fmt.Printf("node2.FSM = %p\n", node2.FSM)
	defer node2.Close()

	if _, err := raftnode.WaitForLeader([]*raftnode.Node{node2}, 2*time.Second); err != nil {
		t.Fatalf("recovered node never became leader: %v", err)
	}

	// CRITICAL: no Propose() calls here. If this passes, the counter
	// value came ENTIRELY from raft replaying its on-disk log/snapshot
	// during NewRaft() construction -- proving real disk-backed recovery.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if node2.FSM.CurrentCounter() == kCommands {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovered node lost state: got %d, want %d",
				node2.FSM.CurrentCounter(), kCommands)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("recovered counter=%d from disk with zero peer involvement", node2.FSM.CurrentCounter())
}

func TestRecovery_DataDirActuallyContainsPersistedFiles(t *testing.T) {
	dataDir := t.TempDir()
	node, err := raftnode.BootstrapSingleFileNode("solo2", "127.0.0.1:19302", dataDir)
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	defer node.Close()

	if _, err := raftnode.WaitForLeader([]*raftnode.Node{node}, 2*time.Second); err != nil {
		t.Fatalf("no leader: %v", err)
	}
	if _, err := node.Propose(raftnode.Command{Kind: raftnode.CmdIncrementCounter}, 1*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}

	boltPath := dataDir + "/raft.db"
	if _, err := os.Stat(boltPath); err != nil {
		t.Fatalf("expected raft.db to exist on disk, got: %v", err)
	}
}