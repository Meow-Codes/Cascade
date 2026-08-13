package raftnode_test

import (
	"testing"
	"time"

	"cascade/raftnode"
)

func TestSnapshot_NewReplicaCatchesUpViaSnapshotNotFullLogReplay(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	leaderID, err := raftnode.WaitForLeader(nodes, 2*time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}
	leader := nodeByIDHelper(t, nodes, leaderID)

	const kCommands = 50
	for i := 0; i < kCommands; i++ {
		if _, err := leader.Propose(raftnode.Command{Kind: raftnode.CmdIncrementCounter}, 1*time.Second); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	if err := leader.Raft.Snapshot().Error(); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	newNode, err := raftnode.AddNodeToCluster(nodes, "n4", leader, 2*time.Second)
	if err != nil {
		t.Fatalf("add node: %v", err)
	}
	nodes = append(nodes, newNode)

	deadline := time.Now().Add(3 * time.Second)
	for {
		if newNode.FSM.CurrentCounter() == int64(kCommands) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("new node did not catch up: got %d, want %d", newNode.FSM.CurrentCounter(), kCommands)
		}
		time.Sleep(20 * time.Millisecond)
	}

	stats := newNode.Raft.Stats()
	if stats["last_snapshot_index"] == "0" {
		t.Fatal("expected new node to have installed a snapshot")
	}
	t.Logf("new node caught up: counter=%d last_snapshot_index=%s", newNode.FSM.CurrentCounter(), stats["last_snapshot_index"])
}