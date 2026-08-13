package raftnode_test

import (
	"testing"
	"time"

	"cascade/raftnode"
)

func findLeaderNode(t *testing.T, nodes []*raftnode.Node) *raftnode.Node {
	t.Helper()
	for _, n := range nodes {
		if n.IsLeader() {
			return n
		}
	}
	t.Fatal("no node currently believes it is leader")
	return nil
}

func TestCluster_ElectsExactlyOneLeader(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	if _, err := raftnode.WaitForLeader(nodes, 2*time.Second); err != nil {
		t.Fatalf("no leader elected: %v", err)
	}

	leaderCount := 0
	for _, n := range nodes {
		if n.IsLeader() {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader, found %d", leaderCount)
	}
}

func TestCluster_ProposedCommandReplicatesToAllFollowers(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	if _, err := raftnode.WaitForLeader(nodes, 2*time.Second); err != nil {
		t.Fatalf("no leader elected: %v", err)
	}
	leader := findLeaderNode(t, nodes)

	result, err := leader.Propose(raftnode.Command{
		Kind: raftnode.CmdCreateTopic, TopicName: "events", NumPartitions: 4,
	}, 1*time.Second)
	if err != nil {
		t.Fatalf("propose failed: %v", err)
	}
	if result.Err != nil {
		t.Fatalf("FSM rejected command: %v", result.Err)
	}

	// raft.Apply() on the leader only guarantees QUORUM commit, not that
	// every individual follower has applied yet -- give followers a
	// moment to catch up before asserting on their local FSM state.
	deadline := time.Now().Add(1 * time.Second)
	for {
		allCaughtUp := true
		for _, n := range nodes {
			if len(n.FSM.Metadata.ListTopics()) != 1 {
				allCaughtUp = false
			}
		}
		if allCaughtUp {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("not all followers caught up in time")
		}
		time.Sleep(10 * time.Millisecond)
	}

	for _, n := range nodes {
		topics := n.FSM.Metadata.ListTopics()
		if len(topics) != 1 || topics[0].Name != "events" {
			t.Fatalf("node %s did not replicate topic correctly: %+v", n.ID, topics)
		}
	}
}

func TestCluster_FollowerRejectsDirectApply(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	if _, err := raftnode.WaitForLeader(nodes, 2*time.Second); err != nil {
		t.Fatalf("no leader elected: %v", err)
	}

	var follower *raftnode.Node
	for _, n := range nodes {
		if !n.IsLeader() {
			follower = n
			break
		}
	}
	if follower == nil {
		t.Fatal("expected at least one follower")
	}

	_, err = follower.Propose(raftnode.Command{Kind: raftnode.CmdCreateTopic, TopicName: "x", NumPartitions: 1}, 1*time.Second)
	if err == nil {
		t.Fatal("expected proposing on a follower to fail")
	}
}

// The core Phase 9 requirement: "kill the leader mid-write burst, verify
// replica promotion + no committed data loss."
func TestCluster_NewLeaderElectedAfterLeaderShutdownNoDataLoss(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	if _, err := raftnode.WaitForLeader(nodes, 2*time.Second); err != nil {
		t.Fatalf("no leader elected: %v", err)
	}
	oldLeader := findLeaderNode(t, nodes)

	result, err := oldLeader.Propose(raftnode.Command{
		Kind: raftnode.CmdCreateTopic, TopicName: "pre-crash-topic", NumPartitions: 2,
	}, 1*time.Second)
	if err != nil || result.Err != nil {
		t.Fatalf("setup propose failed: err=%v result=%v", err, result)
	}

	if err := oldLeader.Raft.Shutdown().Error(); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	remaining := make([]*raftnode.Node, 0, len(nodes)-1)
	for _, n := range nodes {
		if n.ID != oldLeader.ID {
			remaining = append(remaining, n)
		}
	}

	newLeaderID, err := raftnode.WaitForLeader(remaining, 3*time.Second)
	if err != nil {
		t.Fatalf("no new leader elected after original leader shutdown: %v", err)
	}
	if newLeaderID == oldLeader.ID {
		t.Fatalf("expected a DIFFERENT node to become leader, still got %s", newLeaderID)
	}

	newLeader := findLeaderNode(t, remaining)
	found := false
	for _, tp := range newLeader.FSM.Metadata.ListTopics() {
		if tp.Name == "pre-crash-topic" {
			found = true
		}
	}
	if !found {
		t.Fatal("committed topic from before leader crash was lost -- data loss on failover!")
	}

	result2, err := newLeader.Propose(raftnode.Command{
		Kind: raftnode.CmdCreateTopic, TopicName: "post-failover-topic", NumPartitions: 1,
	}, 1*time.Second)
	if err != nil || result2.Err != nil {
		t.Fatalf("new leader failed to accept a write: err=%v result=%v", err, result2)
	}
}