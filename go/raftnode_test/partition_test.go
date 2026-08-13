package raftnode_test

import (
	"testing"
	"time"

	"cascade/raftnode"
)

// The core Phase 9 partition requirement: "verify no split-brain writes."
// Isolates the current leader alone against the other two nodes, proves
// (a) the isolated ex-leader cannot commit anything once it loses quorum,
// (b) the majority side elects a new leader and keeps making progress,
// and (c) after healing, the whole cluster converges to one consistent
// state with no diverged/lost data.
func TestPartition_IsolatedLeaderCannotCommitMajorityMakesProgress(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap failed: %v", err)
	}
	leaderID, err := raftnode.WaitForLeader(nodes, 2*time.Second)
	if err != nil {
		t.Fatalf("no initial leader: %v", err)
	}

	var majority []string
	for _, n := range nodes {
		if n.ID != leaderID {
			majority = append(majority, n.ID)
		}
	}

	raftnode.Partition(nodes, []string{leaderID}, majority)
	t.Logf("partitioned: isolated=%s, majority=%v", leaderID, majority)

	// The isolated ex-leader still LOCALLY believes it's leader for a
	// moment (until its lease/heartbeat times out) -- that's exactly the
	// split-brain risk being tested. Proposing on it must fail: without
	// quorum ack, raft cannot commit the entry.
	isolated := nodeByIDHelper(t, nodes, leaderID)
	_, err = isolated.Propose(raftnode.Command{
		Kind: raftnode.CmdCreateTopic, TopicName: "should-never-commit", NumPartitions: 1,
	}, 500*time.Millisecond)
	if err == nil {
		t.Fatal("isolated ex-leader was able to commit a write without quorum -- SPLIT BRAIN")
	}

	var majorityNodes []*raftnode.Node
	for _, id := range majority {
		majorityNodes = append(majorityNodes, nodeByIDHelper(t, nodes, id))
	}
	newLeaderID, err := raftnode.WaitForLeader(majorityNodes, 3*time.Second)
	if err != nil {
		t.Fatalf("majority side failed to elect a leader: %v", err)
	}
	t.Logf("majority elected new leader: %s", newLeaderID)

	newLeader := nodeByIDHelper(t, majorityNodes, newLeaderID)
	result, err := newLeader.Propose(raftnode.Command{
		Kind: raftnode.CmdCreateTopic, TopicName: "committed-during-partition", NumPartitions: 1,
	}, 1*time.Second)
	if err != nil || result.Err != nil {
		t.Fatalf("majority side failed to make progress during partition: err=%v result=%v", err, result)
	}

	raftnode.Heal(nodes)

	deadline := time.Now().Add(3 * time.Second)
	for {
		converged := true
		for _, n := range nodes {
			topics := n.FSM.Metadata.ListTopics()
			hasCommitted := false
			hasNeverCommitted := false
			for _, tp := range topics {
				if tp.Name == "committed-during-partition" {
					hasCommitted = true
				}
				if tp.Name == "should-never-commit" {
					hasNeverCommitted = true
				}
			}
			if !hasCommitted || hasNeverCommitted {
				converged = false
			}
		}
		if converged {
			break
		}
		if time.Now().After(deadline) {
			for _, n := range nodes {
				t.Logf("node %s final topics: %+v", n.ID, n.FSM.Metadata.ListTopics())
			}
			t.Fatal("cluster did not converge to a consistent state after healing")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func nodeByIDHelper(t *testing.T, nodes []*raftnode.Node, id string) *raftnode.Node {
	t.Helper()
	for _, n := range nodes {
		if n.ID == id {
			return n
		}
	}
	t.Fatalf("no node with id %s", id)
	return nil
}