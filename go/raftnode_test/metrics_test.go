package raftnode_test

import (
	"strings"
	"testing"
	"time"

	"cascade/raftnode"
)

func TestMetrics_RenderPrometheusStatsIncludesExpectedFields(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := raftnode.WaitForLeader(nodes, 2*time.Second); err != nil {
		t.Fatalf("no leader: %v", err)
	}

	text := nodes[0].RenderPrometheusStats()
	for _, expected := range []string{"cascade_raft_state", "cascade_raft_term", "cascade_raft_commit_index"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("expected metric %s in output, got:\n%s", expected, text)
		}
	}
}

func TestMetrics_ReplicationLagZeroWhenCaughtUp(t *testing.T) {
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	leaderID, err := raftnode.WaitForLeader(nodes, 2*time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}
	leader := nodeByIDHelper(t, nodes, leaderID)

	if _, err := leader.Propose(raftnode.Command{Kind: raftnode.CmdIncrementCounter}, 1*time.Second); err != nil {
		t.Fatalf("propose: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // let followers catch up

	for _, n := range nodes {
		if n.ID == leaderID {
			continue
		}
		lag, err := raftnode.ReplicationLag(leader, n)
		if err != nil {
			t.Fatalf("lag calc: %v", err)
		}
		if lag > 0 {
			t.Errorf("expected node %s caught up (lag=0), got lag=%d", n.ID, lag)
		}
	}
}