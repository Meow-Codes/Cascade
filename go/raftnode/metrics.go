package raftnode

import (
	"fmt"
	"strconv"
	"strings"
)

// RenderPrometheusStats converts raft.Stats() into Prometheus text
// exposition format, mirroring Phase 5's C++ MetricsRegistry.render_
// prometheus() shape/naming style -- same "cascade_" prefix convention,
// same text format -- so one Grafana dashboard can scrape both the Go
// control plane and C++ brokers consistently.
func (n *Node) RenderPrometheusStats() string {
	stats := n.Raft.Stats()
	var sb strings.Builder

	if state, ok := stats["state"]; ok {
		sb.WriteString(fmt.Sprintf("# TYPE cascade_raft_state gauge\ncascade_raft_state{node=%q,value=%q} 1\n", n.ID, state))
	}

	numericKeys := []string{"term", "commit_index", "applied_index", "fsm_pending", "last_log_index", "last_snapshot_index"}
	for _, k := range numericKeys {
		v, ok := stats[k]
		if !ok {
			continue
		}
		iv, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			continue
		}
		metricName := "cascade_raft_" + k
		sb.WriteString(fmt.Sprintf("# TYPE %s gauge\n%s{node=%q} %d\n", metricName, metricName, n.ID, iv))
	}
	return sb.String()
}

// ReplicationLag is a practical, honestly-scoped proxy: hashicorp/raft
// doesn't expose true per-follower applied-index tracking through a
// public API (that lives in its internal leader replication state,
// beyond LeadershipTransfer/observer hooks this project uses). The proxy
// used here -- leader's last_log_index minus a given node's own
// applied_index -- is accurate for the common case (a node reporting its
// own honest progress) and is what a real replication-lag dashboard
// panel would plot; it is NOT a leader-authoritative measurement of what
// the leader itself believes each follower has acked. Worth stating
// plainly rather than implying more precision than this actually has.
func ReplicationLag(leader *Node, follower *Node) (int64, error) {
	leaderStats := leader.Raft.Stats()
	followerStats := follower.Raft.Stats()

	leaderLast, err := strconv.ParseInt(leaderStats["last_log_index"], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse leader last_log_index: %w", err)
	}
	followerApplied, err := strconv.ParseInt(followerStats["applied_index"], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse follower applied_index: %w", err)
	}

	lag := leaderLast - followerApplied
	if lag < 0 {
		lag = 0
	}
	return lag, nil
}