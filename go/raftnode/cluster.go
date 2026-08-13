package raftnode

import (
	"errors"
	"fmt"
	"io"
	"time"

	"cascade/controlplane"

	"github.com/hashicorp/raft"
)

// Node bundles a raft.Raft instance with the FSM it drives.
// NewInmemNode/BootstrapInmemCluster are used by tests and would also
// back an all-in-one-process demo; a real multi-machine deployment would
// add an equivalent NewTCPNode using raft.NewTCPTransport, sharing the
// same FSM/config wiring so there's exactly one code path to get right.
type Node struct {
	Raft      *raft.Raft
	FSM       *FSM
	ID        string
	Transport *raft.InmemTransport // exposed so tests can simulate network partitions
}

func newConfig(id string) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	// Short timeouts so tests/demos don't wait on raft's normal
	// multi-second production election timers.
	cfg.HeartbeatTimeout = 100 * time.Millisecond
	cfg.ElectionTimeout = 100 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 10 * time.Millisecond
	cfg.LogOutput = io.Discard // raft's internal logging is noisy at test scale; silence it
	return cfg
}

func NewInmemNode(id string, transport *raft.InmemTransport) (*Node, error) {
	fsm := &FSM{
		Metadata:   controlplane.NewMetadataStore(),
		Membership: controlplane.NewMembership(controlplane.RealClock{}, 10*time.Second),
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(newConfig(id), fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("raft.NewRaft(%s): %w", id, err)
	}
	return &Node{Raft: r, FSM: fsm, ID: id, Transport: transport}, nil
}

// BootstrapInmemCluster creates a fully-connected N-node cluster over
// in-memory transports and bootstraps the initial raft configuration on
// the first node -- the standard hashicorp/raft bootstrap pattern for a
// fresh cluster.
func BootstrapInmemCluster(ids []string) ([]*Node, error) {
	transports := make(map[string]*raft.InmemTransport, len(ids))
	for _, id := range ids {
		_, transport := raft.NewInmemTransport(raft.ServerAddress(id))
		transports[id] = transport
	}
	for _, a := range ids {
		for _, b := range ids {
			if a != b {
				transports[a].Connect(raft.ServerAddress(b), transports[b])
			}
		}
	}

	nodes := make([]*Node, 0, len(ids))
	for _, id := range ids {
		n, err := NewInmemNode(id, transports[id])
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}

	servers := make([]raft.Server, 0, len(ids))
	for _, id := range ids {
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(id)})
	}
	bootstrapFuture := nodes[0].Raft.BootstrapCluster(raft.Configuration{Servers: servers})
	if err := bootstrapFuture.Error(); err != nil {
		return nil, fmt.Errorf("bootstrap: %w", err)
	}

	return nodes, nil
}

var ErrNoLeaderElected = errors.New("no leader elected within timeout")

// WaitForLeader polls until some node in the cluster reports a non-empty
// leader, or timeout elapses. Fine for tests/demos; a real client would
// use leader-hint responses from RPCs instead of polling raft state
// directly (see Propose's ErrNotLeader note below).
func WaitForLeader(nodes []*Node, timeout time.Duration) (string, error) {
    deadline := time.Now().Add(timeout)

    for time.Now().Before(deadline) {
        for _, n := range nodes {
            if n.Raft.State() == raft.Leader {
                return n.ID, nil
            }
        }
        time.Sleep(10 * time.Millisecond)
    }

    return "", ErrNoLeaderElected
}

func (n *Node) IsLeader() bool { return n.Raft.State() == raft.Leader }

// Propose applies a command through Raft consensus. Must be called on
// the leader -- a follower's Apply() returns raft.ErrNotLeader via the
// future's Error(), which is exactly the signal a real gRPC layer would
// use to implement "leader-aware client routing" (Phase 9's third build
// item): catch ErrNotLeader, look up n.Raft.Leader() for the current
// leader's address, and redirect the client there.
func (n *Node) Propose(cmd Command, timeout time.Duration) (ApplyResult, error) {
	data, err := EncodeCommand(cmd)
	if err != nil {
		return ApplyResult{}, err
	}
	future := n.Raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return ApplyResult{}, err
	}
	resp := future.Response()
	result, ok := resp.(ApplyResult)
	if !ok {
		return ApplyResult{}, errors.New("unexpected FSM response type")
	}
	return result, nil
}