package raftnode

import (
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// AddNodeToCluster joins a brand-new, empty node to an ALREADY RUNNING
// cluster -- the scenario Phase 10's "snapshot synchronization for fast
// replica catch-up" describes: a replica joining after the cluster
// already has significant committed history, needing to catch up via
// snapshot install rather than replaying the entire log from index 0.
func AddNodeToCluster(nodes []*Node, newID string, leader *Node, timeout time.Duration) (*Node, error) {
	_, newTransport := raft.NewInmemTransport(raft.ServerAddress(newID))

	for _, existing := range nodes {
		newTransport.Connect(raft.ServerAddress(existing.ID), existing.Transport)
		existing.Transport.Connect(raft.ServerAddress(newID), newTransport)
	}

	newNode, err := NewInmemNode(newID, newTransport)
	if err != nil {
		return nil, fmt.Errorf("creating new node %s: %w", newID, err)
	}

	addFuture := leader.Raft.AddVoter(raft.ServerID(newID), raft.ServerAddress(newID), 0, timeout)
	if err := addFuture.Error(); err != nil {
		return nil, fmt.Errorf("AddVoter(%s): %w", newID, err)
	}

	return newNode, nil
}