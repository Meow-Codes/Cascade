package raftnode
import "github.com/hashicorp/raft"

type rAddr = raft.ServerAddress

// Network partition simulation over hashicorp/raft's InmemTransport.
// Disconnect()/Connect() sever/restore the in-memory link between two
// named peers -- the in-memory equivalent of what `tc`/`netem` would do
// at the OS level for a real TCP-based deployment. Good enough to prove
// the actual invariant Phase 9 cares about (no split-brain writes across
// a partition), without needing real network namespaces.

func nodeByID(nodes []*Node, id string) *Node {
	for _, n := range nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

// Partition splits the cluster into two groups: every cross-group link
// is severed in both directions. Nodes within the same group can still
// reach each other.
func Partition(nodes []*Node, groupA, groupB []string) {
	for _, aID := range groupA {
		a := nodeByID(nodes, aID)
		for _, bID := range groupB {
			a.Transport.Disconnect(serverAddr(bID))
		}
	}
	for _, bID := range groupB {
		b := nodeByID(nodes, bID)
		for _, aID := range groupA {
			b.Transport.Disconnect(serverAddr(aID))
		}
	}
}

// Heal restores every pairwise link -- the partition ends.
func Heal(nodes []*Node) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a.ID != b.ID {
				a.Transport.Connect(serverAddr(b.ID), b.Transport)
			}
		}
	}
}

func serverAddr(id string) rAddr { return rAddr(id) }