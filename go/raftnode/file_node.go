package raftnode

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"cascade/controlplane"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// NewFileNode creates a disk-backed raft node: BoltDB-backed log/stable
// stores, a file-backed snapshot store, and a real TCP transport.
// Unlike NewInmemNode (test/demo cluster only, Phase 9), this node's
// state genuinely survives process restart -- it lives in files under
// dataDir, not in the creating process's heap. This is what makes
// "automatic recovery" a real, testable claim rather than an assumption:
// a fresh process pointed at the same dataDir recovers via raft's own
// startup log/snapshot replay, with zero involvement from any peer.
func NewFileNode(id, bindAddr, dataDir string) (*Node, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	fsm := &FSM{
		Metadata:   controlplane.NewMetadataStore(),
		Membership: controlplane.NewMembership(controlplane.RealClock{}, 10*time.Second),
	}

	boltPath := filepath.Join(dataDir, "raft.db")
	store, err := raftboltdb.New(raftboltdb.Options{Path: boltPath})
	if err != nil {
		return nil, fmt.Errorf("open boltdb store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("open file snapshot store: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("resolve addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("new tcp transport: %w", err)
	}

	// HasExistingState is the deciding signal for whether this process
	// is a fresh start (needs BootstrapCluster) or a RECOVERY (must NOT
	// bootstrap again -- that would attempt to re-seed configuration over
	// real persisted history, which raft correctly rejects).
	hasState, err := raft.HasExistingState(store, store, snapshotStore)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, fmt.Errorf("check existing state: %w", err)
	}

	r, err := raft.NewRaft(newConfig(id), fsm, store, store, snapshotStore, transport)
	// fmt.Println("counter after NewRaft:", fsm.CurrentCounter())
	if err != nil {
		transport.Close()
		store.Close()
		return nil, fmt.Errorf("raft.NewRaft(%s): %w", id, err)
	}

	node := &Node{
		Raft: r, FSM: fsm, ID: id,
		closeFns: []func() error{transport.Close, store.Close},
	}

	if !hasState {
		bootstrapFuture := r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{ID: raft.ServerID(id), Address: raft.ServerAddress(bindAddr)}},
		})
		if err := bootstrapFuture.Error(); err != nil {
			node.Close()
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}
	// hasState == true: this IS the recovery path. raft.NewRaft already
	// replayed the on-disk log/snapshot into fsm during construction
	// above -- by the time we reach here, fsm.Metadata/Membership/counter
	// already reflect everything that was committed before the process
	// that wrote this dataDir exited. No further action needed.
	// fmt.Printf("FSM = %p\n", fsm)
	return node, nil
}

// BootstrapSingleFileNode is a thin convenience wrapper: NewFileNode
// already handles the "bootstrap only if no existing state" logic
// internally, so this exists mainly to make test/demo call sites read
// clearly as "first ever start of this node."
func BootstrapSingleFileNode(id, bindAddr, dataDir string) (*Node, error) {
	return NewFileNode(id, bindAddr, dataDir)
}