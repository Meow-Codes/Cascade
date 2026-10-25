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

type FileNodeOptions struct {
	ID       string
	BindAddr string // raft transport bind AND advertise address, e.g. 127.0.0.1:7001 (must not be 0.0.0.0)
	DataDir  string
	// Bootstrap seeds a brand-new single-voter cluster, but only when there
	// is no existing state. Ignored on restart. Joiners leave this false and
	// are added by the leader (see replicated.Server.JoinCluster).
	Bootstrap bool
	Config    *raft.Config // nil => newConfig(ID) (fast, test-scale timings)
}

// NewFileNode: original single-node entry point, kept for existing tests.
func NewFileNode(id, bindAddr, dataDir string) (*Node, error) {
	return NewFileNodeWithOptions(FileNodeOptions{ID: id, BindAddr: bindAddr, DataDir: dataDir, Bootstrap: true})
}

func NewFileNodeWithOptions(opts FileNodeOptions) (*Node, error) {
	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	fsm := &FSM{
		Metadata:   controlplane.NewMetadataStore(),
		Membership: controlplane.NewMembership(controlplane.RealClock{}, 10*time.Second),
	}

	store, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(opts.DataDir, "raft.db")})
	if err != nil {
		return nil, fmt.Errorf("open boltdb store: %w", err)
	}

	snapshotStore, err := raft.NewFileSnapshotStore(opts.DataDir, 2, os.Stderr)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("open file snapshot store: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", opts.BindAddr)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("resolve addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(opts.BindAddr, addr, 3, 5*time.Second, os.Stderr)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("new tcp transport: %w", err)
	}

	hasState, err := raft.HasExistingState(store, store, snapshotStore)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, fmt.Errorf("check existing state: %w", err)
	}

	cfg := opts.Config
	if cfg == nil {
		cfg = newConfig(opts.ID)
	}
	cfg.LocalID = raft.ServerID(opts.ID)

	r, err := raft.NewRaft(cfg, fsm, store, store, snapshotStore, transport)
	if err != nil {
		transport.Close()
		store.Close()
		return nil, fmt.Errorf("raft.NewRaft(%s): %w", opts.ID, err)
	}

	node := &Node{
		Raft: r, FSM: fsm, ID: opts.ID, Recovered: hasState,
		closeFns: []func() error{transport.Close, store.Close},
	}

	// hasState == true is the recovery path: NewRaft already replayed the
	// on-disk snapshot + log into fsm. Re-bootstrapping would be rejected.
	if !hasState && opts.Bootstrap {
		f := r.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{{ID: raft.ServerID(opts.ID), Address: raft.ServerAddress(opts.BindAddr)}},
		})
		if err := f.Error(); err != nil {
			node.Close()
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}
	return node, nil
}

func BootstrapSingleFileNode(id, bindAddr, dataDir string) (*Node, error) {
	return NewFileNode(id, bindAddr, dataDir)
}