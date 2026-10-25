package raftnode

// CHECKLIST for new replicated state: it must be handled in ALL THREE of
// Apply(), Snapshot() and Restore(). (The counter missed Snapshot/Restore
// once and a snapshot-joined replica silently came up with 0.)

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"cascade/controlplane"
	pb "cascade/gen/cascadepb"

	"github.com/hashicorp/raft"
)

// NodeInfo is a cluster member's addresses. Replicating it lets any node
// translate "the raft leader is n2" into "n2's gRPC address is X" for
// client redirects.
type NodeInfo struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	GRPCAddr string `json:"grpc_addr"`
}

type FSM struct {
	Metadata   *controlplane.MetadataStore
	Membership *controlplane.Membership

	counterMu sync.Mutex
	counter   int64

	nodesMu sync.RWMutex
	nodes   map[string]NodeInfo
}

type CommandKind string

const (
	CmdCreateTopic      CommandKind = "create_topic"
	CmdRegisterBroker   CommandKind = "register_broker"
	CmdHeartbeat        CommandKind = "heartbeat"
	CmdIncrementCounter CommandKind = "increment_counter"
	CmdRegisterNode     CommandKind = "register_node"
)

type Command struct {
	Kind          CommandKind `json:"kind"`
	TopicName     string      `json:"topic_name,omitempty"`
	NumPartitions int32       `json:"num_partitions,omitempty"`
	BrokerID      string      `json:"broker_id,omitempty"`
	Address       string      `json:"address,omitempty"`
	NodeID        string      `json:"node_id,omitempty"`
	RaftAddr      string      `json:"raft_addr,omitempty"`
	GRPCAddr      string      `json:"grpc_addr,omitempty"`
	// Assigned by the leader before proposing. 0 = not set (falls back to
	// the local clock, which is only correct for tests).
	TimestampMs int64 `json:"timestamp_ms,omitempty"`
}

func EncodeCommand(c Command) ([]byte, error) { return json.Marshal(c) }

type ApplyResult struct {
	Err   error
	Value int64
}

func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return ApplyResult{Err: fmt.Errorf("malformed command: %w", err)}
	}

	switch cmd.Kind {
	case CmdCreateTopic:
		return ApplyResult{Err: f.Metadata.CreateTopic(cmd.TopicName, cmd.NumPartitions)}
	case CmdRegisterBroker:
		if cmd.TimestampMs != 0 {
			f.Membership.RegisterAt(cmd.BrokerID, cmd.Address, time.UnixMilli(cmd.TimestampMs))
		} else {
			f.Membership.Register(cmd.BrokerID, cmd.Address)
		}
		return ApplyResult{}
	case CmdHeartbeat:
		var ok bool
		if cmd.TimestampMs != 0 {
			ok = f.Membership.HeartbeatAt(cmd.BrokerID, time.UnixMilli(cmd.TimestampMs))
		} else {
			ok = f.Membership.Heartbeat(cmd.BrokerID)
		}
		if !ok {
			return ApplyResult{Err: fmt.Errorf("heartbeat on unregistered broker: %s", cmd.BrokerID)}
		}
		return ApplyResult{}
	case CmdIncrementCounter:
		f.counterMu.Lock()
		f.counter++
		newVal := f.counter
		f.counterMu.Unlock()
		return ApplyResult{Value: newVal}
	case CmdRegisterNode:
		f.nodesMu.Lock()
		if f.nodes == nil {
			f.nodes = make(map[string]NodeInfo)
		}
		f.nodes[cmd.NodeID] = NodeInfo{ID: cmd.NodeID, RaftAddr: cmd.RaftAddr, GRPCAddr: cmd.GRPCAddr}
		f.nodesMu.Unlock()
		return ApplyResult{}
	default:
		return ApplyResult{Err: fmt.Errorf("unknown command kind: %s", cmd.Kind)}
	}
}

func (f *FSM) CurrentCounter() int64 {
	f.counterMu.Lock()
	defer f.counterMu.Unlock()
	return f.counter
}

func (f *FSM) LookupNode(id string) (NodeInfo, bool) {
	f.nodesMu.RLock()
	defer f.nodesMu.RUnlock()
	n, ok := f.nodes[id]
	return n, ok
}

func (f *FSM) Nodes() []NodeInfo {
	f.nodesMu.RLock()
	defer f.nodesMu.RUnlock()
	out := make([]NodeInfo, 0, len(f.nodes))
	for _, n := range f.nodes {
		out = append(out, n)
	}
	return out
}

type fsmSnapshot struct {
	Topics  []*pb.Topic                        `json:"topics"`
	Brokers []controlplane.BrokerSnapshotEntry `json:"brokers"`
	Counter int64                              `json:"counter"`
	Nodes   []NodeInfo                         `json:"nodes"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{
		Topics:  f.Metadata.SnapshotTopics(),
		Brokers: f.Membership.SnapshotBrokers(),
		Counter: f.CurrentCounter(),
		Nodes:   f.Nodes(),
	}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var snap fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return err
	}
	f.Metadata.RestoreTopics(snap.Topics)
	f.Membership.RestoreBrokers(snap.Brokers)

	f.counterMu.Lock()
	f.counter = snap.Counter
	f.counterMu.Unlock()

	f.nodesMu.Lock()
	f.nodes = make(map[string]NodeInfo, len(snap.Nodes))
	for _, n := range snap.Nodes {
		f.nodes[n.ID] = n
	}
	f.nodesMu.Unlock()
	return nil
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s)
	if err != nil {
		sink.Cancel()
		return err
	}
	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}