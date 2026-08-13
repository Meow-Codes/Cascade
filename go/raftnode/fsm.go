package raftnode

import (
	"encoding/json"
	"fmt"
	"io"

	"cascade/controlplane"
	pb "cascade/gen/cascadepb"

	"github.com/hashicorp/raft"
	"sync"
)

// FSM applies replicated commands to the same MetadataStore/Membership
// structs Server (Phase 8) already uses directly -- Raft doesn't replace
// those, it decides the ORDER in which commands reach them, consistently
// across every node in the cluster.
type FSM struct {
	Metadata   *controlplane.MetadataStore
	Membership *controlplane.Membership

	counterMu sync.Mutex
	counter   int64
}

type CommandKind string

const (
	CmdCreateTopic       CommandKind = "create_topic"
	CmdRegisterBroker    CommandKind = "register_broker"
	CmdHeartbeat         CommandKind = "heartbeat"
	CmdIncrementCounter  CommandKind = "increment_counter"
)

type Command struct {
	Kind          CommandKind `json:"kind"`
	TopicName     string      `json:"topic_name,omitempty"`
	NumPartitions int32       `json:"num_partitions,omitempty"`
	BrokerID      string      `json:"broker_id,omitempty"`
	Address       string      `json:"address,omitempty"`
}

func EncodeCommand(c Command) ([]byte, error) { return json.Marshal(c) }

// ApplyResult is what Apply() returns via raft.ApplyFuture.Response().
// raft.Apply()'s own error only reports replication/commit failure --
// callers still need to know if the FSM itself rejected the command
// (e.g. duplicate topic), which is what this carries.
type ApplyResult struct {
	Err error
	Value int64 // populated by CmdIncrementCounter: the counter's new value after this op
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
		f.Membership.Register(cmd.BrokerID, cmd.Address)
		return ApplyResult{Err: nil}
	case CmdHeartbeat:
		if ok := f.Membership.Heartbeat(cmd.BrokerID); !ok {
			return ApplyResult{Err: fmt.Errorf("heartbeat on unregistered broker: %s", cmd.BrokerID)}
		}
		return ApplyResult{Err: nil}\
	case CmdIncrementCounter:
		f.counterMu.Lock()
		f.counter++
		newVal := f.counter
		f.counterMu.Unlock()
		return ApplyResult{Value: newVal}
	default:
		return ApplyResult{Err: fmt.Errorf("unknown command kind: %s", cmd.Kind)}
	}
}

func (f *FSM) CurrentCounter() int64 {
	f.counterMu.Lock()
	defer f.counterMu.Unlock()
	return f.counter
}

type fsmSnapshot struct {
	Topics  []*pb.Topic                        `json:"topics"`
	Brokers []controlplane.BrokerSnapshotEntry `json:"brokers"`
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{
		Topics:  f.Metadata.SnapshotTopics(),
		Brokers: f.Membership.SnapshotBrokers(),
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