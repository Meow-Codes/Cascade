package controlplane

// Snapshot/Restore support for raftnode.FSM, added in Phase 9. Kept in
// its own file rather than editing metadata_store.go/membership.go so
// the original Phase 8 files stay untouched and independently reviewable.

import (
	"time"

	pb "cascade/gen/cascadepb"
)

func (s *MetadataStore) SnapshotTopics() []*pb.Topic {
	return s.ListTopics() // already returns a fresh copy under RLock
}

func (s *MetadataStore) RestoreTopics(topics []*pb.Topic) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics = make(map[string]*pb.Topic, len(topics))
	for _, t := range topics {
		s.topics[t.Name] = t
	}
}

type BrokerSnapshotEntry struct {
	BrokerID          string `json:"broker_id"`
	Address           string `json:"address"`
	LastHeartbeatUnix int64  `json:"last_heartbeat_unix_ms"`
}

func (m *Membership) SnapshotBrokers() []BrokerSnapshotEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]BrokerSnapshotEntry, 0, len(m.brokers))
	for _, rec := range m.brokers {
		out = append(out, BrokerSnapshotEntry{
			BrokerID:          rec.BrokerID,
			Address:           rec.Address,
			LastHeartbeatUnix: rec.LastHeartbeat.UnixMilli(),
		})
	}
	return out
}

func (m *Membership) RestoreBrokers(entries []BrokerSnapshotEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.brokers = make(map[string]*BrokerRecord, len(entries))
	for _, e := range entries {
		m.brokers[e.BrokerID] = &BrokerRecord{
			BrokerID:      e.BrokerID,
			Address:       e.Address,
			LastHeartbeat: time.UnixMilli(e.LastHeartbeatUnix),
		}
	}
}