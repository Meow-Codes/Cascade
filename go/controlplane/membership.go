package controlplane

import (
	"sync"
	"time"
)

// Membership tracks which C++ brokers are alive via heartbeats -- the
// Go-side equivalent of the C++ ConnectionManager (Phase 2), same
// register/heartbeat/timeout shape, reimplemented here because this
// tracks broker-to-control-plane liveness (cluster membership) rather
// than client-to-broker connection liveness. Deliberately does NOT share
// code with the C++ ConnectionManager -- different language, different
// process, and the failure being detected is conceptually different
// (a whole broker process/node vs. one client connection).
type BrokerRecord struct {
	BrokerID      string
	Address       string
	LastHeartbeat time.Time
}

type Membership struct {
	mu       sync.RWMutex
	brokers  map[string]*BrokerRecord
	clock    Clock
	timeout  time.Duration
}

func NewMembership(clock Clock, timeout time.Duration) *Membership {
	return &Membership{
		brokers: make(map[string]*BrokerRecord),
		clock:   clock,
		timeout: timeout,
	}
}

func (m *Membership) Register(brokerID, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.brokers[brokerID] = &BrokerRecord{
		BrokerID:      brokerID,
		Address:       address,
		LastHeartbeat: m.clock.Now(),
	}
}

// Heartbeat returns false if brokerID was never registered -- caller
// (the gRPC handler) turns that into an error response rather than
// silently creating a phantom broker record.
func (m *Membership) Heartbeat(brokerID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.brokers[brokerID]
	if !ok {
		return false
	}
	rec.LastHeartbeat = m.clock.Now()
	return true
}

type BrokerStatus struct {
	BrokerID          string
	Address           string
	Alive             bool
	LastHeartbeatUnix int64
}

// List reports every registered broker with a computed Alive flag based
// on m.clock.Now() minus LastHeartbeat vs m.timeout. This is a pull
// (computed on read) rather than push (a background sweeper marking
// brokers dead) model -- simpler, and avoids needing a goroutine with its
// own shutdown lifecycle for what's fundamentally the same information
// available on demand.
func (m *Membership) List() []BrokerStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	now := m.clock.Now()
	out := make([]BrokerStatus, 0, len(m.brokers))
	for _, rec := range m.brokers {
		alive := now.Sub(rec.LastHeartbeat) < m.timeout
		out = append(out, BrokerStatus{
			BrokerID:          rec.BrokerID,
			Address:           rec.Address,
			Alive:             alive,
			LastHeartbeatUnix: rec.LastHeartbeat.UnixMilli(),
		})
	}
	return out
}