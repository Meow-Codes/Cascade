package controlplane

import "time"

// RegisterAt / HeartbeatAt apply a caller-supplied timestamp instead of
// reading the clock. The Raft FSM uses these with the leader-assigned
// timestamp carried in each command, so every replica (and every log
// replay after a restart) computes the same LastHeartbeat.

func (m *Membership) RegisterAt(brokerID, address string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.brokers[brokerID] = &BrokerRecord{BrokerID: brokerID, Address: address, LastHeartbeat: at}
}

// HeartbeatAt returns false for an unregistered broker. Timestamps never
// move backwards, so a reordered replay can't make a broker look staler.
func (m *Membership) HeartbeatAt(brokerID string, at time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.brokers[brokerID]
	if !ok {
		return false
	}
	if at.After(rec.LastHeartbeat) {
		rec.LastHeartbeat = at
	}
	return true
}