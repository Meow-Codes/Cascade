package controlplane_test

import (
	"testing"
	"time"

	"cascade/controlplane"
)

func TestMembership_BrokerAliveAfterRegisterBeforeTimeout(t *testing.T) {
	clock := controlplane.NewFakeClock(time.Unix(1000, 0))
	m := controlplane.NewMembership(clock, 5*time.Second)

	m.Register("broker-1", "127.0.0.1:9000")
	clock.Advance(3 * time.Second)

	statuses := m.List()
	if len(statuses) != 1 || !statuses[0].Alive {
		t.Fatalf("expected broker-1 alive, got %+v", statuses)
	}
}

func TestMembership_BrokerDeadAfterTimeoutWithNoHeartbeat(t *testing.T) {
	clock := controlplane.NewFakeClock(time.Unix(1000, 0))
	m := controlplane.NewMembership(clock, 5*time.Second)

	m.Register("broker-1", "127.0.0.1:9000")
	clock.Advance(6 * time.Second) // past the 5s timeout, no heartbeat sent

	statuses := m.List()
	if len(statuses) != 1 || statuses[0].Alive {
		t.Fatalf("expected broker-1 dead, got %+v", statuses)
	}
}

func TestMembership_HeartbeatResetsTimeout(t *testing.T) {
	clock := controlplane.NewFakeClock(time.Unix(1000, 0))
	m := controlplane.NewMembership(clock, 5*time.Second)

	m.Register("broker-1", "127.0.0.1:9000")
	clock.Advance(4 * time.Second)
	if ok := m.Heartbeat("broker-1"); !ok {
		t.Fatal("heartbeat on registered broker should succeed")
	}
	clock.Advance(4 * time.Second) // 8s total elapsed, but only 4s since last heartbeat

	statuses := m.List()
	if len(statuses) != 1 || !statuses[0].Alive {
		t.Fatalf("expected broker-1 still alive after heartbeat reset, got %+v", statuses)
	}
}

func TestMembership_HeartbeatOnUnregisteredBrokerFails(t *testing.T) {
	clock := controlplane.NewFakeClock(time.Unix(1000, 0))
	m := controlplane.NewMembership(clock, 5*time.Second)

	if ok := m.Heartbeat("never-registered"); ok {
		t.Fatal("expected heartbeat on unregistered broker to fail")
	}
}

func TestMetadataStore_RejectsDuplicateTopic(t *testing.T) {
	s := controlplane.NewMetadataStore()
	if err := s.CreateTopic("events", 4); err != nil {
		t.Fatalf("first create should succeed: %v", err)
	}
	if err := s.CreateTopic("events", 2); err == nil {
		t.Fatal("expected error creating duplicate topic")
	}
}

func TestMetadataStore_RejectsNonPositivePartitionCount(t *testing.T) {
	s := controlplane.NewMetadataStore()
	if err := s.CreateTopic("bad", 0); err == nil {
		t.Fatal("expected error for zero partitions")
	}
}