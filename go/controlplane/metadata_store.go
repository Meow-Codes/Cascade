package controlplane

import (
	"fmt"
	"sync"

	pb "cascade/gen/cascadepb"
)

// MetadataStore is the topic/partition registry -- the Go-side mirror of
// what the C++ Broker (Phase 4) tracks locally, except this is the
// cluster-wide source of truth once multiple C++ brokers exist. v1 (this
// phase): in-memory only, single control-plane process. Persisting this
// (so a control-plane restart doesn't forget topics) is explicitly a
// Phase 10 concern (snapshot sync), not solved here.
type MetadataStore struct {
	mu     sync.RWMutex
	topics map[string]*pb.Topic
}

func NewMetadataStore() *MetadataStore {
	return &MetadataStore{topics: make(map[string]*pb.Topic)}
}

func (s *MetadataStore) CreateTopic(name string, numPartitions int32) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.topics[name]; exists {
		return fmt.Errorf("topic already exists: %s", name)
	}
	if numPartitions <= 0 {
		return fmt.Errorf("num_partitions must be positive, got %d", numPartitions)
	}
	s.topics[name] = &pb.Topic{Name: name, NumPartitions: numPartitions}
	return nil
}

func (s *MetadataStore) ListTopics() []*pb.Topic {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*pb.Topic, 0, len(s.topics))
	for _, t := range s.topics {
		out = append(out, t)
	}
	return out
}
