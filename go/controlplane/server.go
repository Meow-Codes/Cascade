package controlplane

import (
	"context"
	"time"

	pb "cascade/gen/cascadepb"
)

// Server implements the generated CascadeControlPlaneServer interface,
// wiring the two pieces above (MetadataStore, Membership) to gRPC. Kept
// deliberately thin -- all real logic lives in the two structs above so
// they stay unit-testable without spinning up gRPC at all (see
// membership_test.go), while contract_test.go separately proves the gRPC
// wiring itself is correct.
type Server struct {
	pb.UnimplementedCascadeControlPlaneServer
	metadata   *MetadataStore
	membership *Membership
}

func NewServer(clock Clock, heartbeatTimeout time.Duration) *Server {
	return &Server{
		metadata:   NewMetadataStore(),
		membership: NewMembership(clock, heartbeatTimeout),
	}
}

func (s *Server) CreateTopic(_ context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	err := s.metadata.CreateTopic(req.Name, req.NumPartitions)
	if err != nil {
		return &pb.CreateTopicResponse{Created: false, Error: err.Error()}, nil
	}
	return &pb.CreateTopicResponse{Created: true}, nil
}

func (s *Server) ListTopics(_ context.Context, _ *pb.Empty) (*pb.ListTopicsResponse, error) {
	return &pb.ListTopicsResponse{Topics: s.metadata.ListTopics()}, nil
}

func (s *Server) RegisterBroker(_ context.Context, req *pb.RegisterBrokerRequest) (*pb.RegisterBrokerResponse, error) {
	s.membership.Register(req.BrokerId, req.Address)
	return &pb.RegisterBrokerResponse{Registered: true}, nil
}

func (s *Server) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	ok := s.membership.Heartbeat(req.BrokerId)
	return &pb.HeartbeatResponse{Acknowledged: ok}, nil
}

func (s *Server) ListBrokers(_ context.Context, _ *pb.Empty) (*pb.ListBrokersResponse, error) {
	statuses := s.membership.List()
	out := make([]*pb.BrokerInfo, 0, len(statuses))
	for _, st := range statuses {
		out = append(out, &pb.BrokerInfo{
			BrokerId:            st.BrokerID,
			Address:              st.Address,
			Alive:                 st.Alive,
			LastHeartbeatUnixMs: st.LastHeartbeatUnix,
		})
	}
	return &pb.ListBrokersResponse{Brokers: out}, nil
}