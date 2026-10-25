package replicated

import (
	"context"
	"errors"
	"time"

	pb "cascade/gen/cascadepb"
	"cascade/raftnode"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// LeaderTrailerKey carries the leader's gRPC address on a FailedPrecondition
// "not leader" error. Trailer metadata (not a proto field) keeps every
// existing message unchanged.
const LeaderTrailerKey = "x-cascade-leader"

// Server implements CascadeControlPlane on top of a Raft node: writes are
// committed through consensus, reads are served from the local FSM
// (a follower read can briefly lag the leader).
type Server struct {
	pb.UnimplementedCascadeControlPlaneServer
	node         *raftnode.Node
	applyTimeout time.Duration
}

func NewServer(node *raftnode.Node, applyTimeout time.Duration) *Server {
	return &Server{node: node, applyTimeout: applyTimeout}
}

func (s *Server) leaderGRPCAddr() (string, bool) {
	_, id := s.node.Raft.LeaderWithID()
	if id == "" {
		return "", false
	}
	info, ok := s.node.FSM.LookupNode(string(id))
	if !ok || info.GRPCAddr == "" {
		return "", false
	}
	return info.GRPCAddr, true
}

func (s *Server) notLeader(ctx context.Context) error {
	if addr, ok := s.leaderGRPCAddr(); ok {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(LeaderTrailerKey, addr))
		return status.Errorf(codes.FailedPrecondition, "not leader; leader is at %s", addr)
	}
	return status.Error(codes.Unavailable, "no leader currently known (election in progress?)")
}

func (s *Server) propose(ctx context.Context, cmd raftnode.Command) (raftnode.ApplyResult, error) {
	cmd.TimestampMs = time.Now().UnixMilli() // leader-assigned => deterministic on every replica
	if !s.node.IsLeader() {
		return raftnode.ApplyResult{}, s.notLeader(ctx)
	}
	res, err := s.node.Propose(cmd, s.applyTimeout)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return raftnode.ApplyResult{}, s.notLeader(ctx)
		}
		return raftnode.ApplyResult{}, status.Errorf(codes.Unavailable, "raft apply failed: %v", err)
	}
	return res, nil
}

func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	res, err := s.propose(ctx, raftnode.Command{
		Kind: raftnode.CmdCreateTopic, TopicName: req.Name, NumPartitions: req.NumPartitions,
	})
	if err != nil {
		return nil, err
	}
	if res.Err != nil {
		return &pb.CreateTopicResponse{Created: false, Error: res.Err.Error()}, nil
	}
	return &pb.CreateTopicResponse{Created: true}, nil
}

func (s *Server) ListTopics(_ context.Context, _ *pb.Empty) (*pb.ListTopicsResponse, error) {
	return &pb.ListTopicsResponse{Topics: s.node.FSM.Metadata.ListTopics()}, nil
}

func (s *Server) RegisterBroker(ctx context.Context, req *pb.RegisterBrokerRequest) (*pb.RegisterBrokerResponse, error) {
	res, err := s.propose(ctx, raftnode.Command{
		Kind: raftnode.CmdRegisterBroker, BrokerID: req.BrokerId, Address: req.Address,
	})
	if err != nil {
		return nil, err
	}
	return &pb.RegisterBrokerResponse{Registered: res.Err == nil}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	res, err := s.propose(ctx, raftnode.Command{Kind: raftnode.CmdHeartbeat, BrokerID: req.BrokerId})
	if err != nil {
		return nil, err
	}
	return &pb.HeartbeatResponse{Acknowledged: res.Err == nil}, nil
}

func (s *Server) ListBrokers(_ context.Context, _ *pb.Empty) (*pb.ListBrokersResponse, error) {
	statuses := s.node.FSM.Membership.List()
	out := make([]*pb.BrokerInfo, 0, len(statuses))
	for _, st := range statuses {
		out = append(out, &pb.BrokerInfo{
			BrokerId: st.BrokerID, Address: st.Address, Alive: st.Alive,
			LastHeartbeatUnixMs: st.LastHeartbeatUnix,
		})
	}
	return &pb.ListBrokersResponse{Brokers: out}, nil
}

// JoinCluster adds a new voter. Leader only; followers answer with the
// leader redirect so a joiner can be pointed at any member.
func (s *Server) JoinCluster(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	if req.NodeId == "" || req.RaftAddress == "" || req.GrpcAddress == "" {
		return &pb.JoinClusterResponse{Error: "node_id, raft_address and grpc_address are required"}, nil
	}
	if !s.node.IsLeader() {
		return nil, s.notLeader(ctx)
	}
	f := s.node.Raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddress), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return nil, s.notLeader(ctx)
		}
		return nil, status.Errorf(codes.Unavailable, "AddVoter failed: %v", err)
	}
	res, err := s.propose(ctx, raftnode.Command{
		Kind: raftnode.CmdRegisterNode, NodeID: req.NodeId, RaftAddr: req.RaftAddress, GRPCAddr: req.GrpcAddress,
	})
	if err != nil {
		return nil, err
	}
	if res.Err != nil {
		return &pb.JoinClusterResponse{Error: res.Err.Error()}, nil
	}
	return &pb.JoinClusterResponse{Joined: true}, nil
}

// EnsureSelfRegistered makes sure this node's addresses are in the
// replicated registry. Needed for the bootstrap node (nobody "joins" it);
// harmless for everyone else. Returns once registered or ctx is done.
func (s *Server) EnsureSelfRegistered(ctx context.Context, self raftnode.NodeInfo) {
	for {
		if cur, ok := s.node.FSM.LookupNode(self.ID); ok && cur == self {
			return
		}
		if s.node.IsLeader() {
			_, _ = s.node.Propose(raftnode.Command{
				Kind: raftnode.CmdRegisterNode, NodeID: self.ID, RaftAddr: self.RaftAddr, GRPCAddr: self.GRPCAddr,
			}, s.applyTimeout)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}