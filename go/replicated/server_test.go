package replicated_test

import (
	"context"
	"net"
	"testing"
	"time"

	pb "cascade/gen/cascadepb"
	"cascade/raftnode"
	"cascade/replicated"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type testCluster struct {
	nodes map[string]*raftnode.Node
	addrs map[string]string // node id -> gRPC address
	stops map[string]func()
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func newTestCluster(t *testing.T) *testCluster {
	t.Helper()
	nodes, err := raftnode.BootstrapInmemCluster([]string{"n1", "n2", "n3"})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	tc := &testCluster{nodes: map[string]*raftnode.Node{}, addrs: map[string]string{}, stops: map[string]func(){}}

	for _, n := range nodes {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		gs := grpc.NewServer()
		pb.RegisterCascadeControlPlaneServer(gs, replicated.NewServer(n, 2*time.Second))
		go gs.Serve(lis)
		tc.nodes[n.ID], tc.addrs[n.ID], tc.stops[n.ID] = n, lis.Addr().String(), gs.Stop
	}
	t.Cleanup(func() {
		for _, stop := range tc.stops {
			stop()
		}
		for _, n := range nodes {
			n.Raft.Shutdown()
		}
	})

	leaderID, err := raftnode.WaitForLeader(nodes, 3*time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}
	for id, addr := range tc.addrs {
		res, err := tc.nodes[leaderID].Propose(raftnode.Command{
			Kind: raftnode.CmdRegisterNode, NodeID: id, RaftAddr: id, GRPCAddr: addr,
		}, 2*time.Second)
		if err != nil || res.Err != nil {
			t.Fatalf("register node %s: %v %v", id, err, res.Err)
		}
	}
	waitFor(t, 3*time.Second, "every FSM knows all 3 nodes", func() bool {
		for _, n := range nodes {
			if len(n.FSM.Nodes()) != 3 {
				return false
			}
		}
		return true
	})
	return tc
}

func (tc *testCluster) leaderID(t *testing.T) string {
	t.Helper()
	for id, n := range tc.nodes {
		if n.IsLeader() {
			return id
		}
	}
	t.Fatal("no leader")
	return ""
}

func (tc *testCluster) followerID(t *testing.T) string {
	t.Helper()
	l := tc.leaderID(t)
	for id := range tc.nodes {
		if id != l {
			return id
		}
	}
	t.Fatal("no follower")
	return ""
}

func TestFollowerReturnsLeaderHintInTrailer(t *testing.T) {
	tc := newTestCluster(t)
	leader, follower := tc.leaderID(t), tc.followerID(t)

	conn, err := grpc.NewClient(tc.addrs[follower], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	var trailer metadata.MD
	_, err = pb.NewCascadeControlPlaneClient(conn).CreateTopic(context.Background(),
		&pb.CreateTopicRequest{Name: "x", NumPartitions: 1}, grpc.Trailer(&trailer))

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FailedPrecondition from a follower, got %v", err)
	}
	hint := trailer.Get(replicated.LeaderTrailerKey)
	if len(hint) != 1 || hint[0] != tc.addrs[leader] {
		t.Fatalf("want leader hint %s, got %v", tc.addrs[leader], hint)
	}
}

func TestClientFollowsRedirectFromFollower(t *testing.T) {
	tc := newTestCluster(t)
	follower := tc.followerID(t)

	// Client knows ONLY a follower; it must reach the leader via the redirect.
	client := replicated.NewClient([]string{tc.addrs[follower]}, nil)
	resp, err := client.CreateTopic(context.Background(), "via-follower", 2)
	if err != nil || !resp.Created {
		t.Fatalf("create via follower: %v %+v", err, resp)
	}
	waitFor(t, 2*time.Second, "topic replicated to every node", func() bool {
		for _, n := range tc.nodes {
			if len(n.FSM.Metadata.ListTopics()) != 1 {
				return false
			}
		}
		return true
	})
}

func TestClientSurvivesLeaderLoss(t *testing.T) {
	tc := newTestCluster(t)
	endpoints := []string{tc.addrs["n1"], tc.addrs["n2"], tc.addrs["n3"]}
	client := replicated.NewClient(endpoints, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if resp, err := client.CreateTopic(ctx, "before-failover", 1); err != nil || !resp.Created {
		t.Fatalf("pre-failover create: %v %+v", err, resp)
	}

	old := tc.leaderID(t)
	tc.stops[old]()
	if err := tc.nodes[old].Raft.Shutdown().Error(); err != nil {
		t.Fatalf("shutdown leader: %v", err)
	}

	resp, err := client.CreateTopic(ctx, "after-failover", 1)
	if err != nil || !resp.Created {
		t.Fatalf("post-failover create: %v %+v", err, resp)
	}

	for id, n := range tc.nodes {
		if id == old {
			continue
		}
		have := map[string]bool{}
		for _, tp := range n.FSM.Metadata.ListTopics() {
			have[tp.Name] = true
		}
		if !have["before-failover"] {
			t.Errorf("node %s lost the pre-failover topic", id)
		}
	}
}