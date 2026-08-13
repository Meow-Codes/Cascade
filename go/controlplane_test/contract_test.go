package controlplane_test

import (
	"context"
	"net"
	"testing"
	"time"

	"cascade/controlplane"
	pb "cascade/gen/cascadepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// bufconn gives an in-memory gRPC connection -- no real TCP port, no
// flakiness from port collisions between test runs, and it exercises the
// REAL generated gRPC client/server stubs (not just calling Server's Go
// methods directly, which membership_test.go already covers). This is
// what actually proves the wire contract, not just the logic behind it.
func startTestServer(t *testing.T, clock controlplane.Clock) pb.CascadeControlPlaneClient {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	pb.RegisterCascadeControlPlaneServer(grpcServer, controlplane.NewServer(clock, 5*time.Second))

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("bufconn server exited: %v", err)
		}
	}()
	t.Cleanup(grpcServer.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return pb.NewCascadeControlPlaneClient(conn)
}

func TestContract_CreateAndListTopics(t *testing.T) {
	client := startTestServer(t, controlplane.RealClock{})
	ctx := context.Background()

	resp, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "events", NumPartitions: 4})
	if err != nil || !resp.Created {
		t.Fatalf("CreateTopic failed: err=%v resp=%+v", err, resp)
	}

	list, err := client.ListTopics(ctx, &pb.Empty{})
	if err != nil {
		t.Fatalf("ListTopics failed: %v", err)
	}
	if len(list.Topics) != 1 || list.Topics[0].Name != "events" || list.Topics[0].NumPartitions != 4 {
		t.Fatalf("unexpected topics: %+v", list.Topics)
	}
}

func TestContract_DuplicateTopicReturnsErrorInResponseNotRpcError(t *testing.T) {
	client := startTestServer(t, controlplane.RealClock{})
	ctx := context.Background()

	client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "dup", NumPartitions: 1})
	resp, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: "dup", NumPartitions: 1})
	if err != nil {
		t.Fatalf("expected a normal gRPC response carrying the error, not a transport error: %v", err)
	}
	if resp.Created || resp.Error == "" {
		t.Fatalf("expected Created=false with an error message, got %+v", resp)
	}
}

func TestContract_RegisterHeartbeatAndListBrokers(t *testing.T) {
	client := startTestServer(t, controlplane.RealClock{})
	ctx := context.Background()

	regResp, err := client.RegisterBroker(ctx, &pb.RegisterBrokerRequest{BrokerId: "b1", Address: "127.0.0.1:9000"})
	if err != nil || !regResp.Registered {
		t.Fatalf("RegisterBroker failed: err=%v resp=%+v", err, regResp)
	}

	hbResp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{BrokerId: "b1"})
	if err != nil || !hbResp.Acknowledged {
		t.Fatalf("Heartbeat failed: err=%v resp=%+v", err, hbResp)
	}

	list, err := client.ListBrokers(ctx, &pb.Empty{})
	if err != nil {
		t.Fatalf("ListBrokers failed: %v", err)
	}
	if len(list.Brokers) != 1 || list.Brokers[0].BrokerId != "b1" || !list.Brokers[0].Alive {
		t.Fatalf("unexpected broker list: %+v", list.Brokers)
	}
}

func TestContract_HeartbeatOnUnknownBrokerNotAcknowledged(t *testing.T) {
	client := startTestServer(t, controlplane.RealClock{})
	ctx := context.Background()

	resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{BrokerId: "ghost"})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.Acknowledged {
		t.Fatal("expected heartbeat on unknown broker to NOT be acknowledged")
	}
}
