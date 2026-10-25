package replicated

import (
	"context"
	"errors"
	"time"

	pb "cascade/gen/cascadepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Client talks to a raftd cluster: follows "not leader" redirects and
// rotates through endpoints when one is unreachable. Connects per attempt,
// which is fine for a CLI/join helper and not meant for high-throughput use.
type Client struct {
	endpoints []string
	creds     credentials.TransportCredentials
	Timeout   time.Duration // per-RPC timeout
}

func NewClient(endpoints []string, creds credentials.TransportCredentials) *Client {
	if creds == nil {
		creds = insecure.NewCredentials()
	}
	return &Client{endpoints: endpoints, creds: creds, Timeout: 2 * time.Second}
}

type rpcFunc func(ctx context.Context, cl pb.CascadeControlPlaneClient, opts ...grpc.CallOption) error

func (c *Client) try(ctx context.Context, target string, fn rpcFunc) (string, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(c.creds))
	if err != nil {
		return "", status.Errorf(codes.Unavailable, "dial %s: %v", target, err)
	}
	defer conn.Close()

	callCtx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	var trailer metadata.MD
	err = fn(callCtx, pb.NewCascadeControlPlaneClient(conn), grpc.Trailer(&trailer))
	hint := ""
	if v := trailer.Get(LeaderTrailerKey); len(v) > 0 {
		hint = v[0]
	}
	return hint, err
}

func (c *Client) do(ctx context.Context, fn rpcFunc) error {
	if len(c.endpoints) == 0 {
		return errors.New("no endpoints configured")
	}
	idx, target := 0, c.endpoints[0]
	var lastErr error

	for attempt := 0; attempt < 40; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		hint, err := c.try(ctx, target, fn)
		if err == nil {
			return nil
		}
		lastErr = err

		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.FailedPrecondition:
			if hint != "" && hint != target {
				target = hint // redirect straight to the leader
				continue
			}
			fallthrough
		case codes.Unavailable, codes.DeadlineExceeded:
			idx = (idx + 1) % len(c.endpoints)
			target = c.endpoints[idx]
			if idx == 0 { // tried everyone once: give an election time to finish
				select {
				case <-ctx.Done():
				case <-time.After(300 * time.Millisecond):
				}
			}
		default:
			return err // application-level error: don't retry
		}
	}
	return lastErr
}

func (c *Client) CreateTopic(ctx context.Context, name string, partitions int32) (*pb.CreateTopicResponse, error) {
	var resp *pb.CreateTopicResponse
	err := c.do(ctx, func(ctx context.Context, cl pb.CascadeControlPlaneClient, opts ...grpc.CallOption) (e error) {
		resp, e = cl.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name, NumPartitions: partitions}, opts...)
		return e
	})
	return resp, err
}

func (c *Client) ListTopics(ctx context.Context) (*pb.ListTopicsResponse, error) {
	var resp *pb.ListTopicsResponse
	err := c.do(ctx, func(ctx context.Context, cl pb.CascadeControlPlaneClient, opts ...grpc.CallOption) (e error) {
		resp, e = cl.ListTopics(ctx, &pb.Empty{}, opts...)
		return e
	})
	return resp, err
}

func (c *Client) ListBrokers(ctx context.Context) (*pb.ListBrokersResponse, error) {
	var resp *pb.ListBrokersResponse
	err := c.do(ctx, func(ctx context.Context, cl pb.CascadeControlPlaneClient, opts ...grpc.CallOption) (e error) {
		resp, e = cl.ListBrokers(ctx, &pb.Empty{}, opts...)
		return e
	})
	return resp, err
}

func (c *Client) Join(ctx context.Context, req *pb.JoinClusterRequest) (*pb.JoinClusterResponse, error) {
	var resp *pb.JoinClusterResponse
	err := c.do(ctx, func(ctx context.Context, cl pb.CascadeControlPlaneClient, opts ...grpc.CallOption) (e error) {
		resp, e = cl.JoinCluster(ctx, req, opts...)
		return e
	})
	return resp, err
}