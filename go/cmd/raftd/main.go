package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pb "cascade/gen/cascadepb"
	"cascade/raftnode"
	"cascade/replicated"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func clientCreds(caFile string) (credentials.TransportCredentials, error) {
	if caFile == "" {
		return insecure.NewCredentials(), nil
	}
	return credentials.NewClientTLSFromFile(caFile, "")
}

func main() {
	id := flag.String("id", "", "unique node id (required)")
	raftAddr := flag.String("raft-addr", "127.0.0.1:7001", "raft transport bind+advertise address (not 0.0.0.0)")
	grpcAddr := flag.String("grpc-addr", "127.0.0.1:50051", "client-facing gRPC listen+advertise address")
	metricsAddr := flag.String("metrics-addr", "127.0.0.1:9201", "Prometheus /metrics address (empty disables)")
	dataDir := flag.String("data-dir", "", "state directory (default ./data/<id>)")
	bootstrap := flag.Bool("bootstrap", false, "first node of a NEW cluster (ignored if state exists)")
	join := flag.String("join", "", "gRPC address(es) of an existing member, comma-separated (ignored if state exists)")
	certFile := flag.String("cert", "", "gRPC server TLS cert (empty = plaintext)")
	keyFile := flag.String("key", "", "gRPC server TLS key")
	caFile := flag.String("ca", "", "CA cert for dialing the cluster when joining over TLS")
	flag.Parse()

	if *id == "" {
		log.Fatal("-id is required")
	}
	if *bootstrap && *join != "" {
		log.Fatal("-bootstrap and -join are mutually exclusive")
	}
	if *dataDir == "" {
		*dataDir = "./data/" + *id
	}

	node, err := raftnode.NewFileNodeWithOptions(raftnode.FileNodeOptions{
		ID: *id, BindAddr: *raftAddr, DataDir: *dataDir,
		Bootstrap: *bootstrap, Config: raftnode.DaemonConfig(*id),
	})
	if err != nil {
		log.Fatalf("start raft node: %v", err)
	}

	switch {
	case node.Recovered:
		log.Printf("recovered raft state from %s (ignoring -bootstrap/-join)", *dataDir)
	case !*bootstrap && *join == "":
		node.Close()
		log.Fatal("fresh node: pass -bootstrap (first node of a new cluster) or -join <grpc-addr>")
	}

	var serverOpts []grpc.ServerOption
	if *certFile != "" {
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("load TLS credentials: %v", err)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	grpcServer := grpc.NewServer(serverOpts...)
	srv := replicated.NewServer(node, 5*time.Second)
	pb.RegisterCascadeControlPlaneServer(grpcServer, srv)

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("listen %s: %v", *grpcAddr, err)
	}
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc serve: %v", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if !node.Recovered && *join != "" {
		cc, err := clientCreds(*caFile)
		if err != nil {
			log.Fatalf("load CA: %v", err)
		}
		client := replicated.NewClient(strings.Split(*join, ","), cc)
		client.Timeout = 15 * time.Second // AddVoter waits for the config change to commit
		joinCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		resp, err := client.Join(joinCtx, &pb.JoinClusterRequest{
			NodeId: *id, RaftAddress: *raftAddr, GrpcAddress: *grpcAddr,
		})
		cancel()
		if err != nil {
			log.Fatalf("join via %s: %v", *join, err)
		}
		if !resp.Joined {
			log.Fatalf("join rejected: %s", resp.Error)
		}
		log.Printf("joined cluster via %s", *join)
	}

	go srv.EnsureSelfRegistered(ctx, raftnode.NodeInfo{ID: *id, RaftAddr: *raftAddr, GRPCAddr: *grpcAddr})

	var ms *raftnode.MetricsServer
	if *metricsAddr != "" {
		ms = raftnode.NewMetricsServer(node, *metricsAddr)
		go func() {
			if err := ms.Start(); err != nil && err != http.ErrServerClosed {
				log.Printf("metrics server: %v", err)
			}
		}()
	}

	log.Printf("raftd %s up: raft=%s grpc=%s metrics=%s", *id, *raftAddr, *grpcAddr, *metricsAddr)
	<-ctx.Done()

	log.Printf("shutting down")
	done := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		grpcServer.Stop()
	}
	if ms != nil {
		ms.Stop()
	}
	if err := node.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}