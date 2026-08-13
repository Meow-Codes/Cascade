package main

import (
	"log"
	"net"
	"time"

	"cascade/controlplane"
	pb "cascade/gen/cascadepb"

	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := controlplane.NewServer(controlplane.RealClock{}, 10*time.Second)
	grpcServer := grpc.NewServer()
	pb.RegisterCascadeControlPlaneServer(grpcServer, server)

	log.Println("cascade control plane listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
}