package main

import (
	// "crypto/tls"
	"crypto/x509"
	"flag"
	"log"
	"net"
	"os"
	"time"

	"cascade/controlplane"
	pb "cascade/gen/cascadepb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	certFile := flag.String("cert", "", "TLS server certificate (PEM). Empty = plaintext (dev only).")
	keyFile := flag.String("key", "", "TLS server private key (PEM). Required if -cert is set.")
	flag.Parse()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	server := controlplane.NewServer(controlplane.RealClock{}, 10*time.Second)

	var grpcServer *grpc.Server
	if *certFile != "" {
		if *keyFile == "" {
			log.Fatal("-key is required when -cert is set")
		}
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("failed to load TLS credentials: %v", err)
		}
		grpcServer = grpc.NewServer(grpc.Creds(creds))
		log.Println("cascade control plane listening on :50051 (TLS enabled)")
	} else {
		grpcServer = grpc.NewServer()
		log.Println("cascade control plane listening on :50051 (PLAINTEXT -- dev only, do not use in production)")
	}

	pb.RegisterCascadeControlPlaneServer(grpcServer, server)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve failed: %v", err)
	}
	_ = x509.NewCertPool // referenced by contract_test.go's TLS variant below; keep import intact if unused here
	_ = os.Stdout
}