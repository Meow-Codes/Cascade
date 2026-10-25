package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"cascade/replicated"

	"google.golang.org/grpc/credentials"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "comma-separated control plane endpoints")
	ca := flag.String("ca", "", "CA cert PEM for TLS (empty = plaintext)")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: cascadectl [-addr a,b,c] [-ca ca.pem] topics | brokers | create-topic <name> <partitions>")
		os.Exit(2)
	}

	var creds credentials.TransportCredentials
	if *ca != "" {
		c, err := credentials.NewClientTLSFromFile(*ca, "")
		if err != nil {
			fmt.Fprintln(os.Stderr, "load CA:", err)
			os.Exit(1)
		}
		creds = c
	}
	client := replicated.NewClient(strings.Split(*addr, ","), creds)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	switch args[0] {
	case "topics":
		resp, err := client.ListTopics(ctx)
		check(err)
		for _, t := range resp.Topics {
			fmt.Printf("name=%s partitions=%d\n", t.Name, t.NumPartitions)
		}
	case "brokers":
		resp, err := client.ListBrokers(ctx)
		check(err)
		for _, b := range resp.Brokers {
			fmt.Printf("id=%s address=%s alive=%t last_heartbeat_unix_ms=%d\n",
				b.BrokerId, b.Address, b.Alive, b.LastHeartbeatUnixMs)
		}
	case "create-topic":
		if len(args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: cascadectl create-topic <name> <partitions>")
			os.Exit(2)
		}
		n, err := strconv.Atoi(args[2])
		check(err)
		resp, err := client.CreateTopic(ctx, args[1], int32(n))
		check(err)
		if !resp.Created {
			fmt.Fprintln(os.Stderr, "rejected:", resp.Error)
			os.Exit(1)
		}
		fmt.Println("created")
	default:
		fmt.Fprintln(os.Stderr, "unknown command:", args[0])
		os.Exit(2)
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}