package raftnode

import (
	"fmt"
	"net/http"
)

// MetricsServer serves Prometheus-format stats for one Node on GET
// /metrics -- the Go-side equivalent of Phase 5's C++ HttpMetricsServer.
// Deliberately uses Go's standard net/http rather than hand-rolling a raw
// socket listener the way Phase 5 had to on the C++ side: Go's
// http.Server already handles concurrent requests/keep-alive correctly,
// so there's no Phase-5-style "framed protocol vs raw HTTP" conflict to
// route around here -- this package has no framed protocol of its own.
type MetricsServer struct {
	node   *Node
	server *http.Server
}

func NewMetricsServer(node *Node, addr string) *MetricsServer {
	mux := http.NewServeMux()
	ms := &MetricsServer{node: node}
	mux.HandleFunc("/metrics", ms.handleMetrics)
	ms.server = &http.Server{Addr: addr, Handler: mux}
	return ms
}

func (ms *MetricsServer) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprint(w, ms.node.RenderPrometheusStats())
}

// Start blocks -- run in a goroutine.
func (ms *MetricsServer) Start() error { return ms.server.ListenAndServe() }
func (ms *MetricsServer) Stop() error  { return ms.server.Close() }