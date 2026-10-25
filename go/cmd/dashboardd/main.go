package main

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed index.html
var indexHTML []byte

type target struct{ name, url string }

type raftNode struct {
	Name              string `json:"name"`
	URL               string `json:"url"`
	Up                bool   `json:"up"`
	State             string `json:"state"`
	IsLeader          bool   `json:"is_leader"`
	Term              int64  `json:"term"`
	CommitIndex       int64  `json:"commit_index"`
	AppliedIndex      int64  `json:"applied_index"`
	ApplyLag          int64  `json:"apply_lag"`
	NumPeers          int64  `json:"num_peers"`
	LastSnapshotIndex int64  `json:"last_snapshot_index"`
}

var stateRe = regexp.MustCompile(`^cascade_raft_state\{node="([^"]*)",value="([^"]*)"\}`)

// parseTargets accepts "n1=http://h:9201/metrics,n2=..." (names optional).
func parseTargets(s string) []target {
	var out []target
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		t := target{url: part}
		if i := strings.Index(part, "="); i > 0 && !strings.ContainsAny(part[:i], "/:") {
			t.name, t.url = part[:i], part[i+1:]
		}
		out = append(out, t)
	}
	return out
}

func scrapeRaft(client *http.Client, t target) raftNode {
	n := raftNode{Name: t.name, URL: t.url}
	if n.Name == "" {
		n.Name = t.url
	}
	resp, err := client.Get(t.url)
	if err != nil {
		return n // connection refused => node is down
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return n
	}
	n.Up = true

	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if m := stateRe.FindStringSubmatch(line); m != nil {
			n.State = m[2]
			if t.name == "" {
				n.Name = m[1]
			}
			continue
		}
		i := strings.LastIndexByte(line, ' ')
		if i < 0 {
			continue
		}
		key := line[:i]
		if b := strings.IndexByte(key, '{'); b >= 0 {
			key = key[:b]
		}
		v, err := strconv.ParseInt(line[i+1:], 10, 64)
		if err != nil {
			continue
		}
		switch key {
		case "cascade_raft_is_leader":
			n.IsLeader = v == 1
		case "cascade_raft_term":
			n.Term = v
		case "cascade_raft_commit_index":
			n.CommitIndex = v
		case "cascade_raft_applied_index":
			n.AppliedIndex = v
		case "cascade_raft_apply_lag_entries":
			n.ApplyLag = v
		case "cascade_raft_num_peers":
			n.NumPeers = v
		case "cascade_raft_last_snapshot_index":
			n.LastSnapshotIndex = v
		}
	}
	return n
}

func main() {
	listen := flag.String("listen", ":8080", "dashboard listen address")
	target0 := flag.String("metrics-url", "http://127.0.0.1:9100/metrics", "broker Prometheus text endpoint")
	raftURLs := flag.String("raft-urls", "", "comma-separated raftd /metrics endpoints, optionally name=url (empty hides the Raft panel)")
	flag.Parse()

	client := &http.Client{Timeout: 2 * time.Second}
	raftClient := &http.Client{Timeout: 700 * time.Millisecond}
	raftTargets := parseTargets(*raftURLs)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	http.HandleFunc("/api/metrics", func(w http.ResponseWriter, _ *http.Request) {
		resp, err := client.Get(*target0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()

		out := map[string]float64{}
		sc := bufio.NewScanner(resp.Body)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			i := strings.LastIndexByte(line, ' ')
			if i < 0 {
				continue
			}
			if v, err := strconv.ParseFloat(line[i+1:], 64); err == nil {
				out[line[:i]] = v
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})

	// Scrapes every Raft node in parallel; a dead node comes back as up=false
	// instead of failing the whole request.
	http.HandleFunc("/api/raft", func(w http.ResponseWriter, _ *http.Request) {
		out := make([]raftNode, len(raftTargets))
		var wg sync.WaitGroup
		for i, t := range raftTargets {
			wg.Add(1)
			go func(i int, t target) {
				defer wg.Done()
				out[i] = scrapeRaft(raftClient, t)
			}(i, t)
		}
		wg.Wait()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})

	log.Printf("dashboard on %s, broker=%s, raft nodes=%d", *listen, *target0, len(raftTargets))
	log.Fatal(http.ListenAndServe(*listen, nil))
}