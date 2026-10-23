package main

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//go:embed index.html
var indexHTML []byte

// dashboardd serves the dashboard page and a same-origin /api/metrics that
// scrapes the broker's Prometheus text endpoint and returns it as flat
// JSON. Proxying server-side sidesteps CORS: the C++ HttpMetricsServer
// sends no Access-Control headers, and it shouldn't have to.
func main() {
	listen := flag.String("listen", ":8080", "dashboard listen address")
	target := flag.String("metrics-url", "http://127.0.0.1:9100/metrics", "Prometheus text endpoint to scrape")
	flag.Parse()

	client := &http.Client{Timeout: 2 * time.Second}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})

	http.HandleFunc("/api/metrics", func(w http.ResponseWriter, _ *http.Request) {
		resp, err := client.Get(*target)
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
				out[line[:i]] = v // key keeps labels, e.g. name_bucket{le="100"}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})

	log.Printf("dashboard on %s, scraping %s", *listen, *target)
	log.Fatal(http.ListenAndServe(*listen, nil))
}