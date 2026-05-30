//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// Package main is a reusable load-test harness for conditional-write
// performance benchmarking. It measures p50/p95/p99 latency, throughput
// (ops/s), and error rate across the conditional-write topology matrix
// defined in the Workstream A kickoff brief (parent task 2026-05-30-0510).
//
// Modes
//
//	insert_miss    — all-new UUIDs (insert_if_not_exists, 100 % miss path)
//	insert_hit     — pre-seeded UUIDs (insert_if_not_exists, 100 % hit path)
//	insert_mixed   — alternating miss/hit 50/50
//	update_exists  — update_if_exists on pre-seeded objects
//	unconditional  — plain POST /v1/objects (no ?condition=) as baseline
//	hot_key        — all workers hammer ONE UUID (per-UUID-lock contention)
//
// Topology matrix (how the overseer invokes this per topology):
// see README.md in this directory.
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---- wire types (minimal, no dependency on internal packages) ----

type weaviateObject struct {
	Class      string                 `json:"class"`
	ID         string                 `json:"id,omitempty"`
	Properties map[string]interface{} `json:"properties"`
	Tenant     string                 `json:"tenant,omitempty"`
}

type weaviateClass struct {
	Class              string              `json:"class"`
	Vectorizer         string              `json:"vectorizer"`
	ReplicationConfig  *replicationConfig  `json:"replicationConfig,omitempty"`
	ShardingConfig     *shardingConfig     `json:"shardingConfig,omitempty"`
	MultiTenancyConfig *multiTenancyConfig `json:"multiTenancyConfig,omitempty"`
}

type replicationConfig struct {
	Factor int `json:"factor"`
}

type shardingConfig struct {
	DesiredCount int `json:"desiredCount"`
}

type multiTenancyConfig struct {
	Enabled bool `json:"enabled"`
}

type tenantPayload struct {
	Name   string `json:"name"`
	Status string `json:"activityStatus"`
}

// ---- result types ----

// Result is the machine-readable output written to --output-file.
type Result struct {
	Timestamp   string  `json:"timestamp"`
	Mode        string  `json:"mode"`
	URL         string  `json:"url"`
	Class       string  `json:"class"`
	Objects     int     `json:"objects_requested"`
	Concurrency int     `json:"concurrency"`
	Shards      int     `json:"shards"`
	RF          int     `json:"replication_factor"`
	Tenants     int     `json:"tenants"`
	CL          string  `json:"consistency_level"`
	DurationS   float64 `json:"duration_s"`

	OpsTotal   int64   `json:"ops_total"`
	OpsPerSec  float64 `json:"ops_per_sec"`
	ErrorCount int64   `json:"error_count"`
	ErrorRate  float64 `json:"error_rate_pct"`

	P50Ms float64 `json:"p50_ms"`
	P95Ms float64 `json:"p95_ms"`
	P99Ms float64 `json:"p99_ms"`
	MinMs float64 `json:"min_ms"`
	MaxMs float64 `json:"max_ms"`
}

// ---- flags ----

type config struct {
	url          string
	mode         string
	className    string
	objects      int
	concurrency  int
	shards       int
	rf           int
	tenants      int
	cl           string
	outputFormat string
	outputFile   string
	skipSetup    bool
	skipTeardown bool
	hotKeyUUID   string
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.url, "url", "http://localhost:8080", "Weaviate base URL (e.g. http://localhost:8080)")
	flag.StringVar(&cfg.mode, "mode", "insert_miss",
		"Workload mode: insert_miss | insert_hit | insert_mixed | update_exists | unconditional | hot_key")
	flag.StringVar(&cfg.className, "class", "CWBench", "Weaviate class/collection name")
	flag.IntVar(&cfg.objects, "objects", 10000, "Number of write operations to perform (per run)")
	flag.IntVar(&cfg.concurrency, "concurrency", 16, "Number of concurrent writers")
	flag.IntVar(&cfg.shards, "shards", 1, "Desired shard count for the class")
	flag.IntVar(&cfg.rf, "rf", 1, "Replication factor")
	flag.IntVar(&cfg.tenants, "tenants", 0, "Number of MT tenants (0 = MT disabled)")
	flag.StringVar(&cfg.cl, "cl", "", "Consistency level: ONE | QUORUM | ALL (empty = server default)")
	flag.StringVar(&cfg.outputFormat, "output-format", "json", "Output format: json | csv | human")
	flag.StringVar(&cfg.outputFile, "output-file", "", "File to write results to ('' = stdout)")
	flag.BoolVar(&cfg.skipSetup, "skip-setup", false, "Skip class/schema creation and pre-seeding")
	flag.BoolVar(&cfg.skipTeardown, "skip-teardown", false, "Do not DELETE the class after the run")
	flag.StringVar(&cfg.hotKeyUUID, "hot-key-uuid", "aaaaaaaa-bbbb-cccc-dddd-000000000001",
		"UUID used in hot_key mode (all workers hammer this one object)")
	flag.Parse()
	return cfg
}

// ---- HTTP helpers ----

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 512,
			MaxIdleConns:        512,
			IdleConnTimeout:     90 * time.Second,
		},
	}
}

func doRequest(client *http.Client, method, url string, body []byte) (int, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewBuffer(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, url, bodyReader)
	if err != nil {
		return 0, fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()
	return resp.StatusCode, nil
}

// ---- schema / setup helpers ----

func deleteClass(client *http.Client, base, class string) {
	url := fmt.Sprintf("%s/v1/schema/%s", base, class)
	_, _ = doRequest(client, http.MethodDelete, url, nil)
}

func createClass(client *http.Client, base string, cfg config) error {
	cls := weaviateClass{
		Class:             cfg.className,
		Vectorizer:        "none",
		ReplicationConfig: &replicationConfig{Factor: cfg.rf},
		ShardingConfig:    &shardingConfig{DesiredCount: cfg.shards},
	}
	if cfg.tenants > 0 {
		cls.MultiTenancyConfig = &multiTenancyConfig{Enabled: true}
	}
	body, err := json.Marshal(cls)
	if err != nil {
		return fmt.Errorf("marshal class: %w", err)
	}
	code, err := doRequest(client, http.MethodPost, base+"/v1/schema", body)
	if err != nil {
		return fmt.Errorf("create class: %w", err)
	}
	if code != 200 && code != 201 {
		return fmt.Errorf("create class returned %d", code)
	}
	return nil
}

func addTenants(client *http.Client, base, class string, n int) error {
	tenants := make([]tenantPayload, n)
	for i := range tenants {
		tenants[i] = tenantPayload{
			Name:   fmt.Sprintf("tenant-%04d", i),
			Status: "HOT",
		}
	}
	body, err := json.Marshal(tenants)
	if err != nil {
		return fmt.Errorf("marshal tenants: %w", err)
	}
	url := fmt.Sprintf("%s/v1/schema/%s/tenants", base, class)
	code, err := doRequest(client, http.MethodPost, url, body)
	if err != nil {
		return fmt.Errorf("add tenants: %w", err)
	}
	if code != 200 && code != 201 {
		return fmt.Errorf("add tenants returned %d", code)
	}
	return nil
}

// seedObjects inserts `n` objects unconditionally so that hit/update modes
// have data to find. Returns the slice of UUIDs inserted.
func seedObjects(client *http.Client, cfg config, n int) ([]string, error) {
	fmt.Fprintf(os.Stderr, "seeding %d objects for mode %s...\n", n, cfg.mode)
	uuids := make([]string, n)
	for i := range uuids {
		uuids[i] = generateUUID(i)
	}

	// Determine tenant to use for seeding (tenant-0 when MT enabled)
	tenant := ""
	if cfg.tenants > 0 {
		tenant = "tenant-0000"
	}

	errCount := 0
	for _, id := range uuids {
		obj := weaviateObject{
			Class:      cfg.className,
			ID:         id,
			Properties: map[string]interface{}{"seq": id},
			Tenant:     tenant,
		}
		body, _ := json.Marshal(obj)
		url := cfg.url + "/v1/objects"
		if tenant != "" {
			url += "?tenant=" + tenant
		}
		code, err := doRequest(client, http.MethodPost, url, body)
		if err != nil || (code != 200 && code != 201) {
			errCount++
		}
	}
	if errCount > 0 {
		fmt.Fprintf(os.Stderr, "warning: %d/%d seed writes failed\n", errCount, n)
	}
	return uuids, nil
}

// ---- UUID generation (deterministic, no dependency) ----

// generateUUID produces a deterministic UUID-shaped string from an integer.
func generateUUID(i int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i)
}

// randomUUID generates a random UUID v4.
func randomUUID(rng *rand.Rand) string {
	var b [16]byte
	_, _ = rng.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ---- stats ----

type stats struct {
	mu      sync.Mutex
	samples []float64 // latencies in ms
}

func (s *stats) record(d time.Duration) {
	ms := float64(d.Microseconds()) / 1000.0
	s.mu.Lock()
	s.samples = append(s.samples, ms)
	s.mu.Unlock()
}

func (s *stats) compute() (p50, p95, p99, minMs, maxMs float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return
	}
	sorted := make([]float64, len(s.samples))
	copy(sorted, s.samples)
	sort.Float64s(sorted)

	n := len(sorted)
	percentile := func(p float64) float64 {
		idx := int(float64(n-1)*p/100.0 + 0.5)
		if idx >= n {
			idx = n - 1
		}
		return sorted[idx]
	}
	return percentile(50), percentile(95), percentile(99), sorted[0], sorted[n-1]
}

// ---- workload builder ----

// buildRequestURL returns the URL for a single write operation given the mode.
func buildRequestURL(base, mode, cl, tenant string) string {
	switch mode {
	case "insert_miss", "insert_hit", "insert_mixed", "hot_key":
		u := base + "/v1/objects?condition=insert_if_not_exists"
		if cl != "" {
			u += "&consistency_level=" + cl
		}
		if tenant != "" {
			u += "&tenant=" + tenant
		}
		return u
	case "update_exists":
		u := base + "/v1/objects?condition=update_if_exists"
		if cl != "" {
			u += "&consistency_level=" + cl
		}
		if tenant != "" {
			u += "&tenant=" + tenant
		}
		return u
	default: // unconditional
		u := base + "/v1/objects"
		params := []string{}
		if cl != "" {
			params = append(params, "consistency_level="+cl)
		}
		if tenant != "" {
			params = append(params, "tenant="+tenant)
		}
		if len(params) > 0 {
			u += "?" + strings.Join(params, "&")
		}
		return u
	}
}

// worker drives ops for a single goroutine.
func worker(
	ctx context.Context,
	client *http.Client,
	cfg config,
	seededUUIDs []string,
	st *stats,
	opsTotal, errTotal *int64,
	opsTarget int,
	workerID int,
) {
	rng := rand.New(rand.NewSource(int64(workerID) * 999983))

	tenant := ""
	if cfg.tenants > 0 {
		tenant = fmt.Sprintf("tenant-%04d", workerID%cfg.tenants)
	}

	url := buildRequestURL(cfg.url, cfg.mode, cfg.cl, tenant)

	for {
		if ctx.Err() != nil {
			return
		}
		current := atomic.AddInt64(opsTotal, 1)
		if current > int64(opsTarget) {
			atomic.AddInt64(opsTotal, -1)
			return
		}

		// Determine the UUID for this request based on mode.
		var id string
		switch cfg.mode {
		case "insert_miss":
			// Always a new UUID — guaranteed miss.
			id = randomUUID(rng)
		case "insert_hit":
			// Pick from pre-seeded UUIDs — guaranteed hit.
			if len(seededUUIDs) > 0 {
				id = seededUUIDs[rng.Intn(len(seededUUIDs))]
			} else {
				id = randomUUID(rng)
			}
		case "insert_mixed":
			// 50/50 miss/hit.
			if rng.Intn(2) == 0 {
				id = randomUUID(rng)
			} else if len(seededUUIDs) > 0 {
				id = seededUUIDs[rng.Intn(len(seededUUIDs))]
			} else {
				id = randomUUID(rng)
			}
		case "update_exists":
			// Must be an existing object.
			if len(seededUUIDs) > 0 {
				id = seededUUIDs[rng.Intn(len(seededUUIDs))]
			} else {
				id = randomUUID(rng)
			}
		case "hot_key":
			// All writers use the same UUID — maximise per-UUID-lock contention.
			id = cfg.hotKeyUUID
		default: // unconditional
			id = randomUUID(rng)
		}

		obj := weaviateObject{
			Class:      cfg.className,
			ID:         id,
			Properties: map[string]interface{}{"seq": current, "worker": workerID},
		}
		if tenant != "" {
			obj.Tenant = tenant
		}
		body, _ := json.Marshal(obj)

		start := time.Now()
		code, err := doRequest(client, http.MethodPost, url, body)
		elapsed := time.Since(start)

		if err != nil || code >= 500 {
			atomic.AddInt64(errTotal, 1)
		}
		st.record(elapsed)
	}
}

// ---- output ----

func writeResults(cfg config, res Result) error {
	var out io.Writer = os.Stdout
	if cfg.outputFile != "" {
		f, err := os.Create(cfg.outputFile)
		if err != nil {
			return fmt.Errorf("open output file: %w", err)
		}
		defer f.Close()
		out = f
	}

	switch cfg.outputFormat {
	case "json":
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	case "csv":
		w := csv.NewWriter(out)
		_ = w.Write([]string{
			"timestamp", "mode", "url", "class",
			"objects_requested", "concurrency", "shards", "rf", "tenants", "cl",
			"duration_s", "ops_total", "ops_per_sec", "error_count", "error_rate_pct",
			"p50_ms", "p95_ms", "p99_ms", "min_ms", "max_ms",
		})
		_ = w.Write([]string{
			res.Timestamp, res.Mode, res.URL, res.Class,
			strconv.Itoa(res.Objects), strconv.Itoa(res.Concurrency),
			strconv.Itoa(res.Shards), strconv.Itoa(res.RF), strconv.Itoa(res.Tenants), res.CL,
			fmt.Sprintf("%.3f", res.DurationS),
			strconv.FormatInt(res.OpsTotal, 10),
			fmt.Sprintf("%.1f", res.OpsPerSec),
			strconv.FormatInt(res.ErrorCount, 10),
			fmt.Sprintf("%.2f", res.ErrorRate),
			fmt.Sprintf("%.2f", res.P50Ms),
			fmt.Sprintf("%.2f", res.P95Ms),
			fmt.Sprintf("%.2f", res.P99Ms),
			fmt.Sprintf("%.2f", res.MinMs),
			fmt.Sprintf("%.2f", res.MaxMs),
		})
		w.Flush()
		return w.Error()
	default: // human
		fmt.Fprintf(out, "\n=== Conditional Write Benchmark Results ===\n")
		fmt.Fprintf(out, "Mode:        %s\n", res.Mode)
		fmt.Fprintf(out, "URL:         %s\n", res.URL)
		fmt.Fprintf(out, "Class:       %s\n", res.Class)
		fmt.Fprintf(out, "Objects:     %d\n", res.Objects)
		fmt.Fprintf(out, "Concurrency: %d\n", res.Concurrency)
		fmt.Fprintf(out, "Shards/RF:   %d/%d\n", res.Shards, res.RF)
		fmt.Fprintf(out, "Tenants:     %d\n", res.Tenants)
		fmt.Fprintf(out, "CL:          %s\n", res.CL)
		fmt.Fprintf(out, "-------------------------------------------\n")
		fmt.Fprintf(out, "Duration:    %.3f s\n", res.DurationS)
		fmt.Fprintf(out, "Ops total:   %d\n", res.OpsTotal)
		fmt.Fprintf(out, "Throughput:  %.1f ops/s\n", res.OpsPerSec)
		fmt.Fprintf(out, "Errors:      %d (%.2f %%)\n", res.ErrorCount, res.ErrorRate)
		fmt.Fprintf(out, "p50 latency: %.2f ms\n", res.P50Ms)
		fmt.Fprintf(out, "p95 latency: %.2f ms\n", res.P95Ms)
		fmt.Fprintf(out, "p99 latency: %.2f ms\n", res.P99Ms)
		fmt.Fprintf(out, "min latency: %.2f ms\n", res.MinMs)
		fmt.Fprintf(out, "max latency: %.2f ms\n", res.MaxMs)
		return nil
	}
}

// ---- main ----

func main() {
	cfg := parseFlags()

	client := newHTTPClient()

	// ---- Setup ----
	var seededUUIDs []string

	if !cfg.skipSetup {
		fmt.Fprintf(os.Stderr, "deleting existing class %s (if any)...\n", cfg.className)
		deleteClass(client, cfg.url, cfg.className)
		time.Sleep(200 * time.Millisecond)

		fmt.Fprintf(os.Stderr, "creating class %s (shards=%d, RF=%d, tenants=%d)...\n",
			cfg.className, cfg.shards, cfg.rf, cfg.tenants)
		if err := createClass(client, cfg.url, cfg); err != nil {
			fmt.Fprintf(os.Stderr, "fatal: create class: %v\n", err)
			os.Exit(1)
		}

		if cfg.tenants > 0 {
			fmt.Fprintf(os.Stderr, "adding %d tenants...\n", cfg.tenants)
			if err := addTenants(client, cfg.url, cfg.className, cfg.tenants); err != nil {
				fmt.Fprintf(os.Stderr, "fatal: add tenants: %v\n", err)
				os.Exit(1)
			}
		}

		// Pre-seed for modes that need existing data.
		switch cfg.mode {
		case "insert_hit", "insert_mixed", "update_exists":
			// Seed half the requested object count as the existing pool.
			seedN := cfg.objects / 2
			if seedN < 1 {
				seedN = 1
			}
			var err error
			seededUUIDs, err = seedObjects(client, cfg, seedN)
			if err != nil {
				fmt.Fprintf(os.Stderr, "fatal: seed: %v\n", err)
				os.Exit(1)
			}
		case "hot_key":
			// Seed the hot-key object so update_if_exists would find it;
			// insert_if_not_exists on a miss is fine too, but seeding ensures
			// contention starts from the very first op.
			seededUUIDs = []string{cfg.hotKeyUUID}
			obj := weaviateObject{
				Class:      cfg.className,
				ID:         cfg.hotKeyUUID,
				Properties: map[string]interface{}{"seq": 0},
			}
			body, _ := json.Marshal(obj)
			_, _ = doRequest(client, http.MethodPost, cfg.url+"/v1/objects", body)
		}
	}

	// ---- Run ----
	fmt.Fprintf(os.Stderr, "starting load: mode=%s objects=%d concurrency=%d\n",
		cfg.mode, cfg.objects, cfg.concurrency)

	var (
		opsTotal int64
		errTotal int64
		st       stats
		wg       sync.WaitGroup
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()

	perWorker := cfg.objects
	for i := 0; i < cfg.concurrency; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			worker(ctx, client, cfg, seededUUIDs, &st, &opsTotal, &errTotal, perWorker, workerID)
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)

	actualOps := atomic.LoadInt64(&opsTotal)
	actualErr := atomic.LoadInt64(&errTotal)
	p50, p95, p99, minMs, maxMs := st.compute()
	opsPerSec := float64(actualOps) / elapsed.Seconds()
	errRate := 0.0
	if actualOps > 0 {
		errRate = float64(actualErr) / float64(actualOps) * 100.0
	}

	res := Result{
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		Mode:        cfg.mode,
		URL:         cfg.url,
		Class:       cfg.className,
		Objects:     cfg.objects,
		Concurrency: cfg.concurrency,
		Shards:      cfg.shards,
		RF:          cfg.rf,
		Tenants:     cfg.tenants,
		CL:          cfg.cl,
		DurationS:   elapsed.Seconds(),
		OpsTotal:    actualOps,
		OpsPerSec:   opsPerSec,
		ErrorCount:  actualErr,
		ErrorRate:   errRate,
		P50Ms:       p50,
		P95Ms:       p95,
		P99Ms:       p99,
		MinMs:       minMs,
		MaxMs:       maxMs,
	}

	if err := writeResults(cfg, res); err != nil {
		fmt.Fprintf(os.Stderr, "write results: %v\n", err)
		os.Exit(1)
	}

	// ---- Teardown ----
	if !cfg.skipTeardown {
		fmt.Fprintf(os.Stderr, "deleting class %s...\n", cfg.className)
		deleteClass(client, cfg.url, cfg.className)
	}

	// Exit non-zero on high error rate so the overseer can detect failures.
	if errRate > 10.0 {
		fmt.Fprintf(os.Stderr, "error rate %.2f%% exceeds 10%% threshold\n", errRate)
		os.Exit(2)
	}
}
