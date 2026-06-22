//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package pni

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	flagPNIOutputDir   = flag.String("pni-out", "/tmp/pni", "Output directory")
	flagPNIMaxQueries  = flag.Int("pni-max-queries", 500, "Maximum queries")
	flagPNIK           = flag.Int("pni-k", 10, "K for recall@k")
	flagPNISeed        = flag.Uint64("pni-seed", 42, "Seed for encoder")
	flagPNIDatasetsDir = flag.String("pni-datasets", "/Users/abdel/Documents/datasets", "Datasets directory")
)

// DatasetConfig defines a dataset
type DatasetConfig struct {
	Name      string
	Path      string
	BaseCount int
}

// PolicyConfig defines a search policy
type PolicyConfig struct {
	Name             string
	TargetCandidates int
	MinSegments      int
	IsAdaptive       bool
}

// BenchmarkResult holds results for one dataset/policy combination
type BenchmarkResult struct {
	Dataset          string  `json:"dataset"`
	BaseCount        int     `json:"base_count"`
	QueryCount       int     `json:"query_count"`
	Dims             int     `json:"dims"`
	Policy           string  `json:"policy"`
	TargetCandidates int     `json:"target_candidates"`
	Recall10         float64 `json:"recall_10"`
	P50LatencyUs     float64 `json:"p50_latency_us"`
	P95LatencyUs     float64 `json:"p95_latency_us"`
	P50Candidates    int     `json:"p50_candidates"`
	P95Candidates    int     `json:"p95_candidates"`
	P50Rescores      int     `json:"p50_rescores"`
	P95Rescores      int     `json:"p95_rescores"`
	P50Segments      int     `json:"p50_segments"`
	AvgDistances     float64 `json:"avg_distances"`
	MemoryMB         float64 `json:"memory_mb"`
	BytesPerVector   float64 `json:"bytes_per_vector"`
}

// BenchmarkReport is the full output
type BenchmarkReport struct {
	Config struct {
		MaxQueries int    `json:"max_queries"`
		K          int    `json:"k"`
		Seed       uint64 `json:"seed"`
	} `json:"config"`
	Results   []BenchmarkResult `json:"results"`
	Timestamp string            `json:"timestamp"`
}

// HDF5Data holds loaded dataset
type HDF5Data struct {
	Train     [][]float32
	Test      [][]float32
	Neighbors [][]int
}

func TestPNIBenchmark(t *testing.T) {
	if os.Getenv("PNI_BENCHMARK") == "" {
		t.Skip("Skipping PNI benchmark test. Set PNI_BENCHMARK=1 to run.")
	}

	datasets := []DatasetConfig{
		{
			Name:      "dbpedia-100k",
			Path:      filepath.Join(*flagPNIDatasetsDir, "dbpedia-100k-openai-ada002.hdf5"),
			BaseCount: 100000,
		},
		{
			Name:      "dbpedia-500k",
			Path:      filepath.Join(*flagPNIDatasetsDir, "dbpedia-500k-openai-ada002.hdf5"),
			BaseCount: 500000,
		},
		{
			Name:      "dbpedia-1m",
			Path:      filepath.Join(*flagPNIDatasetsDir, "dbpedia-openai-1000k-angular.hdf5"),
			BaseCount: 990000,
		},
	}

	policies := []PolicyConfig{
		{Name: "A_500", TargetCandidates: 500, MinSegments: 4},
		{Name: "B_1000", TargetCandidates: 1000, MinSegments: 4},
		{Name: "C_2000", TargetCandidates: 2000, MinSegments: 4},
		{Name: "D_adaptive", IsAdaptive: true},
	}

	// Create output directory
	err := os.MkdirAll(*flagPNIOutputDir, 0755)
	require.NoError(t, err)

	var allResults []BenchmarkResult

	for _, ds := range datasets {
		// Check if file exists
		if _, err := os.Stat(ds.Path); os.IsNotExist(err) {
			t.Logf("SKIPPING %s: file not found", ds.Name)
			continue
		}

		t.Log("\n" + strings.Repeat("=", 60))
		t.Logf("Dataset: %s", ds.Name)
		t.Log(strings.Repeat("=", 60))

		// Load dataset
		data, err := loadHDF5(t, ds.Path, *flagPNIMaxQueries)
		if err != nil {
			t.Logf("ERROR loading %s: %v", ds.Name, err)
			continue
		}

		dims := len(data.Train[0])
		t.Logf("Loaded: %d docs, %d queries, %d dims", len(data.Train), len(data.Test), dims)

		// Normalize vectors
		t.Log("Normalizing vectors...")
		normalizeVectors(data.Train)
		normalizeVectors(data.Test)

		// Build index
		t.Log("Building PNI index...")
		buildStart := time.Now()
		idx := NewIndex(dims, *flagPNISeed)
		err = idx.Build(data.Train)
		require.NoError(t, err)
		buildTime := time.Since(buildStart)
		t.Logf("Build time: %v", buildTime)
		t.Logf("Memory: %.2f MB, %.2f bytes/vector", float64(idx.MemoryFootprint())/1e6, idx.BytesPerVector())

		// Run each policy
		for _, policy := range policies {
			t.Logf("\n--- Policy: %s ---", policy.Name)

			result := runBenchmark(t, idx, data, ds, policy, *flagPNIK)
			allResults = append(allResults, result)

			t.Logf("Recall@10: %.4f", result.Recall10)
			t.Logf("P50 latency: %.1f µs, P95: %.1f µs", result.P50LatencyUs, result.P95LatencyUs)
			t.Logf("P50 candidates: %d, P95: %d", result.P50Candidates, result.P95Candidates)
		}
	}

	if len(allResults) == 0 {
		t.Fatal("No results collected")
	}

	// Build report
	report := BenchmarkReport{
		Results:   allResults,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	report.Config.MaxQueries = *flagPNIMaxQueries
	report.Config.K = *flagPNIK
	report.Config.Seed = *flagPNISeed

	// Write outputs
	writeOutputs(t, *flagPNIOutputDir, report)

	// Print summary
	printSummary(t, report)
}

func runBenchmark(t *testing.T, idx *Index, data *HDF5Data, ds DatasetConfig, policy PolicyConfig, k int) BenchmarkResult {
	numQueries := len(data.Test)

	var latencies []float64
	var candidates []int
	var rescores []int
	var segments []int
	var distances []int64
	var hits int
	totalGT := 0

	for qi, query := range data.Test {
		if qi > 0 && qi%100 == 0 {
			t.Logf("  Query %d/%d...", qi, numQueries)
		}

		// Get ground truth
		gtIDs := data.Neighbors[qi]
		if len(gtIDs) > k {
			gtIDs = gtIDs[:k]
		}
		gtSet := make(map[uint64]bool)
		for _, id := range gtIDs {
			gtSet[uint64(id)] = true
		}
		totalGT += len(gtSet)

		// Search
		start := time.Now()
		var result SearchResult
		if policy.IsAdaptive {
			result = idx.SearchAdaptive(query, DefaultAdaptivePolicy(), k)
		} else {
			opts := SearchOptions{
				TargetCandidates:      policy.TargetCandidates,
				MinSegmentsBeforeStop: policy.MinSegments,
				K:                     k,
			}
			result = idx.Search(query, opts)
		}
		elapsed := time.Since(start)

		// Record metrics
		latencies = append(latencies, float64(elapsed.Microseconds()))
		candidates = append(candidates, result.Stats.FinalCandidates)
		rescores = append(rescores, result.Stats.RescoresComputed)
		segments = append(segments, result.Stats.SegmentsProcessed)
		distances = append(distances, result.Stats.DistancesComputed)

		// Count hits
		for _, id := range result.IDs {
			if gtSet[id] {
				hits++
			}
		}
	}

	// Compute statistics
	sort.Float64s(latencies)
	sort.Ints(candidates)
	sort.Ints(rescores)
	sort.Ints(segments)

	avgDistances := 0.0
	for _, d := range distances {
		avgDistances += float64(d)
	}
	avgDistances /= float64(len(distances))

	return BenchmarkResult{
		Dataset:          ds.Name,
		BaseCount:        idx.DocCount(),
		QueryCount:       numQueries,
		Dims:             idx.Dims(),
		Policy:           policy.Name,
		TargetCandidates: policy.TargetCandidates,
		Recall10:         float64(hits) / float64(totalGT),
		P50LatencyUs:     percentile(latencies, 0.50),
		P95LatencyUs:     percentile(latencies, 0.95),
		P50Candidates:    intPercentile(candidates, 0.50),
		P95Candidates:    intPercentile(candidates, 0.95),
		P50Rescores:      intPercentile(rescores, 0.50),
		P95Rescores:      intPercentile(rescores, 0.95),
		P50Segments:      intPercentile(segments, 0.50),
		AvgDistances:     avgDistances,
		MemoryMB:         float64(idx.MemoryFootprint()) / 1e6,
		BytesPerVector:   idx.BytesPerVector(),
	}
}

func loadHDF5(t *testing.T, path string, maxQueries int) (*HDF5Data, error) {
	tmpFile, err := os.CreateTemp("", "hdf5_data_*.json")
	if err != nil {
		return nil, err
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	scriptFile, err := os.CreateTemp("", "load_hdf5_*.py")
	if err != nil {
		return nil, err
	}
	scriptPath := scriptFile.Name()
	defer os.Remove(scriptPath)

	script := fmt.Sprintf(`import h5py
import json
import numpy as np

path = %q
max_queries = %d
out_path = %q

with h5py.File(path, 'r') as f:
    train_key = None
    for k in ['train', 'base']:
        if k in f:
            train_key = k
            break
    if train_key is None:
        raise ValueError("No train/base key found")

    test_key = None
    for k in ['test', 'queries', 'query']:
        if k in f:
            test_key = k
            break
    if test_key is None:
        raise ValueError("No test/query key found")

    train = np.array(f[train_key][:], dtype=np.float32)
    test = np.array(f[test_key][:max_queries], dtype=np.float32)

    neighbors = None
    if 'neighbors' in f:
        neighbors = np.array(f['neighbors'][:max_queries], dtype=np.int32)

    data = {
        'train_shape': list(train.shape),
        'test_shape': list(test.shape),
        'neighbors_shape': list(neighbors.shape) if neighbors is not None else None,
        'train': train.tolist(),
        'test': test.tolist(),
        'neighbors': neighbors.tolist() if neighbors is not None else None,
    }

    with open(out_path, 'w') as out:
        json.dump(data, out)

    print(f"Loaded train: {train.shape}, test: {test.shape}")
`, path, maxQueries, tmpPath)

	if _, err := scriptFile.WriteString(script); err != nil {
		scriptFile.Close()
		return nil, err
	}
	scriptFile.Close()

	cmd := exec.Command("bash", "-c", fmt.Sprintf("source /tmp/hdf5_venv/bin/activate && python3 %s", scriptPath))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("python error: %v\nOutput: %s", err, string(output))
	}
	t.Log(strings.TrimSpace(string(output)))

	jsonData, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Train     [][]float32 `json:"train"`
		Test      [][]float32 `json:"test"`
		Neighbors [][]int     `json:"neighbors"`
	}
	if err := json.Unmarshal(jsonData, &raw); err != nil {
		return nil, err
	}

	return &HDF5Data{
		Train:     raw.Train,
		Test:      raw.Test,
		Neighbors: raw.Neighbors,
	}, nil
}

func normalizeVectors(vecs [][]float32) {
	for i := range vecs {
		var sumSq float64
		for _, v := range vecs[i] {
			sumSq += float64(v) * float64(v)
		}
		norm := float32(math.Sqrt(sumSq))
		if norm > 0 {
			for j := range vecs[i] {
				vecs[i][j] /= norm
			}
		}
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func intPercentile(sorted []int, p float64) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func writeOutputs(t *testing.T, outDir string, report BenchmarkReport) {
	// CSV
	csvPath := filepath.Join(outDir, "pni_benchmark.csv")
	writeCSV(t, csvPath, report.Results)

	// JSON
	jsonPath := filepath.Join(outDir, "pni_results.json")
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Errorf("Failed to marshal JSON: %v", err)
	} else {
		if err := os.WriteFile(jsonPath, jsonData, 0644); err != nil {
			t.Errorf("Failed to write JSON: %v", err)
		} else {
			t.Logf("JSON written to: %s", jsonPath)
		}
	}

	// Markdown
	mdPath := filepath.Join(outDir, "pni_report.md")
	writeMarkdown(t, mdPath, report)
}

func writeCSV(t *testing.T, path string, results []BenchmarkResult) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"dataset", "base_count", "query_count", "dims", "policy", "target_candidates",
		"recall_10", "p50_latency_us", "p95_latency_us",
		"p50_candidates", "p95_candidates", "p50_rescores", "p95_rescores",
		"p50_segments", "avg_distances", "memory_mb", "bytes_per_vector",
	}
	w.Write(header)

	for _, r := range results {
		row := []string{
			r.Dataset, strconv.Itoa(r.BaseCount), strconv.Itoa(r.QueryCount), strconv.Itoa(r.Dims),
			r.Policy, strconv.Itoa(r.TargetCandidates),
			fmt.Sprintf("%.4f", r.Recall10),
			fmt.Sprintf("%.1f", r.P50LatencyUs), fmt.Sprintf("%.1f", r.P95LatencyUs),
			strconv.Itoa(r.P50Candidates), strconv.Itoa(r.P95Candidates),
			strconv.Itoa(r.P50Rescores), strconv.Itoa(r.P95Rescores),
			strconv.Itoa(r.P50Segments), fmt.Sprintf("%.1f", r.AvgDistances),
			fmt.Sprintf("%.2f", r.MemoryMB), fmt.Sprintf("%.2f", r.BytesPerVector),
		}
		w.Write(row)
	}

	t.Logf("CSV written to: %s", path)
}

func writeMarkdown(t *testing.T, path string, report BenchmarkReport) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "# Progressive Neighborhood Index (PNI) Benchmark\n\n")
	fmt.Fprintf(w, "**Date**: %s\n\n", report.Timestamp)

	fmt.Fprintf(w, "## Configuration\n\n")
	fmt.Fprintf(w, "| Parameter | Value |\n")
	fmt.Fprintf(w, "|-----------|-------|\n")
	fmt.Fprintf(w, "| Max Queries | %d |\n", report.Config.MaxQueries)
	fmt.Fprintf(w, "| K | %d |\n", report.Config.K)
	fmt.Fprintf(w, "| RQ1 Seed | %d |\n\n", report.Config.Seed)

	fmt.Fprintf(w, "## Policies\n\n")
	fmt.Fprintf(w, "- **A_500**: TargetCandidates=500, MinSegments=4\n")
	fmt.Fprintf(w, "- **B_1000**: TargetCandidates=1000, MinSegments=4\n")
	fmt.Fprintf(w, "- **C_2000**: TargetCandidates=2000, MinSegments=4\n")
	fmt.Fprintf(w, "- **D_adaptive**: 30%%@64d, 10%%@128d, 2%%@256d, 1000@512d\n\n")

	// Group by dataset
	datasets := []string{"dbpedia-100k", "dbpedia-500k", "dbpedia-1m"}
	for _, ds := range datasets {
		fmt.Fprintf(w, "## %s\n\n", ds)

		fmt.Fprintf(w, "| Policy | Recall@10 | P50 Lat (µs) | P95 Lat (µs) | P50 Cands | P95 Cands | Avg Dists |\n")
		fmt.Fprintf(w, "|--------|-----------|--------------|--------------|-----------|-----------|----------|\n")

		for _, r := range report.Results {
			if r.Dataset == ds {
				fmt.Fprintf(w, "| %s | %.4f | %.1f | %.1f | %d | %d | %.0f |\n",
					r.Policy, r.Recall10, r.P50LatencyUs, r.P95LatencyUs,
					r.P50Candidates, r.P95Candidates, r.AvgDistances)
			}
		}
		fmt.Fprintf(w, "\n")
	}

	// Memory summary
	fmt.Fprintf(w, "## Memory Usage\n\n")
	fmt.Fprintf(w, "| Dataset | Memory (MB) | Bytes/Vector |\n")
	fmt.Fprintf(w, "|---------|-------------|-------------|\n")
	seen := make(map[string]bool)
	for _, r := range report.Results {
		if !seen[r.Dataset] {
			fmt.Fprintf(w, "| %s | %.2f | %.2f |\n", r.Dataset, r.MemoryMB, r.BytesPerVector)
			seen[r.Dataset] = true
		}
	}
	fmt.Fprintf(w, "\n")

	// Analysis
	fmt.Fprintf(w, "## Analysis\n\n")
	fmt.Fprintf(w, "### Recall vs Latency Trade-off\n\n")
	fmt.Fprintf(w, "Higher TargetCandidates → Higher recall, higher latency.\n\n")
	fmt.Fprintf(w, "### Scaling Behavior\n\n")
	fmt.Fprintf(w, "As N increases, does recall remain stable?\n")
	fmt.Fprintf(w, "As N increases, does latency scale sublinearly?\n\n")

	t.Logf("Markdown written to: %s", path)
}

func printSummary(t *testing.T, report BenchmarkReport) {
	t.Log("\n" + strings.Repeat("=", 80))
	t.Log("PNI BENCHMARK SUMMARY")
	t.Log(strings.Repeat("=", 80))

	t.Log("\nDataset        | Policy      | Recall@10 | P50 Lat | P50 Cands")
	t.Log("---------------|-------------|-----------|---------|----------")

	for _, r := range report.Results {
		t.Logf("%-14s | %-11s | %.4f    | %6.0f µs | %5d",
			r.Dataset, r.Policy, r.Recall10, r.P50LatencyUs, r.P50Candidates)
	}

	t.Log("\n" + strings.Repeat("=", 80))
}
