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
	flagPolicyEOutputDir   = flag.String("policy-e-out", "/tmp/pni_policy_e", "Output directory")
	flagPolicyEMaxQueries  = flag.Int("policy-e-max-queries", 500, "Maximum queries")
	flagPolicyEK           = flag.Int("policy-e-k", 10, "K for recall@k")
	flagPolicyESeed        = flag.Uint64("policy-e-seed", 42, "Seed for encoder")
	flagPolicyEDatasetsDir = flag.String("policy-e-datasets", "/Users/abdel/Documents/datasets", "Datasets directory")
)

// PolicyEResult holds benchmark results for one policy/dataset combination.
type PolicyEResult struct {
	Dataset   string `json:"dataset"`
	BaseCount int    `json:"base_count"`
	Dims      int    `json:"dims"`
	Policy    string `json:"policy"`

	Recall10 float64 `json:"recall_10"`

	P50LatencyUs float64 `json:"p50_latency_us"`
	P95LatencyUs float64 `json:"p95_latency_us"`

	AvgFinalCandidates int `json:"avg_final_candidates"`
	P50FinalCandidates int `json:"p50_final_candidates"`
	P95FinalCandidates int `json:"p95_final_candidates"`

	AvgSegmentsUsed float64 `json:"avg_segments_used"`
	P50SegmentsUsed int     `json:"p50_segments_used"`
	P95SegmentsUsed int     `json:"p95_segments_used"`

	AvgDistances float64 `json:"avg_distances"`
	P50Distances int64   `json:"p50_distances"`
	P95Distances int64   `json:"p95_distances"`

	AvgRescored int `json:"avg_rescored"`
	P50Rescored int `json:"p50_rescored"`
	P95Rescored int `json:"p95_rescored"`
}

// PolicyEReport is the full benchmark report.
type PolicyEReport struct {
	Config struct {
		MaxQueries int    `json:"max_queries"`
		K          int    `json:"k"`
		Seed       uint64 `json:"seed"`
	} `json:"config"`
	Results   []PolicyEResult `json:"results"`
	Timestamp string          `json:"timestamp"`
}

func TestPolicyEBenchmark(t *testing.T) {
	if os.Getenv("PNI_BENCHMARK") == "" {
		t.Skip("Skipping PNI benchmark test. Set PNI_BENCHMARK=1 to run.")
	}

	datasets := []struct {
		Name string
		Path string
	}{
		{"dbpedia-100k", filepath.Join(*flagPolicyEDatasetsDir, "dbpedia-100k-openai-ada002.hdf5")},
		{"dbpedia-500k", filepath.Join(*flagPolicyEDatasetsDir, "dbpedia-500k-openai-ada002.hdf5")},
		{"dbpedia-1m", filepath.Join(*flagPolicyEDatasetsDir, "dbpedia-openai-1000k-angular.hdf5")},
	}

	k := *flagPolicyEK

	// Define all Policy E variants
	policies := []MarginPolicy{
		// Constant margin policies
		PolicyE1(k),
		PolicyE2(k),
		PolicyE3(k),
		PolicyE4(k),
		// Adaptive margin policies
		PolicyE5(k),
		PolicyE6(k),
		PolicyE7(k),
		PolicyE8(k),
		// Full RQ1 versions (no early stopping)
		PolicyE_FullRQ1(PolicyE3(k)),  // margin=16, full RQ1
		PolicyE_FullRQ1(PolicyE4(k)),  // margin=24, full RQ1
		PolicyE_FullRQ1(PolicyE8(k)),  // dims schedule, full RQ1
	}

	// Also include D_adaptive for comparison
	includeDAdaptive := true

	// Create output directory
	err := os.MkdirAll(*flagPolicyEOutputDir, 0755)
	require.NoError(t, err)

	var allResults []PolicyEResult

	for _, ds := range datasets {
		if _, err := os.Stat(ds.Path); os.IsNotExist(err) {
			t.Logf("SKIPPING %s: file not found", ds.Name)
			continue
		}

		t.Log("\n" + strings.Repeat("=", 70))
		t.Logf("Dataset: %s", ds.Name)
		t.Log(strings.Repeat("=", 70))

		// Load dataset
		data, err := loadPolicyEHDF5(t, ds.Path, *flagPolicyEMaxQueries)
		if err != nil {
			t.Logf("ERROR loading %s: %v", ds.Name, err)
			continue
		}

		dims := len(data.Train[0])
		t.Logf("Loaded: %d docs, %d queries, %d dims", len(data.Train), len(data.Test), dims)

		// Normalize vectors
		t.Log("Normalizing vectors...")
		policyENormalizeVectors(data.Train)
		policyENormalizeVectors(data.Test)

		// Build index
		t.Log("Building PNI index...")
		buildStart := time.Now()
		idx := NewIndex(dims, *flagPolicyESeed)
		err = idx.Build(data.Train)
		require.NoError(t, err)
		t.Logf("Build time: %v", time.Since(buildStart))

		// Run D_adaptive for comparison
		if includeDAdaptive {
			t.Log("\n--- D_adaptive (baseline) ---")
			result := runDAdaptiveBenchmark(t, idx, data, ds.Name, k)
			allResults = append(allResults, result)
			t.Logf("Recall@10: %.4f, P50 lat: %.1f µs, P50 cands: %d",
				result.Recall10, result.P50LatencyUs, result.P50FinalCandidates)
		}

		// Run all Policy E variants
		for _, policy := range policies {
			t.Logf("\n--- %s ---", policy.Name)
			result := runPolicyEBenchmark(t, idx, data, ds.Name, policy)
			allResults = append(allResults, result)
			t.Logf("Recall@10: %.4f, P50 lat: %.1f µs, P50 cands: %d, P50 segs: %d",
				result.Recall10, result.P50LatencyUs, result.P50FinalCandidates, result.P50SegmentsUsed)
		}
	}

	if len(allResults) == 0 {
		t.Fatal("No results collected")
	}

	// Build report
	report := PolicyEReport{
		Results:   allResults,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	report.Config.MaxQueries = *flagPolicyEMaxQueries
	report.Config.K = *flagPolicyEK
	report.Config.Seed = *flagPolicyESeed

	// Write outputs
	writePolicyEOutputs(t, *flagPolicyEOutputDir, report)

	// Print summary and analysis
	printPolicyEAnalysis(t, report)
}

func runDAdaptiveBenchmark(t *testing.T, idx *Index, data *HDF5Data, dsName string, k int) PolicyEResult {
	numQueries := len(data.Test)
	policy := DefaultAdaptivePolicy()

	var latencies []float64
	var finalCands []int
	var segments []int
	var distances []int64
	var rescored []int
	var hits int
	totalGT := 0

	for qi, query := range data.Test {
		if qi > 0 && qi%100 == 0 {
			t.Logf("  Query %d/%d...", qi, numQueries)
		}

		gtIDs := data.Neighbors[qi]
		if len(gtIDs) > k {
			gtIDs = gtIDs[:k]
		}
		gtSet := make(map[uint64]bool)
		for _, id := range gtIDs {
			gtSet[uint64(id)] = true
		}
		totalGT += len(gtSet)

		start := time.Now()
		result := idx.SearchAdaptive(query, policy, k)
		elapsed := time.Since(start)

		latencies = append(latencies, float64(elapsed.Microseconds()))
		finalCands = append(finalCands, result.Stats.FinalCandidates)
		segments = append(segments, result.Stats.SegmentsProcessed)
		distances = append(distances, result.Stats.DistancesComputed)
		rescored = append(rescored, result.Stats.RescoresComputed)

		for _, id := range result.IDs {
			if gtSet[id] {
				hits++
			}
		}
	}

	sort.Float64s(latencies)
	sort.Ints(finalCands)
	sort.Ints(segments)
	sort.Ints(rescored)

	sortedDist := make([]int64, len(distances))
	copy(sortedDist, distances)
	sort.Slice(sortedDist, func(i, j int) bool { return sortedDist[i] < sortedDist[j] })

	return PolicyEResult{
		Dataset:            dsName,
		BaseCount:          idx.DocCount(),
		Dims:               idx.Dims(),
		Policy:             "D_adaptive",
		Recall10:           float64(hits) / float64(totalGT),
		P50LatencyUs:       policyEPercentileF(latencies, 0.50),
		P95LatencyUs:       policyEPercentileF(latencies, 0.95),
		AvgFinalCandidates: policyEAvgInt(finalCands),
		P50FinalCandidates: policyEPercentileI(finalCands, 0.50),
		P95FinalCandidates: policyEPercentileI(finalCands, 0.95),
		AvgSegmentsUsed:    policyEAvgFloat(segments),
		P50SegmentsUsed:    policyEPercentileI(segments, 0.50),
		P95SegmentsUsed:    policyEPercentileI(segments, 0.95),
		AvgDistances:       policyEAvgInt64(distances),
		P50Distances:       policyEPercentileI64(sortedDist, 0.50),
		P95Distances:       policyEPercentileI64(sortedDist, 0.95),
		AvgRescored:        policyEAvgInt(rescored),
		P50Rescored:        policyEPercentileI(rescored, 0.50),
		P95Rescored:        policyEPercentileI(rescored, 0.95),
	}
}

func runPolicyEBenchmark(t *testing.T, idx *Index, data *HDF5Data, dsName string, policy MarginPolicy) PolicyEResult {
	numQueries := len(data.Test)
	k := policy.K

	var latencies []float64
	var finalCands []int
	var segments []int
	var distances []int64
	var rescored []int
	var hits int
	totalGT := 0

	for qi, query := range data.Test {
		if qi > 0 && qi%100 == 0 {
			t.Logf("  Query %d/%d...", qi, numQueries)
		}

		gtIDs := data.Neighbors[qi]
		if len(gtIDs) > k {
			gtIDs = gtIDs[:k]
		}
		gtSet := make(map[uint64]bool)
		for _, id := range gtIDs {
			gtSet[uint64(id)] = true
		}
		totalGT += len(gtSet)

		start := time.Now()
		result := idx.SearchMargin(query, policy)
		elapsed := time.Since(start)

		latencies = append(latencies, float64(elapsed.Microseconds()))
		finalCands = append(finalCands, result.MarginStats.FinalCandidates)
		segments = append(segments, result.MarginStats.SegmentsProcessed)
		distances = append(distances, result.MarginStats.DistancesComputed)
		rescored = append(rescored, result.MarginStats.RescoresComputed)

		for _, id := range result.IDs {
			if gtSet[id] {
				hits++
			}
		}
	}

	sort.Float64s(latencies)
	sort.Ints(finalCands)
	sort.Ints(segments)
	sort.Ints(rescored)

	sortedDist := make([]int64, len(distances))
	copy(sortedDist, distances)
	sort.Slice(sortedDist, func(i, j int) bool { return sortedDist[i] < sortedDist[j] })

	return PolicyEResult{
		Dataset:            dsName,
		BaseCount:          idx.DocCount(),
		Dims:               idx.Dims(),
		Policy:             policy.Name,
		Recall10:           float64(hits) / float64(totalGT),
		P50LatencyUs:       policyEPercentileF(latencies, 0.50),
		P95LatencyUs:       policyEPercentileF(latencies, 0.95),
		AvgFinalCandidates: policyEAvgInt(finalCands),
		P50FinalCandidates: policyEPercentileI(finalCands, 0.50),
		P95FinalCandidates: policyEPercentileI(finalCands, 0.95),
		AvgSegmentsUsed:    policyEAvgFloat(segments),
		P50SegmentsUsed:    policyEPercentileI(segments, 0.50),
		P95SegmentsUsed:    policyEPercentileI(segments, 0.95),
		AvgDistances:       policyEAvgInt64(distances),
		P50Distances:       policyEPercentileI64(sortedDist, 0.50),
		P95Distances:       policyEPercentileI64(sortedDist, 0.95),
		AvgRescored:        policyEAvgInt(rescored),
		P50Rescored:        policyEPercentileI(rescored, 0.50),
		P95Rescored:        policyEPercentileI(rescored, 0.95),
	}
}

func loadPolicyEHDF5(t *testing.T, path string, maxQueries int) (*HDF5Data, error) {
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

func policyENormalizeVectors(vecs [][]float32) {
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

func policyEPercentileF(sorted []float64, p float64) float64 {
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

func policyEPercentileI(sorted []int, p float64) int {
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

func policyEPercentileI64(sorted []int64, p float64) int64 {
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

func policyEAvgInt(vals []int) int {
	if len(vals) == 0 {
		return 0
	}
	sum := 0
	for _, v := range vals {
		sum += v
	}
	return sum / len(vals)
}

func policyEAvgFloat(vals []int) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0
	for _, v := range vals {
		sum += v
	}
	return float64(sum) / float64(len(vals))
}

func policyEAvgInt64(vals []int64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum int64
	for _, v := range vals {
		sum += v
	}
	return float64(sum) / float64(len(vals))
}

func writePolicyEOutputs(t *testing.T, outDir string, report PolicyEReport) {
	// CSV
	csvPath := filepath.Join(outDir, "pni_policy_e_benchmark.csv")
	writePolicyECSV(t, csvPath, report.Results)

	// JSON
	jsonPath := filepath.Join(outDir, "pni_policy_e_results.json")
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
	mdPath := filepath.Join(outDir, "pni_policy_e_report.md")
	writePolicyEMarkdown(t, mdPath, report)
}

func writePolicyECSV(t *testing.T, path string, results []PolicyEResult) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"dataset", "base_count", "dims", "policy",
		"recall_10", "p50_latency_us", "p95_latency_us",
		"avg_final_cands", "p50_final_cands", "p95_final_cands",
		"avg_segments", "p50_segments", "p95_segments",
		"avg_distances", "p50_distances", "p95_distances",
		"avg_rescored", "p50_rescored", "p95_rescored",
	}
	w.Write(header)

	for _, r := range results {
		row := []string{
			r.Dataset, strconv.Itoa(r.BaseCount), strconv.Itoa(r.Dims), r.Policy,
			fmt.Sprintf("%.4f", r.Recall10),
			fmt.Sprintf("%.1f", r.P50LatencyUs), fmt.Sprintf("%.1f", r.P95LatencyUs),
			strconv.Itoa(r.AvgFinalCandidates), strconv.Itoa(r.P50FinalCandidates), strconv.Itoa(r.P95FinalCandidates),
			fmt.Sprintf("%.1f", r.AvgSegmentsUsed), strconv.Itoa(r.P50SegmentsUsed), strconv.Itoa(r.P95SegmentsUsed),
			fmt.Sprintf("%.0f", r.AvgDistances), strconv.FormatInt(r.P50Distances, 10), strconv.FormatInt(r.P95Distances, 10),
			strconv.Itoa(r.AvgRescored), strconv.Itoa(r.P50Rescored), strconv.Itoa(r.P95Rescored),
		}
		w.Write(row)
	}

	t.Logf("CSV written to: %s", path)
}

func writePolicyEMarkdown(t *testing.T, path string, report PolicyEReport) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "# Policy E Benchmark: Best-Distance + Margin Pruning\n\n")
	fmt.Fprintf(w, "**Date**: %s\n\n", report.Timestamp)

	fmt.Fprintf(w, "## Configuration\n\n")
	fmt.Fprintf(w, "| Parameter | Value |\n")
	fmt.Fprintf(w, "|-----------|-------|\n")
	fmt.Fprintf(w, "| Max Queries | %d |\n", report.Config.MaxQueries)
	fmt.Fprintf(w, "| K | %d |\n", report.Config.K)
	fmt.Fprintf(w, "| RQ1 Seed | %d |\n\n", report.Config.Seed)

	fmt.Fprintf(w, "## Policy E Description\n\n")
	fmt.Fprintf(w, "Policy E keeps candidates whose accumulated distance is within a **margin** of the best:\n\n")
	fmt.Fprintf(w, "```\nkeep if: accDist <= best + margin\n```\n\n")
	fmt.Fprintf(w, "### Policy Variants\n\n")
	fmt.Fprintf(w, "| Policy | Margin Schedule | Early Stop |\n")
	fmt.Fprintf(w, "|--------|-----------------|------------|\n")
	fmt.Fprintf(w, "| E1 | constant 8 | Yes |\n")
	fmt.Fprintf(w, "| E2 | constant 12 | Yes |\n")
	fmt.Fprintf(w, "| E3 | constant 16 | Yes |\n")
	fmt.Fprintf(w, "| E4 | constant 24 | Yes |\n")
	fmt.Fprintf(w, "| E5 | max(8, seg*2) | Yes |\n")
	fmt.Fprintf(w, "| E6 | max(12, seg*2) | Yes |\n")
	fmt.Fprintf(w, "| E7 | max(16, seg*3) | Yes |\n")
	fmt.Fprintf(w, "| E8 | dims-based (8→48) | Yes |\n")
	fmt.Fprintf(w, "| *_fullRQ1 | same as base | No |\n\n")

	// Results by dataset
	datasets := []string{"dbpedia-100k", "dbpedia-500k", "dbpedia-1m"}
	for _, ds := range datasets {
		fmt.Fprintf(w, "## %s\n\n", ds)
		fmt.Fprintf(w, "| Policy | Recall@10 | P50 Lat (µs) | P95 Lat (µs) | P50 Cands | P95 Cands | P50 Segs |\n")
		fmt.Fprintf(w, "|--------|-----------|--------------|--------------|-----------|-----------|----------|\n")

		for _, r := range report.Results {
			if r.Dataset == ds {
				fmt.Fprintf(w, "| %s | **%.4f** | %.0f | %.0f | %d | %d | %d |\n",
					r.Policy, r.Recall10, r.P50LatencyUs, r.P95LatencyUs,
					r.P50FinalCandidates, r.P95FinalCandidates, r.P50SegmentsUsed)
			}
		}
		fmt.Fprintf(w, "\n")
	}

	// Analysis section
	fmt.Fprintf(w, "## Analysis\n\n")

	fmt.Fprintf(w, "### 1. Best Margin Schedule\n\n")
	fmt.Fprintf(w, "Compare recall@10 across policies to identify the best margin schedule.\n\n")

	fmt.Fprintf(w, "### 2. Policy E vs Oracle RQ1 Keep\n\n")
	fmt.Fprintf(w, "Does Policy E close the gap between oracle RQ1 requiredKeep and actual PNI recall?\n\n")
	fmt.Fprintf(w, "- Oracle RQ1_full requiredKeep: ~50-80 candidates (from neighborhood density experiment)\n")
	fmt.Fprintf(w, "- If Policy E reaches similar candidate counts with high recall, it validates the approach.\n\n")

	fmt.Fprintf(w, "### 3. Early Stopping Impact\n\n")
	fmt.Fprintf(w, "Compare `*_fullRQ1` variants against their base versions:\n")
	fmt.Fprintf(w, "- If fullRQ1 has significantly better recall, early stopping is hurting.\n")
	fmt.Fprintf(w, "- If similar, early stopping is fine.\n\n")

	fmt.Fprintf(w, "### 4. Full RQ1 + Policy E Recall\n\n")
	fmt.Fprintf(w, "Does processing all segments with margin pruning achieve better recall?\n\n")

	fmt.Fprintf(w, "### 5. Candidate Count Consistency\n\n")
	fmt.Fprintf(w, "Do P50 candidate counts remain stable across 100K, 500K, and 1M?\n")
	fmt.Fprintf(w, "This validates the constant-neighborhood hypothesis.\n\n")

	t.Logf("Markdown written to: %s", path)
}

func printPolicyEAnalysis(t *testing.T, report PolicyEReport) {
	t.Log("\n" + strings.Repeat("=", 90))
	t.Log("POLICY E BENCHMARK ANALYSIS")
	t.Log(strings.Repeat("=", 90))

	// Summary table
	t.Log("\n--- SUMMARY TABLE ---")
	t.Log("Dataset        | Policy              | Recall@10 | P50 Lat | P50 Cands | P50 Segs")
	t.Log("---------------|---------------------|-----------|---------|-----------|----------")

	for _, r := range report.Results {
		t.Logf("%-14s | %-19s | %.4f    | %7.0f | %9d | %8d",
			r.Dataset, r.Policy, r.Recall10, r.P50LatencyUs, r.P50FinalCandidates, r.P50SegmentsUsed)
	}

	// Find best policies per dataset
	t.Log("\n--- BEST POLICIES BY DATASET ---")
	datasets := []string{"dbpedia-100k", "dbpedia-500k", "dbpedia-1m"}
	for _, ds := range datasets {
		var bestRecall float64
		var bestPolicy string
		for _, r := range report.Results {
			if r.Dataset == ds && r.Recall10 > bestRecall {
				bestRecall = r.Recall10
				bestPolicy = r.Policy
			}
		}
		t.Logf("%s: Best = %s (Recall@10 = %.4f)", ds, bestPolicy, bestRecall)
	}

	// Compare full RQ1 vs early stopping
	t.Log("\n--- FULL RQ1 vs EARLY STOPPING ---")
	comparisons := []struct {
		base    string
		fullRQ1 string
	}{
		{"E3_margin16", "E3_margin16_fullRQ1"},
		{"E4_margin24", "E4_margin24_fullRQ1"},
		{"E8_dims_schedule", "E8_dims_schedule_fullRQ1"},
	}

	for _, cmp := range comparisons {
		t.Logf("\n%s vs %s:", cmp.base, cmp.fullRQ1)
		for _, ds := range datasets {
			var baseRecall, fullRecall float64
			for _, r := range report.Results {
				if r.Dataset == ds {
					if r.Policy == cmp.base {
						baseRecall = r.Recall10
					}
					if r.Policy == cmp.fullRQ1 {
						fullRecall = r.Recall10
					}
				}
			}
			delta := fullRecall - baseRecall
			t.Logf("  %s: base=%.4f, fullRQ1=%.4f, delta=%+.4f", ds, baseRecall, fullRecall, delta)
		}
	}

	// Candidate count consistency
	t.Log("\n--- CANDIDATE COUNT CONSISTENCY ---")
	t.Log("Do P50 candidates remain stable as N grows?")
	for _, policy := range []string{"D_adaptive", "E4_margin24", "E8_dims_schedule", "E8_dims_schedule_fullRQ1"} {
		t.Logf("\n%s:", policy)
		for _, r := range report.Results {
			if r.Policy == policy {
				t.Logf("  %s (N=%d): P50=%d, P95=%d", r.Dataset, r.BaseCount, r.P50FinalCandidates, r.P95FinalCandidates)
			}
		}
	}

	// Decision criterion
	t.Log("\n--- DECISION CRITERION ---")
	t.Log("Policy E is better if:")
	t.Log("  - Recall@10 >= 0.95 (or clearly better than D_adaptive)")
	t.Log("  - P95 latency in 10-100ms range")
	t.Log("  - Final candidates remain reasonable")

	var bestOverall string
	var bestOverallRecall float64
	for _, r := range report.Results {
		if r.Dataset == "dbpedia-1m" && r.Recall10 > bestOverallRecall && r.P95LatencyUs < 100000 {
			bestOverallRecall = r.Recall10
			bestOverall = r.Policy
		}
	}
	t.Logf("\nRecommended policy for 1M scale: %s (Recall@10 = %.4f)", bestOverall, bestOverallRecall)

	t.Log("\n" + strings.Repeat("=", 90))
}
