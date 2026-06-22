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
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// Recall Ceiling Experiment
// Goal: Show whether PNI can approach recall@10 ≈ 1.0 by spending more memory/latency.

var (
	ceilingOutDir     = flag.String("ceiling-out", "/tmp/pni_recall_ceiling", "Output directory")
	ceilingMaxQueries = flag.Int("ceiling-max-queries", 500, "Max queries to run")
	ceilingK          = flag.Int("ceiling-k", 10, "K for recall@K")
	ceilingDatasets   = flag.String("ceiling-datasets", "/Users/abdel/Documents/datasets", "Datasets directory")
)

// CeilingPolicy defines a recall ceiling test policy.
type CeilingPolicy struct {
	Name string

	// RQ1DimsForPruning is how many RQ1 dimensions to use for progressive pruning.
	RQ1DimsForPruning int

	// KeepCurve maps cumulative dimensions to keep fraction or absolute count.
	KeepCurve map[int]float64

	// FullRQ1Rerank: if true, after progressive pruning, compute full RQ1 distance
	// on remaining candidates and keep best FullRQ1KeepCount.
	FullRQ1Rerank     bool
	FullRQ1KeepCount  int
	PreFullRQ1Reduce  int // reduce to this many before full RQ1

	// RescoreLimit caps float rescores.
	RescoreLimit int
}

// CeilingResult holds results for one policy on one dataset.
type CeilingResult struct {
	Dataset string
	Policy  string

	Recall10 float64

	P50Latency float64
	P95Latency float64

	P50FinalCandidates int
	P95FinalCandidates int

	P50SegmentsProcessed int
	P95SegmentsProcessed int

	AvgDistancesComputed int64
	AvgRescoresComputed  int

	// Memory estimates
	RQ1PrefixBytes  int // bytes per vector for RQ1 prefix only
	TotalBytesPerVec int // including float vectors for rescore
}

func TestRecallCeiling(t *testing.T) {
	if os.Getenv("PNI_BENCHMARK") == "" {
		t.Skip("Skipping PNI benchmark test. Set PNI_BENCHMARK=1 to run.")
	}

	flag.Parse()

	datasets := []string{
		"dbpedia-100k-openai-ada002.hdf5",
		"dbpedia-500k-openai-ada002.hdf5",
		"dbpedia-openai-1000k-angular.hdf5",
	}

	policies := []CeilingPolicy{
		// Ceiling_A: 512d prefix, moderate pruning
		{
			Name:              "Ceiling_A",
			RQ1DimsForPruning: 512,
			KeepCurve: map[int]float64{
				64:  0.50, // 50%
				128: 0.25, // 25%
				256: 0.10, // 10%
				512: 5000, // 5000 absolute
			},
			RescoreLimit: 5000,
		},
		// Ceiling_B: 768d prefix
		{
			Name:              "Ceiling_B",
			RQ1DimsForPruning: 768,
			KeepCurve: map[int]float64{
				64:  0.60,
				128: 0.35,
				256: 0.15,
				512: 5000,
				768: 3000,
			},
			RescoreLimit: 3000,
		},
		// Ceiling_C: 1024d prefix
		{
			Name:              "Ceiling_C",
			RQ1DimsForPruning: 1024,
			KeepCurve: map[int]float64{
				64:   0.70,
				128:  0.50,
				256:  0.25,
				512:  10000,
				768:  5000,
				1024: 3000,
			},
			RescoreLimit: 3000,
		},
		// Ceiling_D: 1536d prefix (all dims)
		{
			Name:              "Ceiling_D",
			RQ1DimsForPruning: 1536,
			KeepCurve: map[int]float64{
				64:   0.80,
				128:  0.60,
				256:  0.35,
				512:  20000,
				768:  10000,
				1024: 5000,
				1536: 3000,
			},
			RescoreLimit: 3000,
		},
		// Ceiling_E: Progressive to 512d, then full RQ1 rerank
		{
			Name:              "Ceiling_E",
			RQ1DimsForPruning: 512,
			KeepCurve: map[int]float64{
				64:  0.50,
				128: 0.25,
				256: 0.10,
				512: 5000,
			},
			FullRQ1Rerank:    true,
			PreFullRQ1Reduce: 5000,
			FullRQ1KeepCount: 3000,
			RescoreLimit:     3000,
		},
		// Ceiling_F: Progressive to 512d, then full RQ1 rerank (more candidates)
		{
			Name:              "Ceiling_F",
			RQ1DimsForPruning: 512,
			KeepCurve: map[int]float64{
				64:  0.60,
				128: 0.35,
				256: 0.15,
				512: 10000,
			},
			FullRQ1Rerank:    true,
			PreFullRQ1Reduce: 10000,
			FullRQ1KeepCount: 5000,
			RescoreLimit:     5000,
		},
	}

	// Create output directory
	if err := os.MkdirAll(*ceilingOutDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	var allResults []CeilingResult

	for _, dsName := range datasets {
		dsPath := filepath.Join(*ceilingDatasets, dsName)
		if _, err := os.Stat(dsPath); os.IsNotExist(err) {
			t.Logf("Skipping %s (not found)", dsName)
			continue
		}

		t.Logf("\n======================================================================")
		t.Logf("Dataset: %s", dsName)
		t.Logf("======================================================================")

		train, test, err := loadHDF5Dataset(dsPath, *ceilingMaxQueries)
		if err != nil {
			t.Fatalf("Failed to load dataset: %v", err)
		}

		datasetName := dsName[:len(dsName)-5] // remove .hdf5

		t.Logf("Loaded: %d docs, %d queries, %d dims", len(train), len(test), len(train[0]))

		// Normalize vectors
		t.Logf("Normalizing vectors...")
		normalizeVectorsCeiling(train)
		normalizeVectorsCeiling(test)

		// Build index
		t.Logf("Building PNI index...")
		dims := len(train[0])
		idx := NewIndex(dims, 42)
		buildStart := time.Now()
		if err := idx.Build(train); err != nil {
			t.Fatalf("Build failed: %v", err)
		}
		t.Logf("Build time: %v", time.Since(buildStart))

		// Compute ground truth
		t.Logf("Computing ground truth...")
		groundTruth := computeGroundTruthCeiling(train, test, *ceilingK)

		// Run each policy
		for _, policy := range policies {
			t.Logf("\n--- %s ---", policy.Name)

			results := runCeilingPolicy(idx, test, groundTruth, policy, *ceilingK)

			result := aggregateCeilingResults(datasetName, policy, results, dims)
			allResults = append(allResults, result)

			t.Logf("Recall@%d: %.4f", *ceilingK, result.Recall10)
			t.Logf("P50 latency: %.1f µs, P95: %.1f µs", result.P50Latency, result.P95Latency)
			t.Logf("P50 candidates: %d, P95: %d", result.P50FinalCandidates, result.P95FinalCandidates)
			t.Logf("RQ1 prefix memory: %d B/vec", result.RQ1PrefixBytes)
		}
	}

	// Write outputs
	writeCeilingCSV(t, allResults)
	writeCeilingJSON(t, allResults)
	writeCeilingMarkdown(t, allResults)

	// Print analysis
	printCeilingAnalysis(t, allResults)
}

// CeilingQueryResult holds per-query stats.
type CeilingQueryResult struct {
	Recall           float64
	Latency          time.Duration
	FinalCandidates  int
	SegmentsProcessed int
	DistancesComputed int64
	RescoresComputed  int
}

func runCeilingPolicy(idx *Index, queries [][]float32, groundTruth [][]uint64, policy CeilingPolicy, k int) []CeilingQueryResult {
	results := make([]CeilingQueryResult, len(queries))

	for i, query := range queries {
		if (i+1)%100 == 0 {
			fmt.Printf("  Query %d/%d...\n", i+1, len(queries))
		}

		start := time.Now()
		searchResult := searchCeiling(idx, query, policy, k)
		elapsed := time.Since(start)

		// Compute recall
		hits := 0
		gtSet := make(map[uint64]bool)
		for _, id := range groundTruth[i] {
			gtSet[id] = true
		}
		for _, id := range searchResult.IDs {
			if gtSet[id] {
				hits++
			}
		}

		results[i] = CeilingQueryResult{
			Recall:            float64(hits) / float64(k),
			Latency:           elapsed,
			FinalCandidates:   searchResult.Stats.FinalCandidates,
			SegmentsProcessed: searchResult.Stats.SegmentsProcessed,
			DistancesComputed: searchResult.Stats.DistancesComputed,
			RescoresComputed:  searchResult.Stats.RescoresComputed,
		}
	}

	return results
}

func searchCeiling(idx *Index, query []float32, policy CeilingPolicy, k int) SearchResult {
	if idx.docCount == 0 {
		return SearchResult{}
	}

	// Encode query
	queryCodes := idx.encoder.Encode(query)

	// Initialize all docs as alive
	alive := make([]int, idx.docCount)
	for i := 0; i < idx.docCount; i++ {
		alive[i] = i
	}
	aliveCount := idx.docCount

	// Accumulated Hamming distances
	accDist := make([]int, idx.docCount)

	stats := SearchStats{
		CandidatesPerSegment: make([]int, 0, idx.segmentCount),
	}

	// Determine how many segments to use for progressive pruning
	maxPruningSeg := (policy.RQ1DimsForPruning + 63) / 64
	if maxPruningSeg > idx.segmentCount {
		maxPruningSeg = idx.segmentCount
	}

	// Progressive elimination up to RQ1DimsForPruning
	for seg := 0; seg < maxPruningSeg; seg++ {
		queryCode := queryCodes[seg]
		segmentData := idx.segments[seg]

		// Compute distances for alive candidates
		for i := 0; i < aliveCount; i++ {
			docID := alive[i]
			accDist[docID] += bits.OnesCount64(queryCode ^ segmentData[docID])
		}
		stats.DistancesComputed += int64(aliveCount)

		stats.CandidatesPerSegment = append(stats.CandidatesPerSegment, aliveCount)

		// Compute adaptive target based on dims processed
		dimsProcessed := (seg + 1) * 64
		if dimsProcessed > policy.RQ1DimsForPruning {
			dimsProcessed = policy.RQ1DimsForPruning
		}

		target := computeCeilingTarget(policy.KeepCurve, dimsProcessed, idx.docCount)

		// Prune if needed
		if target > 0 && aliveCount > target {
			aliveCount = pruneToK(alive[:aliveCount], accDist, target)
		}

		stats.SegmentsProcessed = seg + 1
	}

	// Full RQ1 rerank if requested
	if policy.FullRQ1Rerank && aliveCount > 0 {
		// Compute full RQ1 distance on remaining candidates
		fullRQ1Dist := make([]int, idx.docCount)
		for i := 0; i < aliveCount; i++ {
			docID := alive[i]
			dist := accDist[docID] // already have partial distance
			// Add remaining segments
			for seg := maxPruningSeg; seg < idx.segmentCount; seg++ {
				dist += bits.OnesCount64(queryCodes[seg] ^ idx.segments[seg][docID])
			}
			fullRQ1Dist[docID] = dist
			stats.DistancesComputed += int64(idx.segmentCount - maxPruningSeg)
		}

		// Prune to FullRQ1KeepCount using full distances
		if policy.FullRQ1KeepCount > 0 && aliveCount > policy.FullRQ1KeepCount {
			aliveCount = pruneToK(alive[:aliveCount], fullRQ1Dist, policy.FullRQ1KeepCount)
		}

		stats.SegmentsProcessed = idx.segmentCount
	}

	// Apply rescore limit
	if policy.RescoreLimit > 0 && aliveCount > policy.RescoreLimit {
		aliveCount = pruneToK(alive[:aliveCount], accDist, policy.RescoreLimit)
	}

	stats.FinalCandidates = aliveCount

	// Float rescore
	candidates := alive[:aliveCount]
	result := idx.rescore(query, candidates, k, &stats)
	result.Stats = stats

	return result
}

func computeCeilingTarget(keepCurve map[int]float64, dimsProcessed, totalDocs int) int {
	// Find the most recent applicable checkpoint
	var target float64 = -1
	var bestDims int = -1

	for dims, val := range keepCurve {
		if dimsProcessed >= dims && dims > bestDims {
			target = val
			bestDims = dims
		}
	}

	if target < 0 {
		return totalDocs // No pruning yet
	}

	if target <= 1.0 {
		return int(target * float64(totalDocs))
	}
	return int(target)
}

func aggregateCeilingResults(dataset string, policy CeilingPolicy, results []CeilingQueryResult, dims int) CeilingResult {
	n := len(results)
	if n == 0 {
		return CeilingResult{Dataset: dataset, Policy: policy.Name}
	}

	// Collect metrics
	recalls := make([]float64, n)
	latencies := make([]float64, n)
	candidates := make([]int, n)
	segments := make([]int, n)
	var totalDist int64
	var totalRescores int

	for i, r := range results {
		recalls[i] = r.Recall
		latencies[i] = float64(r.Latency.Microseconds())
		candidates[i] = r.FinalCandidates
		segments[i] = r.SegmentsProcessed
		totalDist += r.DistancesComputed
		totalRescores += r.RescoresComputed
	}

	// Sort for percentiles
	sort.Float64s(recalls)
	sort.Float64s(latencies)
	sort.Ints(candidates)
	sort.Ints(segments)

	// Memory calculation
	rq1PrefixBytes := (policy.RQ1DimsForPruning + 63) / 64 * 8
	if policy.FullRQ1Rerank {
		rq1PrefixBytes = dims / 64 * 8 // need full RQ1
	}
	totalBytesPerVec := rq1PrefixBytes + dims*4 // plus float vectors

	return CeilingResult{
		Dataset:              dataset,
		Policy:               policy.Name,
		Recall10:             avgCeiling(recalls),
		P50Latency:           percentileCeiling(latencies, 0.50),
		P95Latency:           percentileCeiling(latencies, 0.95),
		P50FinalCandidates:   percentileIntCeiling(candidates, 0.50),
		P95FinalCandidates:   percentileIntCeiling(candidates, 0.95),
		P50SegmentsProcessed: percentileIntCeiling(segments, 0.50),
		P95SegmentsProcessed: percentileIntCeiling(segments, 0.95),
		AvgDistancesComputed: totalDist / int64(n),
		AvgRescoresComputed:  totalRescores / n,
		RQ1PrefixBytes:       rq1PrefixBytes,
		TotalBytesPerVec:     totalBytesPerVec,
	}
}

func computeGroundTruthCeiling(train, test [][]float32, k int) [][]uint64 {
	groundTruth := make([][]uint64, len(test))

	for i, query := range test {
		type scored struct {
			id   int
			dist float32
		}
		scores := make([]scored, len(train))
		for j, doc := range train {
			scores[j] = scored{j, cosineDistance(query, doc)}
		}

		sort.Slice(scores, func(a, b int) bool {
			return scores[a].dist < scores[b].dist
		})

		groundTruth[i] = make([]uint64, k)
		for j := 0; j < k && j < len(scores); j++ {
			groundTruth[i][j] = uint64(scores[j].id)
		}
	}

	return groundTruth
}

func writeCeilingCSV(t *testing.T, results []CeilingResult) {
	path := filepath.Join(*ceilingOutDir, "pni_recall_ceiling.csv")
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header
	w.Write([]string{
		"dataset", "policy", "recall10",
		"p50_latency_us", "p95_latency_us",
		"p50_candidates", "p95_candidates",
		"p50_segments", "p95_segments",
		"avg_distances", "avg_rescores",
		"rq1_prefix_bytes", "total_bytes_per_vec",
	})

	for _, r := range results {
		w.Write([]string{
			r.Dataset, r.Policy, fmt.Sprintf("%.4f", r.Recall10),
			fmt.Sprintf("%.1f", r.P50Latency), fmt.Sprintf("%.1f", r.P95Latency),
			fmt.Sprintf("%d", r.P50FinalCandidates), fmt.Sprintf("%d", r.P95FinalCandidates),
			fmt.Sprintf("%d", r.P50SegmentsProcessed), fmt.Sprintf("%d", r.P95SegmentsProcessed),
			fmt.Sprintf("%d", r.AvgDistancesComputed), fmt.Sprintf("%d", r.AvgRescoresComputed),
			fmt.Sprintf("%d", r.RQ1PrefixBytes), fmt.Sprintf("%d", r.TotalBytesPerVec),
		})
	}

	t.Logf("CSV written to: %s", path)
}

func writeCeilingJSON(t *testing.T, results []CeilingResult) {
	path := filepath.Join(*ceilingOutDir, "pni_recall_ceiling.json")
	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Errorf("Failed to marshal JSON: %v", err)
		return
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Errorf("Failed to write JSON: %v", err)
		return
	}

	t.Logf("JSON written to: %s", path)
}

func writeCeilingMarkdown(t *testing.T, results []CeilingResult) {
	path := filepath.Join(*ceilingOutDir, "pni_recall_ceiling.md")
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	fmt.Fprintf(f, "# PNI Recall Ceiling Experiment\n\n")
	fmt.Fprintf(f, "**Date**: %s\n\n", time.Now().Format(time.RFC3339))

	fmt.Fprintf(f, "## Configuration\n\n")
	fmt.Fprintf(f, "| Parameter | Value |\n")
	fmt.Fprintf(f, "|-----------|-------|\n")
	fmt.Fprintf(f, "| Max Queries | %d |\n", *ceilingMaxQueries)
	fmt.Fprintf(f, "| K | %d |\n", *ceilingK)

	fmt.Fprintf(f, "\n## Policy Descriptions\n\n")
	fmt.Fprintf(f, "| Policy | RQ1 Dims | Strategy | Rescore Limit |\n")
	fmt.Fprintf(f, "|--------|----------|----------|---------------|\n")
	fmt.Fprintf(f, "| Ceiling_A | 512 | Progressive pruning | 5000 |\n")
	fmt.Fprintf(f, "| Ceiling_B | 768 | Progressive pruning | 3000 |\n")
	fmt.Fprintf(f, "| Ceiling_C | 1024 | Progressive pruning | 3000 |\n")
	fmt.Fprintf(f, "| Ceiling_D | 1536 | Progressive pruning | 3000 |\n")
	fmt.Fprintf(f, "| Ceiling_E | 512→full | Progressive + full RQ1 rerank | 3000 |\n")
	fmt.Fprintf(f, "| Ceiling_F | 512→full | Progressive + full RQ1 rerank | 5000 |\n")

	// Group by dataset
	datasetResults := make(map[string][]CeilingResult)
	for _, r := range results {
		datasetResults[r.Dataset] = append(datasetResults[r.Dataset], r)
	}

	for _, ds := range []string{"dbpedia-100k-openai-ada002", "dbpedia-500k-openai-ada002", "dbpedia-openai-1000k-angular"} {
		res, ok := datasetResults[ds]
		if !ok {
			continue
		}

		fmt.Fprintf(f, "\n## %s\n\n", ds)
		fmt.Fprintf(f, "| Policy | Recall@10 | P50 Lat (µs) | P95 Lat (µs) | P50 Cands | P95 Cands | RQ1 B/vec |\n")
		fmt.Fprintf(f, "|--------|-----------|--------------|--------------|-----------|-----------|----------|\n")

		for _, r := range res {
			fmt.Fprintf(f, "| %s | **%.4f** | %.1f | %.1f | %d | %d | %d |\n",
				r.Policy, r.Recall10, r.P50Latency, r.P95Latency,
				r.P50FinalCandidates, r.P95FinalCandidates, r.RQ1PrefixBytes)
		}
	}

	fmt.Fprintf(f, "\n## Memory Estimates\n\n")
	fmt.Fprintf(f, "| RQ1 Prefix | Bytes/Vector |\n")
	fmt.Fprintf(f, "|------------|-------------|\n")
	fmt.Fprintf(f, "| 512d | 64 B |\n")
	fmt.Fprintf(f, "| 768d | 96 B |\n")
	fmt.Fprintf(f, "| 1024d | 128 B |\n")
	fmt.Fprintf(f, "| 1536d | 192 B |\n")

	t.Logf("Markdown written to: %s", path)
}

func printCeilingAnalysis(t *testing.T, results []CeilingResult) {
	t.Logf("\n==========================================================================================")
	t.Logf("RECALL CEILING ANALYSIS")
	t.Logf("==========================================================================================")

	// Question 1: Can PNI reach recall >= 0.95?
	t.Logf("\n--- Q1: Can PNI reach recall@10 >= 0.95? ---")
	for _, r := range results {
		if r.Recall10 >= 0.95 {
			t.Logf("YES: %s/%s achieves %.4f recall", r.Dataset, r.Policy, r.Recall10)
		}
	}
	anyAbove95 := false
	for _, r := range results {
		if r.Recall10 >= 0.95 {
			anyAbove95 = true
			break
		}
	}
	if !anyAbove95 {
		t.Logf("NO: No policy achieved recall >= 0.95")
	}

	// Question 2: Can PNI reach recall >= 0.98?
	t.Logf("\n--- Q2: Can PNI reach recall@10 >= 0.98? ---")
	anyAbove98 := false
	for _, r := range results {
		if r.Recall10 >= 0.98 {
			anyAbove98 = true
			t.Logf("YES: %s/%s achieves %.4f recall", r.Dataset, r.Policy, r.Recall10)
		}
	}
	if !anyAbove98 {
		t.Logf("NO: No policy achieved recall >= 0.98")
	}

	// Question 3: Cost analysis
	t.Logf("\n--- Q3: Memory/Latency/Candidate costs ---")
	for _, r := range results {
		if r.Recall10 >= 0.90 {
			t.Logf("%s/%s: recall=%.4f, latency=%.1fµs, cands=%d, mem=%dB/vec",
				r.Dataset, r.Policy, r.Recall10, r.P50Latency, r.P50FinalCandidates, r.RQ1PrefixBytes)
		}
	}

	// Question 4: Does increasing RQ1 dimensions help?
	t.Logf("\n--- Q4: Does increasing RQ1 dimensions help? ---")
	// Group by dataset and compare
	datasetResults := make(map[string][]CeilingResult)
	for _, r := range results {
		datasetResults[r.Dataset] = append(datasetResults[r.Dataset], r)
	}

	for ds, res := range datasetResults {
		t.Logf("\n%s:", ds)
		// Sort by RQ1 prefix bytes (proxy for dims)
		sorted := make([]CeilingResult, len(res))
		copy(sorted, res)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].RQ1PrefixBytes < sorted[j].RQ1PrefixBytes
		})
		for _, r := range sorted {
			t.Logf("  %s (RQ1=%dB): recall=%.4f", r.Policy, r.RQ1PrefixBytes, r.Recall10)
		}
	}

	// Question 5: Best high-recall approach
	t.Logf("\n--- Q5: Best high-recall approach ---")
	// Find best recall per dataset
	for ds, res := range datasetResults {
		var best CeilingResult
		for _, r := range res {
			if r.Recall10 > best.Recall10 {
				best = r
			}
		}
		t.Logf("%s: Best = %s (recall=%.4f, latency=%.1fµs)",
			ds, best.Policy, best.Recall10, best.P50Latency)
	}

	t.Logf("\n==========================================================================================")
}

// Helper functions for ceiling experiment

func avgCeiling(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func percentileCeiling(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func percentileIntCeiling(sorted []int, p float64) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func loadHDF5Dataset(path string, maxQueries int) (train, test [][]float32, err error) {
	tmpFile, err := os.CreateTemp("", "hdf5_data_*.json")
	if err != nil {
		return nil, nil, err
	}
	tmpPath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(tmpPath)

	scriptFile, err := os.CreateTemp("", "load_hdf5_*.py")
	if err != nil {
		return nil, nil, err
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

    data = {
        'train': train.tolist(),
        'test': test.tolist(),
    }

    with open(out_path, 'w') as out:
        json.dump(data, out)

    print(f"Loaded train: {train.shape}, test: {test.shape}")
`, path, maxQueries, tmpPath)

	if _, err := scriptFile.WriteString(script); err != nil {
		scriptFile.Close()
		return nil, nil, err
	}
	scriptFile.Close()

	cmd := exec.Command("bash", "-c", fmt.Sprintf("source /tmp/hdf5_venv/bin/activate && python3 %s", scriptPath))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, nil, fmt.Errorf("python error: %v\nOutput: %s", err, string(output))
	}
	fmt.Printf("%s\n", string(output))

	jsonData, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, nil, err
	}

	var raw struct {
		Train [][]float32 `json:"train"`
		Test  [][]float32 `json:"test"`
	}
	if err := json.Unmarshal(jsonData, &raw); err != nil {
		return nil, nil, err
	}

	return raw.Train, raw.Test, nil
}

func normalizeVectorsCeiling(vecs [][]float32) {
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
