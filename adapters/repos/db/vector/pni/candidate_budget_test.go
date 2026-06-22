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

// Candidate Budget Experiment
// Goal: Determine what recall we can achieve with practical candidate budgets (100-2000)
// after RQ1 progressive pruning.

var (
	budgetOutDir     = flag.String("budget-out", "/tmp/pni_candidate_budget", "Output directory")
	budgetMaxQueries = flag.Int("budget-max-queries", 500, "Max queries to run")
	budgetK          = flag.Int("budget-k", 10, "K for recall@K")
	budgetDatasets   = flag.String("budget-datasets", "/Users/abdel/Documents/datasets", "Datasets directory")
)

// BudgetPolicy defines a candidate budget test configuration.
type BudgetPolicy struct {
	Name string

	// RQ1DimsForPruning is how many RQ1 dimensions to use for progressive pruning.
	RQ1DimsForPruning int

	// KeepCurve maps cumulative dimensions to keep fraction.
	KeepCurve map[int]float64

	// FinalCandidates is the target number of candidates after RQ1 pruning.
	FinalCandidates int
}

// BudgetResult holds results for one policy on one dataset.
type BudgetResult struct {
	Dataset string
	Policy  string

	RQ1Dims         int
	FinalCandidates int

	Recall10 float64

	P50Latency float64
	P95Latency float64

	P50ActualCandidates int
	P95ActualCandidates int

	P50SegmentsProcessed int
	P95SegmentsProcessed int

	AvgDistancesComputed int64
	AvgRescoresComputed  int

	RQ1PrefixBytes int
}

func TestCandidateBudget(t *testing.T) {
	if os.Getenv("PNI_BENCHMARK") == "" {
		t.Skip("Skipping PNI benchmark test. Set PNI_BENCHMARK=1 to run.")
	}

	flag.Parse()

	datasets := []string{
		"dbpedia-100k-openai-ada002.hdf5",
		"dbpedia-500k-openai-ada002.hdf5",
		"dbpedia-openai-1000k-angular.hdf5",
	}

	// Final candidate targets to test
	candidateTargets := []int{100, 200, 350, 500, 750, 1000, 1500, 2000}

	// RQ1 dimension prefixes to test
	rq1Prefixes := []int{512, 768, 1024, 1536}

	// Create output directory
	if err := os.MkdirAll(*budgetOutDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	var allResults []BudgetResult

	for _, dsName := range datasets {
		dsPath := filepath.Join(*budgetDatasets, dsName)
		if _, err := os.Stat(dsPath); os.IsNotExist(err) {
			t.Logf("Skipping %s (not found)", dsName)
			continue
		}

		t.Logf("\n======================================================================")
		t.Logf("Dataset: %s", dsName)
		t.Logf("======================================================================")

		train, test, err := loadHDF5Budget(dsPath, *budgetMaxQueries)
		if err != nil {
			t.Fatalf("Failed to load dataset: %v", err)
		}

		datasetName := dsName[:len(dsName)-5] // remove .hdf5

		t.Logf("Loaded: %d docs, %d queries, %d dims", len(train), len(test), len(train[0]))

		// Normalize vectors
		t.Logf("Normalizing vectors...")
		normalizeVectorsBudget(train)
		normalizeVectorsBudget(test)

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
		groundTruth := computeGroundTruthBudget(train, test, *budgetK)

		// Generate policies for each combination
		for _, rq1Dims := range rq1Prefixes {
			for _, target := range candidateTargets {
				policy := createBudgetPolicy(rq1Dims, target, len(train))
				t.Logf("\n--- %s ---", policy.Name)

				results := runBudgetPolicy(idx, test, groundTruth, policy, *budgetK)

				result := aggregateBudgetResults(datasetName, policy, results, dims)
				allResults = append(allResults, result)

				t.Logf("Recall@%d: %.4f", *budgetK, result.Recall10)
				t.Logf("P50 latency: %.1f µs, actual candidates: %d", result.P50Latency, result.P50ActualCandidates)
			}
		}
	}

	// Write outputs
	writeBudgetCSV(t, allResults)
	writeBudgetJSON(t, allResults)
	writeBudgetMarkdown(t, allResults)

	// Print decision analysis
	printBudgetAnalysis(t, allResults)
}

func createBudgetPolicy(rq1Dims, finalTarget, docCount int) BudgetPolicy {
	// Create aggressive keep curves that reach the target
	keepCurve := make(map[int]float64)

	// Calculate intermediate targets
	// We want to prune progressively to reach finalTarget
	numSegments := rq1Dims / 64

	// Start aggressive from the beginning to reach low targets
	for seg := 1; seg <= numSegments; seg++ {
		dimsProcessed := seg * 64
		if dimsProcessed > rq1Dims {
			dimsProcessed = rq1Dims
		}

		// Linear interpolation from 80% at 64d to finalTarget at rq1Dims
		progress := float64(dimsProcessed) / float64(rq1Dims)
		targetFraction := 0.80 * (1 - progress) + float64(finalTarget)/float64(docCount)*progress

		// Ensure we're always reducing
		targetCount := int(targetFraction * float64(docCount))
		if targetCount < finalTarget {
			targetCount = finalTarget
		}

		keepCurve[dimsProcessed] = float64(targetCount)
	}

	// Force final target
	keepCurve[rq1Dims] = float64(finalTarget)

	return BudgetPolicy{
		Name:              fmt.Sprintf("RQ1_%dd_C%d", rq1Dims, finalTarget),
		RQ1DimsForPruning: rq1Dims,
		KeepCurve:         keepCurve,
		FinalCandidates:   finalTarget,
	}
}

// BudgetQueryResult holds per-query stats.
type BudgetQueryResult struct {
	Recall            float64
	Latency           time.Duration
	ActualCandidates  int
	SegmentsProcessed int
	DistancesComputed int64
	RescoresComputed  int
}

func runBudgetPolicy(idx *Index, queries [][]float32, groundTruth [][]uint64, policy BudgetPolicy, k int) []BudgetQueryResult {
	results := make([]BudgetQueryResult, len(queries))

	for i, query := range queries {
		if (i+1)%100 == 0 {
			fmt.Printf("  Query %d/%d...\n", i+1, len(queries))
		}

		start := time.Now()
		searchResult := searchBudget(idx, query, policy, k)
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

		results[i] = BudgetQueryResult{
			Recall:            float64(hits) / float64(k),
			Latency:           elapsed,
			ActualCandidates:  searchResult.Stats.FinalCandidates,
			SegmentsProcessed: searchResult.Stats.SegmentsProcessed,
			DistancesComputed: searchResult.Stats.DistancesComputed,
			RescoresComputed:  searchResult.Stats.RescoresComputed,
		}
	}

	return results
}

func searchBudget(idx *Index, query []float32, policy BudgetPolicy, k int) SearchResult {
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

		target := computeBudgetTarget(policy.KeepCurve, dimsProcessed, idx.docCount)

		// Prune if needed
		if target > 0 && aliveCount > target {
			aliveCount = pruneToK(alive[:aliveCount], accDist, target)
		}

		stats.SegmentsProcessed = seg + 1
	}

	// Force final candidate count
	if aliveCount > policy.FinalCandidates {
		aliveCount = pruneToK(alive[:aliveCount], accDist, policy.FinalCandidates)
	}

	stats.FinalCandidates = aliveCount

	// Float rescore
	candidates := alive[:aliveCount]
	result := idx.rescore(query, candidates, k, &stats)
	result.Stats = stats

	return result
}

func computeBudgetTarget(keepCurve map[int]float64, dimsProcessed, totalDocs int) int {
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

func aggregateBudgetResults(dataset string, policy BudgetPolicy, results []BudgetQueryResult, dims int) BudgetResult {
	n := len(results)
	if n == 0 {
		return BudgetResult{Dataset: dataset, Policy: policy.Name}
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
		candidates[i] = r.ActualCandidates
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

	return BudgetResult{
		Dataset:              dataset,
		Policy:               policy.Name,
		RQ1Dims:              policy.RQ1DimsForPruning,
		FinalCandidates:      policy.FinalCandidates,
		Recall10:             avgBudget(recalls),
		P50Latency:           percentileBudget(latencies, 0.50),
		P95Latency:           percentileBudget(latencies, 0.95),
		P50ActualCandidates:  percentileIntBudget(candidates, 0.50),
		P95ActualCandidates:  percentileIntBudget(candidates, 0.95),
		P50SegmentsProcessed: percentileIntBudget(segments, 0.50),
		P95SegmentsProcessed: percentileIntBudget(segments, 0.95),
		AvgDistancesComputed: totalDist / int64(n),
		AvgRescoresComputed:  totalRescores / n,
		RQ1PrefixBytes:       rq1PrefixBytes,
	}
}

func computeGroundTruthBudget(train, test [][]float32, k int) [][]uint64 {
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

func writeBudgetCSV(t *testing.T, results []BudgetResult) {
	path := filepath.Join(*budgetOutDir, "pni_candidate_budget.csv")
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
		"dataset", "policy", "rq1_dims", "final_candidates", "recall10",
		"p50_latency_us", "p95_latency_us",
		"p50_actual_candidates", "p95_actual_candidates",
		"p50_segments", "p95_segments",
		"avg_distances", "avg_rescores",
		"rq1_prefix_bytes",
	})

	for _, r := range results {
		w.Write([]string{
			r.Dataset, r.Policy, fmt.Sprintf("%d", r.RQ1Dims),
			fmt.Sprintf("%d", r.FinalCandidates), fmt.Sprintf("%.4f", r.Recall10),
			fmt.Sprintf("%.1f", r.P50Latency), fmt.Sprintf("%.1f", r.P95Latency),
			fmt.Sprintf("%d", r.P50ActualCandidates), fmt.Sprintf("%d", r.P95ActualCandidates),
			fmt.Sprintf("%d", r.P50SegmentsProcessed), fmt.Sprintf("%d", r.P95SegmentsProcessed),
			fmt.Sprintf("%d", r.AvgDistancesComputed), fmt.Sprintf("%d", r.AvgRescoresComputed),
			fmt.Sprintf("%d", r.RQ1PrefixBytes),
		})
	}

	t.Logf("CSV written to: %s", path)
}

func writeBudgetJSON(t *testing.T, results []BudgetResult) {
	path := filepath.Join(*budgetOutDir, "pni_candidate_budget.json")
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

func writeBudgetMarkdown(t *testing.T, results []BudgetResult) {
	path := filepath.Join(*budgetOutDir, "pni_candidate_budget.md")
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	fmt.Fprintf(f, "# PNI Candidate Budget Experiment\n\n")
	fmt.Fprintf(f, "**Date**: %s\n\n", time.Now().Format(time.RFC3339))
	fmt.Fprintf(f, "**Goal**: Determine what recall we can achieve with practical candidate budgets (100-2000) after RQ1 progressive pruning.\n\n")

	fmt.Fprintf(f, "## Configuration\n\n")
	fmt.Fprintf(f, "| Parameter | Value |\n")
	fmt.Fprintf(f, "|-----------|-------|\n")
	fmt.Fprintf(f, "| Max Queries | %d |\n", *budgetMaxQueries)
	fmt.Fprintf(f, "| K | %d |\n", *budgetK)
	fmt.Fprintf(f, "| Final Candidate Targets | 100, 200, 350, 500, 750, 1000, 1500, 2000 |\n")
	fmt.Fprintf(f, "| RQ1 Prefix Sizes | 512d, 768d, 1024d, 1536d |\n")

	// Memory reference
	fmt.Fprintf(f, "\n## Memory Estimates\n\n")
	fmt.Fprintf(f, "| RQ1 Prefix | Bytes/Vector |\n")
	fmt.Fprintf(f, "|------------|-------------|\n")
	fmt.Fprintf(f, "| 512d | 64 B |\n")
	fmt.Fprintf(f, "| 768d | 96 B |\n")
	fmt.Fprintf(f, "| 1024d | 128 B |\n")
	fmt.Fprintf(f, "| 1536d | 192 B |\n")

	// Group by dataset
	datasetResults := make(map[string][]BudgetResult)
	for _, r := range results {
		datasetResults[r.Dataset] = append(datasetResults[r.Dataset], r)
	}

	for _, ds := range []string{"dbpedia-100k-openai-ada002", "dbpedia-500k-openai-ada002", "dbpedia-openai-1000k-angular"} {
		res, ok := datasetResults[ds]
		if !ok {
			continue
		}

		fmt.Fprintf(f, "\n## %s\n\n", ds)

		// Create pivot table by RQ1 dims
		for _, rq1Dims := range []int{512, 768, 1024, 1536} {
			fmt.Fprintf(f, "### RQ1 %dd (%d B/vec)\n\n", rq1Dims, rq1Dims/8)
			fmt.Fprintf(f, "| Candidates | Recall@10 | P50 Lat (µs) | P95 Lat (µs) |\n")
			fmt.Fprintf(f, "|------------|-----------|--------------|-------------|\n")

			for _, r := range res {
				if r.RQ1Dims == rq1Dims {
					fmt.Fprintf(f, "| %d | **%.4f** | %.1f | %.1f |\n",
						r.FinalCandidates, r.Recall10, r.P50Latency, r.P95Latency)
				}
			}
			fmt.Fprintf(f, "\n")
		}
	}

	// Summary table: recall at 350 candidates for each RQ1 prefix
	fmt.Fprintf(f, "## Summary: Recall at 350 Candidates\n\n")
	fmt.Fprintf(f, "| Dataset | RQ1 512d | RQ1 768d | RQ1 1024d | RQ1 1536d |\n")
	fmt.Fprintf(f, "|---------|----------|----------|-----------|----------|\n")

	for _, ds := range []string{"dbpedia-100k-openai-ada002", "dbpedia-500k-openai-ada002", "dbpedia-openai-1000k-angular"} {
		res, ok := datasetResults[ds]
		if !ok {
			continue
		}

		row := fmt.Sprintf("| %s ", ds)
		for _, rq1Dims := range []int{512, 768, 1024, 1536} {
			found := false
			for _, r := range res {
				if r.RQ1Dims == rq1Dims && r.FinalCandidates == 350 {
					row += fmt.Sprintf("| %.4f ", r.Recall10)
					found = true
					break
				}
			}
			if !found {
				row += "| N/A "
			}
		}
		row += "|\n"
		fmt.Fprint(f, row)
	}

	t.Logf("Markdown written to: %s", path)
}

func printBudgetAnalysis(t *testing.T, results []BudgetResult) {
	t.Logf("\n==========================================================================================")
	t.Logf("CANDIDATE BUDGET ANALYSIS")
	t.Logf("==========================================================================================")

	// Group by dataset
	datasetResults := make(map[string][]BudgetResult)
	for _, r := range results {
		datasetResults[r.Dataset] = append(datasetResults[r.Dataset], r)
	}

	// Question 1: What recall can we achieve with 350 candidates?
	t.Logf("\n--- Q1: What recall can we achieve with 350 candidates? ---")
	for ds, res := range datasetResults {
		t.Logf("\n%s:", ds)
		for _, r := range res {
			if r.FinalCandidates == 350 {
				status := "✗"
				if r.Recall10 >= 0.95 {
					status = "✓"
				}
				t.Logf("  RQ1 %dd: recall=%.4f %s", r.RQ1Dims, r.Recall10, status)
			}
		}
	}

	// Question 2: What's the minimum candidates for 95% recall?
	t.Logf("\n--- Q2: Minimum candidates for 95%% recall ---")
	for ds, res := range datasetResults {
		t.Logf("\n%s:", ds)
		for _, rq1Dims := range []int{512, 768, 1024, 1536} {
			minCands := -1
			for _, r := range res {
				if r.RQ1Dims == rq1Dims && r.Recall10 >= 0.95 {
					if minCands == -1 || r.FinalCandidates < minCands {
						minCands = r.FinalCandidates
					}
				}
			}
			if minCands > 0 {
				t.Logf("  RQ1 %dd: %d candidates → 95%%+ recall", rq1Dims, minCands)
			} else {
				t.Logf("  RQ1 %dd: No configuration reached 95%% recall", rq1Dims)
			}
		}
	}

	// Question 3: What's the minimum candidates for 98% recall?
	t.Logf("\n--- Q3: Minimum candidates for 98%% recall ---")
	for ds, res := range datasetResults {
		t.Logf("\n%s:", ds)
		for _, rq1Dims := range []int{512, 768, 1024, 1536} {
			minCands := -1
			for _, r := range res {
				if r.RQ1Dims == rq1Dims && r.Recall10 >= 0.98 {
					if minCands == -1 || r.FinalCandidates < minCands {
						minCands = r.FinalCandidates
					}
				}
			}
			if minCands > 0 {
				t.Logf("  RQ1 %dd: %d candidates → 98%%+ recall", rq1Dims, minCands)
			} else {
				t.Logf("  RQ1 %dd: No configuration reached 98%% recall", rq1Dims)
			}
		}
	}

	// Question 4: Recall vs candidate count trend
	t.Logf("\n--- Q4: Recall trend with RQ1 1536d (best quality) ---")
	for ds, res := range datasetResults {
		t.Logf("\n%s:", ds)
		t.Logf("  Candidates → Recall@10")
		for _, r := range res {
			if r.RQ1Dims == 1536 {
				bar := ""
				bars := int(r.Recall10 * 20)
				for i := 0; i < bars; i++ {
					bar += "█"
				}
				t.Logf("  %4d → %.4f %s", r.FinalCandidates, r.Recall10, bar)
			}
		}
	}

	// Question 5: Is 350 candidates viable for production?
	t.Logf("\n--- Q5: Is 350 candidates viable for production? ---")
	viable350 := true
	for ds, res := range datasetResults {
		for _, r := range res {
			if r.FinalCandidates == 350 && r.RQ1Dims == 1536 {
				if r.Recall10 < 0.95 {
					t.Logf("  %s: NO - recall %.4f < 0.95 (with full RQ1 1536d)", ds, r.Recall10)
					viable350 = false
				} else {
					t.Logf("  %s: YES - recall %.4f >= 0.95", ds, r.Recall10)
				}
			}
		}
	}
	if viable350 {
		t.Logf("\n  CONCLUSION: 350 candidates appears viable with RQ1 1536d for 95%% recall target")
	} else {
		t.Logf("\n  CONCLUSION: 350 candidates NOT viable for 95%% recall - need more candidates")
	}

	// Decision summary
	t.Logf("\n--- DECISION SUMMARY ---")
	t.Logf("The experiment answers: What recall do we get if the RQ1 stage outputs only N candidates?")
	t.Logf("")
	t.Logf("Key findings:")

	// Find best configuration per recall target
	for ds, res := range datasetResults {
		t.Logf("\n%s:", ds)
		// Best config for 95% recall
		best95 := struct {
			dims, cands int
			recall      float64
		}{}
		for _, r := range res {
			if r.Recall10 >= 0.95 {
				if best95.cands == 0 || r.FinalCandidates < best95.cands || (r.FinalCandidates == best95.cands && r.RQ1Dims < best95.dims) {
					best95.dims = r.RQ1Dims
					best95.cands = r.FinalCandidates
					best95.recall = r.Recall10
				}
			}
		}
		if best95.cands > 0 {
			t.Logf("  95%% recall: RQ1 %dd, %d candidates (actual: %.4f)", best95.dims, best95.cands, best95.recall)
		}

		// Best config for 98% recall
		best98 := struct {
			dims, cands int
			recall      float64
		}{}
		for _, r := range res {
			if r.Recall10 >= 0.98 {
				if best98.cands == 0 || r.FinalCandidates < best98.cands || (r.FinalCandidates == best98.cands && r.RQ1Dims < best98.dims) {
					best98.dims = r.RQ1Dims
					best98.cands = r.FinalCandidates
					best98.recall = r.Recall10
				}
			}
		}
		if best98.cands > 0 {
			t.Logf("  98%% recall: RQ1 %dd, %d candidates (actual: %.4f)", best98.dims, best98.cands, best98.recall)
		}
	}

	t.Logf("\n==========================================================================================")
}

// Helper functions for budget experiment

func avgBudget(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func percentileBudget(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func percentileIntBudget(sorted []int, p float64) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func loadHDF5Budget(path string, maxQueries int) (train, test [][]float32, err error) {
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

func normalizeVectorsBudget(vecs [][]float32) {
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
