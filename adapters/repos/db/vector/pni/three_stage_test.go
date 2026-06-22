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

// 3-Stage PNI Experiment
// Architecture:
// Stage 1: RQ1 prefix progressive pruning (in-memory, fast)
// Stage 2: SQ8 full-dimensional scoring (simulated disk read)
// Stage 3: Float rescore final candidates (simulated disk read)

var (
	threeStageOutDir     = flag.String("three-stage-out", "/tmp/pni_three_stage", "Output directory")
	threeStageMaxQueries = flag.Int("three-stage-max-queries", 500, "Max queries to run")
	threeStageK          = flag.Int("three-stage-k", 10, "K for recall@K")
	threeStageDatasets   = flag.String("three-stage-datasets", "/Users/abdel/Documents/datasets", "Datasets directory")
)

// ThreeStagePolicy defines a 3-stage search policy.
type ThreeStagePolicy struct {
	Name string

	// Stage 1: RQ1 prefix
	Stage1RQ1Dims    int // RQ1 dimensions to use (512, 768, 1024)
	Stage1Candidates int // reduce to this many candidates

	// Stage 2: SQ8 full dims
	Stage2Candidates int // reduce to this many candidates

	// Stage 3: Float rescore
	Stage3K int // final top-K
}

// ThreeStageIndex extends Index with SQ8 quantized vectors.
type ThreeStageIndex struct {
	*Index

	// SQ8 quantized vectors: uint8 per dimension
	sq8Vectors [][]uint8

	// SQ8 scaling parameters (per-dimension)
	sq8Min   []float32
	sq8Scale []float32
}

// ThreeStageResult holds results for one policy on one dataset.
type ThreeStageResult struct {
	Dataset string
	Policy  string

	Recall10 float64

	// Latencies
	P50TotalLatency  float64
	P95TotalLatency  float64
	P50Stage1Latency float64
	P95Stage1Latency float64
	P50Stage2Latency float64
	P95Stage2Latency float64
	P50Stage3Latency float64
	P95Stage3Latency float64

	// Candidates
	P50Stage1Candidates int
	P95Stage1Candidates int
	P50Stage2Candidates int
	P95Stage2Candidates int

	// Memory (RAM)
	RAMBytesPerVec int // RQ1 prefix only

	// Simulated disk I/O per query
	P50SQ8DiskBytes   int64
	P95SQ8DiskBytes   int64
	P50FloatDiskBytes int64
	P95FloatDiskBytes int64
	P50TotalDiskBytes int64
	P95TotalDiskBytes int64
}

// ThreeStageQueryResult holds per-query stats.
type ThreeStageQueryResult struct {
	Recall float64

	TotalLatency  time.Duration
	Stage1Latency time.Duration
	Stage2Latency time.Duration
	Stage3Latency time.Duration

	Stage1Candidates int
	Stage2Candidates int

	SQ8DiskBytes   int64
	FloatDiskBytes int64
}

func TestThreeStage(t *testing.T) {
	flag.Parse()

	datasets := []string{
		"dbpedia-100k-openai-ada002.hdf5",
		"dbpedia-500k-openai-ada002.hdf5",
		"dbpedia-openai-1000k-angular.hdf5",
	}

	policies := []ThreeStagePolicy{
		// ThreeStage_A: baseline
		{
			Name:             "ThreeStage_A",
			Stage1RQ1Dims:    512,
			Stage1Candidates: 500,
			Stage2Candidates: 50,
			Stage3K:          10,
		},
		// ThreeStage_B: more Stage1 candidates
		{
			Name:             "ThreeStage_B",
			Stage1RQ1Dims:    512,
			Stage1Candidates: 1000,
			Stage2Candidates: 50,
			Stage3K:          10,
		},
		// ThreeStage_C: more candidates through pipeline
		{
			Name:             "ThreeStage_C",
			Stage1RQ1Dims:    512,
			Stage1Candidates: 2000,
			Stage2Candidates: 100,
			Stage3K:          10,
		},
		// ThreeStage_D: longer RQ1 prefix
		{
			Name:             "ThreeStage_D",
			Stage1RQ1Dims:    768,
			Stage1Candidates: 1000,
			Stage2Candidates: 50,
			Stage3K:          10,
		},
		// ThreeStage_E: even longer RQ1 prefix
		{
			Name:             "ThreeStage_E",
			Stage1RQ1Dims:    1024,
			Stage1Candidates: 1000,
			Stage2Candidates: 50,
			Stage3K:          10,
		},
		// ThreeStage_F: high candidate count
		{
			Name:             "ThreeStage_F",
			Stage1RQ1Dims:    512,
			Stage1Candidates: 5000,
			Stage2Candidates: 100,
			Stage3K:          10,
		},
	}

	// Baselines for comparison
	baselinePolicies := []struct {
		Name   string
		Search func(*ThreeStageIndex, []float32, int) (SearchResult, time.Duration)
	}{
		{
			Name: "D_adaptive",
			Search: func(idx *ThreeStageIndex, query []float32, k int) (SearchResult, time.Duration) {
				start := time.Now()
				result := idx.Index.SearchAdaptive(query, DefaultAdaptivePolicy(), k)
				return result, time.Since(start)
			},
		},
		{
			Name: "E4_margin24",
			Search: func(idx *ThreeStageIndex, query []float32, k int) (SearchResult, time.Duration) {
				start := time.Now()
				result := idx.Index.SearchMargin(query, PolicyE4(k))
				return result.SearchResult, time.Since(start)
			},
		},
	}

	// Create output directory
	if err := os.MkdirAll(*threeStageOutDir, 0755); err != nil {
		t.Fatalf("Failed to create output dir: %v", err)
	}

	var allResults []ThreeStageResult
	var baselineResults []ThreeStageResult

	for _, dsName := range datasets {
		dsPath := filepath.Join(*threeStageDatasets, dsName)
		if _, err := os.Stat(dsPath); os.IsNotExist(err) {
			t.Logf("Skipping %s (not found)", dsName)
			continue
		}

		t.Logf("\n======================================================================")
		t.Logf("Dataset: %s", dsName)
		t.Logf("======================================================================")

		train, test, err := loadHDF5ThreeStage(dsPath, *threeStageMaxQueries)
		if err != nil {
			t.Fatalf("Failed to load dataset: %v", err)
		}

		datasetName := dsName[:len(dsName)-5] // remove .hdf5

		t.Logf("Loaded: %d docs, %d queries, %d dims", len(train), len(test), len(train[0]))

		// Normalize vectors
		t.Logf("Normalizing vectors...")
		normalizeVectorsThreeStage(train)
		normalizeVectorsThreeStage(test)

		// Build 3-stage index
		t.Logf("Building 3-stage PNI index...")
		dims := len(train[0])
		idx := NewThreeStageIndex(dims, 42)
		buildStart := time.Now()
		if err := idx.Build(train); err != nil {
			t.Fatalf("Build failed: %v", err)
		}
		t.Logf("Build time: %v", time.Since(buildStart))
		t.Logf("SQ8 quantization complete")

		// Compute ground truth
		t.Logf("Computing ground truth...")
		groundTruth := computeGroundTruthThreeStage(train, test, *threeStageK)

		// Run baselines first
		for _, baseline := range baselinePolicies {
			t.Logf("\n--- %s (baseline) ---", baseline.Name)

			results := runBaselinePolicy(idx, test, groundTruth, baseline.Search, *threeStageK)

			// Aggregate
			recalls := make([]float64, len(results))
			latencies := make([]float64, len(results))
			for i, r := range results {
				recalls[i] = r.Recall
				latencies[i] = float64(r.Latency.Microseconds())
			}
			sort.Float64s(latencies)

			result := ThreeStageResult{
				Dataset:         datasetName,
				Policy:          baseline.Name,
				Recall10:        avgThreeStage(recalls),
				P50TotalLatency: percentileThreeStage(latencies, 0.50),
				P95TotalLatency: percentileThreeStage(latencies, 0.95),
				RAMBytesPerVec:  dims / 64 * 8, // full RQ1
			}
			baselineResults = append(baselineResults, result)

			t.Logf("Recall@%d: %.4f", *threeStageK, result.Recall10)
			t.Logf("P50 latency: %.1f µs, P95: %.1f µs", result.P50TotalLatency, result.P95TotalLatency)
		}

		// Run 3-stage policies
		for _, policy := range policies {
			t.Logf("\n--- %s ---", policy.Name)

			results := runThreeStagePolicy(idx, test, groundTruth, policy)

			result := aggregateThreeStageResults(datasetName, policy, results, dims)
			allResults = append(allResults, result)

			t.Logf("Recall@%d: %.4f", *threeStageK, result.Recall10)
			t.Logf("P50 total latency: %.1f µs", result.P50TotalLatency)
			t.Logf("  Stage1: %.1f µs, Stage2: %.1f µs, Stage3: %.1f µs",
				result.P50Stage1Latency, result.P50Stage2Latency, result.P50Stage3Latency)
			t.Logf("P50 candidates: Stage1=%d, Stage2=%d",
				result.P50Stage1Candidates, result.P50Stage2Candidates)
			t.Logf("RAM: %d B/vec, Disk/query: SQ8=%.1fKB, Float=%.1fKB",
				result.RAMBytesPerVec,
				float64(result.P50SQ8DiskBytes)/1024,
				float64(result.P50FloatDiskBytes)/1024)
		}
	}

	// Write outputs
	writeThreeStageCSV(t, allResults, baselineResults)
	writeThreeStageJSON(t, allResults, baselineResults)
	writeThreeStageMarkdown(t, allResults, baselineResults)

	// Print analysis
	printThreeStageAnalysis(t, allResults, baselineResults)
}

type BaselineQueryResult struct {
	Recall  float64
	Latency time.Duration
}

func runBaselinePolicy(idx *ThreeStageIndex, queries [][]float32, groundTruth [][]uint64,
	searchFn func(*ThreeStageIndex, []float32, int) (SearchResult, time.Duration), k int) []BaselineQueryResult {

	results := make([]BaselineQueryResult, len(queries))

	for i, query := range queries {
		if (i+1)%100 == 0 {
			fmt.Printf("  Query %d/%d...\n", i+1, len(queries))
		}

		searchResult, latency := searchFn(idx, query, k)

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

		results[i] = BaselineQueryResult{
			Recall:  float64(hits) / float64(k),
			Latency: latency,
		}
	}

	return results
}

func runThreeStagePolicy(idx *ThreeStageIndex, queries [][]float32, groundTruth [][]uint64, policy ThreeStagePolicy) []ThreeStageQueryResult {
	results := make([]ThreeStageQueryResult, len(queries))

	for i, query := range queries {
		if (i+1)%100 == 0 {
			fmt.Printf("  Query %d/%d...\n", i+1, len(queries))
		}

		result := searchThreeStage(idx, query, policy)

		// Compute recall
		hits := 0
		gtSet := make(map[uint64]bool)
		for _, id := range groundTruth[i] {
			gtSet[id] = true
		}
		for _, id := range result.IDs {
			if gtSet[id] {
				hits++
			}
		}

		results[i] = ThreeStageQueryResult{
			Recall:           float64(hits) / float64(policy.Stage3K),
			TotalLatency:     result.TotalLatency,
			Stage1Latency:    result.Stage1Latency,
			Stage2Latency:    result.Stage2Latency,
			Stage3Latency:    result.Stage3Latency,
			Stage1Candidates: result.Stage1Candidates,
			Stage2Candidates: result.Stage2Candidates,
			SQ8DiskBytes:     result.SQ8DiskBytes,
			FloatDiskBytes:   result.FloatDiskBytes,
		}
	}

	return results
}

// ThreeStageSearchResult holds search results with stage-wise stats.
type ThreeStageSearchResult struct {
	IDs       []uint64
	Distances []float32

	TotalLatency  time.Duration
	Stage1Latency time.Duration
	Stage2Latency time.Duration
	Stage3Latency time.Duration

	Stage1Candidates int
	Stage2Candidates int

	SQ8DiskBytes   int64
	FloatDiskBytes int64
}

func searchThreeStage(idx *ThreeStageIndex, query []float32, policy ThreeStagePolicy) ThreeStageSearchResult {
	totalStart := time.Now()

	// Stage 1: RQ1 prefix progressive pruning
	stage1Start := time.Now()

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

	// Progressive pruning with RQ1 prefix
	maxPruningSeg := (policy.Stage1RQ1Dims + 63) / 64
	if maxPruningSeg > idx.segmentCount {
		maxPruningSeg = idx.segmentCount
	}

	// Define keep curve for Stage 1
	keepCurve := map[int]float64{
		64:  0.30, // 30%
		128: 0.10, // 10%
		256: 0.02, // 2%
	}
	keepCurve[policy.Stage1RQ1Dims] = float64(policy.Stage1Candidates)

	for seg := 0; seg < maxPruningSeg; seg++ {
		queryCode := queryCodes[seg]
		segmentData := idx.segments[seg]

		// Compute distances for alive candidates
		for i := 0; i < aliveCount; i++ {
			docID := alive[i]
			accDist[docID] += bits.OnesCount64(queryCode ^ segmentData[docID])
		}

		// Compute adaptive target
		dimsProcessed := (seg + 1) * 64
		if dimsProcessed > policy.Stage1RQ1Dims {
			dimsProcessed = policy.Stage1RQ1Dims
		}

		target := computeCeilingTarget(keepCurve, dimsProcessed, idx.docCount)

		if target > 0 && aliveCount > target {
			aliveCount = pruneToK(alive[:aliveCount], accDist, target)
		}
	}

	// Ensure we have at most Stage1Candidates
	if aliveCount > policy.Stage1Candidates {
		aliveCount = pruneToK(alive[:aliveCount], accDist, policy.Stage1Candidates)
	}

	stage1Latency := time.Since(stage1Start)
	stage1Candidates := aliveCount

	// Stage 2: SQ8 scoring on Stage 1 candidates
	stage2Start := time.Now()

	// Simulate disk read for SQ8 vectors
	sq8DiskBytes := int64(aliveCount) * int64(idx.dims) * 1 // 1 byte per dim

	// Quantize query to SQ8
	querySQ8 := idx.QuantizeToSQ8(query)

	// Score with SQ8
	type sq8Scored struct {
		id   int
		dist int // L2 in quantized space
	}
	sq8Scores := make([]sq8Scored, aliveCount)
	for i := 0; i < aliveCount; i++ {
		docID := alive[i]
		dist := idx.SQ8Distance(querySQ8, idx.sq8Vectors[docID])
		sq8Scores[i] = sq8Scored{docID, dist}
	}

	// Sort by SQ8 distance
	sort.Slice(sq8Scores, func(i, j int) bool {
		return sq8Scores[i].dist < sq8Scores[j].dist
	})

	// Keep top Stage2Candidates
	stage2Count := policy.Stage2Candidates
	if stage2Count > len(sq8Scores) {
		stage2Count = len(sq8Scores)
	}

	stage2Latency := time.Since(stage2Start)
	stage2Candidates := stage2Count

	// Stage 3: Float rescore
	stage3Start := time.Now()

	// Simulate disk read for float vectors
	floatDiskBytes := int64(stage2Count) * int64(idx.dims) * 4 // 4 bytes per dim

	// Rescore with float
	type floatScored struct {
		id   uint64
		dist float32
	}
	floatScores := make([]floatScored, stage2Count)
	for i := 0; i < stage2Count; i++ {
		docID := sq8Scores[i].id
		dist := cosineDistance(query, idx.vectors[docID])
		floatScores[i] = floatScored{uint64(docID), dist}
	}

	// Sort by float distance
	sort.Slice(floatScores, func(i, j int) bool {
		return floatScores[i].dist < floatScores[j].dist
	})

	// Return top-K
	k := policy.Stage3K
	if k > len(floatScores) {
		k = len(floatScores)
	}

	ids := make([]uint64, k)
	distances := make([]float32, k)
	for i := 0; i < k; i++ {
		ids[i] = floatScores[i].id
		distances[i] = floatScores[i].dist
	}

	stage3Latency := time.Since(stage3Start)
	totalLatency := time.Since(totalStart)

	return ThreeStageSearchResult{
		IDs:              ids,
		Distances:        distances,
		TotalLatency:     totalLatency,
		Stage1Latency:    stage1Latency,
		Stage2Latency:    stage2Latency,
		Stage3Latency:    stage3Latency,
		Stage1Candidates: stage1Candidates,
		Stage2Candidates: stage2Candidates,
		SQ8DiskBytes:     sq8DiskBytes,
		FloatDiskBytes:   floatDiskBytes,
	}
}

// NewThreeStageIndex creates a new 3-stage index.
func NewThreeStageIndex(dims int, seed uint64) *ThreeStageIndex {
	return &ThreeStageIndex{
		Index: NewIndex(dims, seed),
	}
}

// Build builds the 3-stage index including SQ8 quantization.
func (idx *ThreeStageIndex) Build(vectors [][]float32) error {
	// Build base index
	if err := idx.Index.Build(vectors); err != nil {
		return err
	}

	// Compute SQ8 parameters (per-dimension min/scale)
	dims := idx.dims
	idx.sq8Min = make([]float32, dims)
	idx.sq8Scale = make([]float32, dims)

	// Find min/max per dimension
	sq8Max := make([]float32, dims)
	for d := 0; d < dims; d++ {
		idx.sq8Min[d] = vectors[0][d]
		sq8Max[d] = vectors[0][d]
	}

	for _, vec := range vectors {
		for d := 0; d < dims; d++ {
			if vec[d] < idx.sq8Min[d] {
				idx.sq8Min[d] = vec[d]
			}
			if vec[d] > sq8Max[d] {
				sq8Max[d] = vec[d]
			}
		}
	}

	// Compute scale (map [min, max] to [0, 255])
	for d := 0; d < dims; d++ {
		range_ := sq8Max[d] - idx.sq8Min[d]
		if range_ > 0 {
			idx.sq8Scale[d] = 255.0 / range_
		} else {
			idx.sq8Scale[d] = 1.0
		}
	}

	// Quantize all vectors
	idx.sq8Vectors = make([][]uint8, len(vectors))
	for i, vec := range vectors {
		idx.sq8Vectors[i] = idx.QuantizeToSQ8(vec)
	}

	return nil
}

// QuantizeToSQ8 quantizes a float vector to SQ8.
func (idx *ThreeStageIndex) QuantizeToSQ8(vec []float32) []uint8 {
	result := make([]uint8, len(vec))
	for d := 0; d < len(vec); d++ {
		val := (vec[d] - idx.sq8Min[d]) * idx.sq8Scale[d]
		if val < 0 {
			val = 0
		}
		if val > 255 {
			val = 255
		}
		result[d] = uint8(val)
	}
	return result
}

// SQ8Distance computes L2 distance in SQ8 space.
func (idx *ThreeStageIndex) SQ8Distance(a, b []uint8) int {
	dist := 0
	for d := 0; d < len(a); d++ {
		diff := int(a[d]) - int(b[d])
		dist += diff * diff
	}
	return dist
}

func aggregateThreeStageResults(dataset string, policy ThreeStagePolicy, results []ThreeStageQueryResult, dims int) ThreeStageResult {
	n := len(results)
	if n == 0 {
		return ThreeStageResult{Dataset: dataset, Policy: policy.Name}
	}

	recalls := make([]float64, n)
	totalLats := make([]float64, n)
	stage1Lats := make([]float64, n)
	stage2Lats := make([]float64, n)
	stage3Lats := make([]float64, n)
	stage1Cands := make([]int, n)
	stage2Cands := make([]int, n)
	sq8Bytes := make([]int64, n)
	floatBytes := make([]int64, n)
	totalBytes := make([]int64, n)

	for i, r := range results {
		recalls[i] = r.Recall
		totalLats[i] = float64(r.TotalLatency.Microseconds())
		stage1Lats[i] = float64(r.Stage1Latency.Microseconds())
		stage2Lats[i] = float64(r.Stage2Latency.Microseconds())
		stage3Lats[i] = float64(r.Stage3Latency.Microseconds())
		stage1Cands[i] = r.Stage1Candidates
		stage2Cands[i] = r.Stage2Candidates
		sq8Bytes[i] = r.SQ8DiskBytes
		floatBytes[i] = r.FloatDiskBytes
		totalBytes[i] = r.SQ8DiskBytes + r.FloatDiskBytes
	}

	sort.Float64s(totalLats)
	sort.Float64s(stage1Lats)
	sort.Float64s(stage2Lats)
	sort.Float64s(stage3Lats)
	sort.Ints(stage1Cands)
	sort.Ints(stage2Cands)
	sort.Slice(sq8Bytes, func(i, j int) bool { return sq8Bytes[i] < sq8Bytes[j] })
	sort.Slice(floatBytes, func(i, j int) bool { return floatBytes[i] < floatBytes[j] })
	sort.Slice(totalBytes, func(i, j int) bool { return totalBytes[i] < totalBytes[j] })

	// RAM: only RQ1 prefix
	ramBytesPerVec := (policy.Stage1RQ1Dims + 63) / 64 * 8

	return ThreeStageResult{
		Dataset:             dataset,
		Policy:              policy.Name,
		Recall10:            avgThreeStage(recalls),
		P50TotalLatency:     percentileThreeStage(totalLats, 0.50),
		P95TotalLatency:     percentileThreeStage(totalLats, 0.95),
		P50Stage1Latency:    percentileThreeStage(stage1Lats, 0.50),
		P95Stage1Latency:    percentileThreeStage(stage1Lats, 0.95),
		P50Stage2Latency:    percentileThreeStage(stage2Lats, 0.50),
		P95Stage2Latency:    percentileThreeStage(stage2Lats, 0.95),
		P50Stage3Latency:    percentileThreeStage(stage3Lats, 0.50),
		P95Stage3Latency:    percentileThreeStage(stage3Lats, 0.95),
		P50Stage1Candidates: percentileIntThreeStage(stage1Cands, 0.50),
		P95Stage1Candidates: percentileIntThreeStage(stage1Cands, 0.95),
		P50Stage2Candidates: percentileIntThreeStage(stage2Cands, 0.50),
		P95Stage2Candidates: percentileIntThreeStage(stage2Cands, 0.95),
		RAMBytesPerVec:      ramBytesPerVec,
		P50SQ8DiskBytes:     percentileInt64(sq8Bytes, 0.50),
		P95SQ8DiskBytes:     percentileInt64(sq8Bytes, 0.95),
		P50FloatDiskBytes:   percentileInt64(floatBytes, 0.50),
		P95FloatDiskBytes:   percentileInt64(floatBytes, 0.95),
		P50TotalDiskBytes:   percentileInt64(totalBytes, 0.50),
		P95TotalDiskBytes:   percentileInt64(totalBytes, 0.95),
	}
}

func percentileInt64(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func avgThreeStage(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
}

func percentileThreeStage(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func percentileIntThreeStage(sorted []int, p float64) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}

func writeThreeStageCSV(t *testing.T, results, baselines []ThreeStageResult) {
	path := filepath.Join(*threeStageOutDir, "pni_three_stage.csv")
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
		"p50_total_lat_us", "p95_total_lat_us",
		"p50_stage1_lat_us", "p50_stage2_lat_us", "p50_stage3_lat_us",
		"p50_stage1_cands", "p50_stage2_cands",
		"ram_bytes_per_vec",
		"p50_sq8_disk_bytes", "p50_float_disk_bytes", "p50_total_disk_bytes",
	})

	// Write baselines
	for _, r := range baselines {
		w.Write([]string{
			r.Dataset, r.Policy, fmt.Sprintf("%.4f", r.Recall10),
			fmt.Sprintf("%.1f", r.P50TotalLatency), fmt.Sprintf("%.1f", r.P95TotalLatency),
			"-", "-", "-",
			"-", "-",
			fmt.Sprintf("%d", r.RAMBytesPerVec),
			"-", "-", "-",
		})
	}

	// Write 3-stage results
	for _, r := range results {
		w.Write([]string{
			r.Dataset, r.Policy, fmt.Sprintf("%.4f", r.Recall10),
			fmt.Sprintf("%.1f", r.P50TotalLatency), fmt.Sprintf("%.1f", r.P95TotalLatency),
			fmt.Sprintf("%.1f", r.P50Stage1Latency), fmt.Sprintf("%.1f", r.P50Stage2Latency), fmt.Sprintf("%.1f", r.P50Stage3Latency),
			fmt.Sprintf("%d", r.P50Stage1Candidates), fmt.Sprintf("%d", r.P50Stage2Candidates),
			fmt.Sprintf("%d", r.RAMBytesPerVec),
			fmt.Sprintf("%d", r.P50SQ8DiskBytes), fmt.Sprintf("%d", r.P50FloatDiskBytes), fmt.Sprintf("%d", r.P50TotalDiskBytes),
		})
	}

	t.Logf("CSV written to: %s", path)
}

func writeThreeStageJSON(t *testing.T, results, baselines []ThreeStageResult) {
	path := filepath.Join(*threeStageOutDir, "pni_three_stage.json")

	data := struct {
		ThreeStage []ThreeStageResult `json:"three_stage"`
		Baselines  []ThreeStageResult `json:"baselines"`
	}{
		ThreeStage: results,
		Baselines:  baselines,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		t.Errorf("Failed to marshal JSON: %v", err)
		return
	}

	if err := os.WriteFile(path, jsonData, 0644); err != nil {
		t.Errorf("Failed to write JSON: %v", err)
		return
	}

	t.Logf("JSON written to: %s", path)
}

func writeThreeStageMarkdown(t *testing.T, results, baselines []ThreeStageResult) {
	path := filepath.Join(*threeStageOutDir, "pni_three_stage.md")
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	fmt.Fprintf(f, "# 3-Stage PNI Experiment\n\n")
	fmt.Fprintf(f, "**Date**: %s\n\n", time.Now().Format(time.RFC3339))

	fmt.Fprintf(f, "## Architecture\n\n")
	fmt.Fprintf(f, "```\n")
	fmt.Fprintf(f, "Stage 1: RQ1 prefix progressive pruning (in-memory)\n")
	fmt.Fprintf(f, "Stage 2: SQ8 full-dim scoring (simulated disk)\n")
	fmt.Fprintf(f, "Stage 3: Float rescore final candidates (simulated disk)\n")
	fmt.Fprintf(f, "```\n\n")

	fmt.Fprintf(f, "## Policies\n\n")
	fmt.Fprintf(f, "| Policy | Stage1 RQ1 Dims | Stage1 Cands | Stage2 Cands | Stage3 K |\n")
	fmt.Fprintf(f, "|--------|-----------------|--------------|--------------|----------|\n")
	fmt.Fprintf(f, "| ThreeStage_A | 512 | 500 | 50 | 10 |\n")
	fmt.Fprintf(f, "| ThreeStage_B | 512 | 1000 | 50 | 10 |\n")
	fmt.Fprintf(f, "| ThreeStage_C | 512 | 2000 | 100 | 10 |\n")
	fmt.Fprintf(f, "| ThreeStage_D | 768 | 1000 | 50 | 10 |\n")
	fmt.Fprintf(f, "| ThreeStage_E | 1024 | 1000 | 50 | 10 |\n")
	fmt.Fprintf(f, "| ThreeStage_F | 512 | 5000 | 100 | 10 |\n")

	// Group by dataset
	datasetResults := make(map[string][]ThreeStageResult)
	for _, r := range results {
		datasetResults[r.Dataset] = append(datasetResults[r.Dataset], r)
	}
	datasetBaselines := make(map[string][]ThreeStageResult)
	for _, r := range baselines {
		datasetBaselines[r.Dataset] = append(datasetBaselines[r.Dataset], r)
	}

	for _, ds := range []string{"dbpedia-100k-openai-ada002", "dbpedia-500k-openai-ada002", "dbpedia-openai-1000k-angular"} {
		res, ok := datasetResults[ds]
		if !ok {
			continue
		}

		fmt.Fprintf(f, "\n## %s\n\n", ds)

		// Baselines
		fmt.Fprintf(f, "### Baselines\n\n")
		fmt.Fprintf(f, "| Policy | Recall@10 | P50 Lat (µs) | RAM B/vec |\n")
		fmt.Fprintf(f, "|--------|-----------|--------------|----------|\n")
		for _, b := range datasetBaselines[ds] {
			fmt.Fprintf(f, "| %s | **%.4f** | %.1f | %d |\n",
				b.Policy, b.Recall10, b.P50TotalLatency, b.RAMBytesPerVec)
		}

		// 3-stage results
		fmt.Fprintf(f, "\n### 3-Stage Results\n\n")
		fmt.Fprintf(f, "| Policy | Recall@10 | P50 Total (µs) | P50 S1 | P50 S2 | P50 S3 | S1 Cands | S2 Cands | RAM B/vec | SQ8 KB | Float KB |\n")
		fmt.Fprintf(f, "|--------|-----------|----------------|--------|--------|--------|----------|----------|----------|--------|----------|\n")

		for _, r := range res {
			fmt.Fprintf(f, "| %s | **%.4f** | %.1f | %.1f | %.1f | %.1f | %d | %d | %d | %.1f | %.1f |\n",
				r.Policy, r.Recall10, r.P50TotalLatency,
				r.P50Stage1Latency, r.P50Stage2Latency, r.P50Stage3Latency,
				r.P50Stage1Candidates, r.P50Stage2Candidates,
				r.RAMBytesPerVec,
				float64(r.P50SQ8DiskBytes)/1024,
				float64(r.P50FloatDiskBytes)/1024)
		}
	}

	fmt.Fprintf(f, "\n## Memory Model\n\n")
	fmt.Fprintf(f, "| Component | Bytes/Vector |\n")
	fmt.Fprintf(f, "|-----------|-------------|\n")
	fmt.Fprintf(f, "| RQ1 512d prefix | 64 B |\n")
	fmt.Fprintf(f, "| RQ1 768d prefix | 96 B |\n")
	fmt.Fprintf(f, "| RQ1 1024d prefix | 128 B |\n")
	fmt.Fprintf(f, "| SQ8 full (1536d) | 1536 B (disk) |\n")
	fmt.Fprintf(f, "| Float full (1536d) | 6144 B (disk) |\n")

	t.Logf("Markdown written to: %s", path)
}

func printThreeStageAnalysis(t *testing.T, results, baselines []ThreeStageResult) {
	t.Logf("\n==========================================================================================")
	t.Logf("3-STAGE PNI ANALYSIS")
	t.Logf("==========================================================================================")

	// Decision criteria check
	t.Logf("\n--- Decision Criteria ---")
	t.Logf("3-stage is promising if:")
	t.Logf("  - recall@10 >= 0.95")
	t.Logf("  - RAM ~64-128 B/vector")
	t.Logf("  - Stage 2 disk <= few MB/query")
	t.Logf("  - Stage 3 disk <= few hundred KB/query")
	t.Logf("  - P95 latency in 10-100ms class")

	// Check each criterion
	t.Logf("\n--- Results vs Criteria ---")
	for _, r := range results {
		meetsRecall := r.Recall10 >= 0.95
		meetsRAM := r.RAMBytesPerVec <= 128
		meetsSQ8Disk := r.P50SQ8DiskBytes <= 5*1024*1024 // 5MB
		meetsFloatDisk := r.P50FloatDiskBytes <= 500*1024 // 500KB
		meetsLatency := r.P95TotalLatency <= 100000       // 100ms

		status := "PROMISING"
		if !meetsRecall {
			status = "RECALL TOO LOW"
		}

		t.Logf("%s/%s: %s", r.Dataset, r.Policy, status)
		t.Logf("  recall=%.4f (>= 0.95? %v)", r.Recall10, meetsRecall)
		t.Logf("  RAM=%dB/vec (<= 128? %v)", r.RAMBytesPerVec, meetsRAM)
		t.Logf("  SQ8=%.1fKB (<= 5MB? %v)", float64(r.P50SQ8DiskBytes)/1024, meetsSQ8Disk)
		t.Logf("  Float=%.1fKB (<= 500KB? %v)", float64(r.P50FloatDiskBytes)/1024, meetsFloatDisk)
		t.Logf("  P95 lat=%.1fµs (<= 100ms? %v)", r.P95TotalLatency, meetsLatency)
	}

	// Compare to baselines
	t.Logf("\n--- Comparison to Baselines ---")
	datasetBaselines := make(map[string][]ThreeStageResult)
	for _, b := range baselines {
		datasetBaselines[b.Dataset] = append(datasetBaselines[b.Dataset], b)
	}

	for _, r := range results {
		for _, b := range datasetBaselines[r.Dataset] {
			if b.Policy == "D_adaptive" {
				recallDelta := r.Recall10 - b.Recall10
				ramRatio := float64(r.RAMBytesPerVec) / float64(b.RAMBytesPerVec)
				t.Logf("%s/%s vs D_adaptive: recall delta=%.4f, RAM ratio=%.2fx",
					r.Dataset, r.Policy, recallDelta, ramRatio)
			}
		}
	}

	// Find best 3-stage policy per dataset
	t.Logf("\n--- Best 3-Stage Policy per Dataset ---")
	datasetResults := make(map[string][]ThreeStageResult)
	for _, r := range results {
		datasetResults[r.Dataset] = append(datasetResults[r.Dataset], r)
	}

	for ds, res := range datasetResults {
		var best ThreeStageResult
		for _, r := range res {
			if r.Recall10 > best.Recall10 {
				best = r
			}
		}
		t.Logf("%s: Best = %s (recall=%.4f, RAM=%dB/vec, disk=%.1fKB)",
			ds, best.Policy, best.Recall10, best.RAMBytesPerVec,
			float64(best.P50TotalDiskBytes)/1024)
	}

	// Tradeoff analysis
	t.Logf("\n--- RAM vs Recall Tradeoff ---")
	for ds, res := range datasetResults {
		t.Logf("%s:", ds)
		// Sort by RAM
		sorted := make([]ThreeStageResult, len(res))
		copy(sorted, res)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].RAMBytesPerVec < sorted[j].RAMBytesPerVec
		})
		for _, r := range sorted {
			t.Logf("  RAM=%dB: recall=%.4f (%s)", r.RAMBytesPerVec, r.Recall10, r.Policy)
		}
	}

	t.Logf("\n==========================================================================================")
}

func loadHDF5ThreeStage(path string, maxQueries int) (train, test [][]float32, err error) {
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

func normalizeVectorsThreeStage(vecs [][]float32) {
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

func computeGroundTruthThreeStage(train, test [][]float32, k int) [][]uint64 {
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
