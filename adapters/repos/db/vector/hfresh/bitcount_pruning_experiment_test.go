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

package hfresh

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// =============================================================================
// BITCOUNT BUCKET PRUNING EXPERIMENT
// =============================================================================
//
// This experiment measures how many actual XOR+popcount distance computations
// can be avoided using bitcount-based lower bounds.
//
// Idea: For each 64-bit segment, organize vectors by popcount (0..64).
// For a query with popcount qCount, the lower bound Hamming distance to any
// vector with popcount vCount is |qCount - vCount|.
// If this lower bound already exceeds the threshold, we can skip the XOR+popcount.
//
// =============================================================================

var (
	flagBCDocsPath    = flag.String("bc-docs", "", "Path to documents JSONL file")
	flagBCQueriesPath = flag.String("bc-queries", "", "Path to queries JSONL file")
	flagBCGTPath      = flag.String("bc-gt", "", "Path to ground-truth JSONL file")
	flagBCOutputDir   = flag.String("bc-out", "/tmp/rq1_bitcount_pruning_1m", "Output directory for results")
	flagBCK           = flag.Int("bc-k", 10, "K for recall@k")
	flagBCSeed        = flag.Uint64("bc-seed", 42, "Seed for RQ1 quantizer")
)

// BitcountBucket holds doc IDs grouped by popcount for a segment
type BitcountBucket struct {
	Popcount int
	DocIDs   []int // indices into docCodes
}

// SingleSegmentStats holds statistics for single-segment pruning experiment
type SingleSegmentStats struct {
	SegmentIndex        int     `json:"segment_index"`
	DimensionsUsed      int     `json:"dimensions_used"`
	TotalDocs           int     `json:"total_docs"`
	AvgComputed         float64 `json:"avg_computed"`
	P50Computed         int     `json:"p50_computed"`
	P90Computed         int     `json:"p90_computed"`
	P95Computed         int     `json:"p95_computed"`
	P99Computed         int     `json:"p99_computed"`
	MaxComputed         int     `json:"max_computed"`
	PctFullScan         float64 `json:"pct_full_scan"`
	PctSkipped          float64 `json:"pct_skipped"`
	AvgThreshold        float64 `json:"avg_threshold"`
	P50Threshold        int     `json:"p50_threshold"`
	P95Threshold        int     `json:"p95_threshold"`
	AvgBucketsVisited   float64 `json:"avg_buckets_visited"`
	P50BucketsVisited   int     `json:"p50_buckets_visited"`
	P95BucketsVisited   int     `json:"p95_buckets_visited"`
	EstimatedP50Ms      float64 `json:"estimated_p50_ms"`
	EstimatedP95Ms      float64 `json:"estimated_p95_ms"`
}

// ProgressiveSegmentStats holds statistics for progressive pruning experiment
type ProgressiveSegmentStats struct {
	SegmentIndex           int     `json:"segment_index"`
	DimensionsUsed         int     `json:"dimensions_used"`
	AvgAlive               float64 `json:"avg_alive"`
	P50Alive               int     `json:"p50_alive"`
	P95Alive               int     `json:"p95_alive"`
	MaxAlive               int     `json:"max_alive"`
	AvgSegmentComputed     float64 `json:"avg_segment_computed"`
	P50SegmentComputed     int     `json:"p50_segment_computed"`
	P95SegmentComputed     int     `json:"p95_segment_computed"`
	AvgCumulativeComputed  float64 `json:"avg_cumulative_computed"`
	P50CumulativeComputed  int     `json:"p50_cumulative_computed"`
	P95CumulativeComputed  int     `json:"p95_cumulative_computed"`
	AvgSkippedLowerBound   float64 `json:"avg_skipped_lower_bound"`
	P50SkippedLowerBound   int     `json:"p50_skipped_lower_bound"`
	P95SkippedLowerBound   int     `json:"p95_skipped_lower_bound"`
	PctVsNaive             float64 `json:"pct_vs_naive"`
	EstimatedP50Ms         float64 `json:"estimated_p50_ms"`
	EstimatedP95Ms         float64 `json:"estimated_p95_ms"`
	// From oracle experiment for comparison
	OracleP50RequiredKeep  int     `json:"oracle_p50_required_keep"`
	OracleP95RequiredKeep  int     `json:"oracle_p95_required_keep"`
}

// BitcountPruningReport is the full experiment output
type BitcountPruningReport struct {
	Config struct {
		DocsPath       string  `json:"docs_path"`
		QueriesPath    string  `json:"queries_path"`
		GTPath         string  `json:"gt_path"`
		NumDocs        int     `json:"num_docs"`
		NumQueries     int     `json:"num_queries"`
		VectorDims     int     `json:"vector_dims"`
		NumSegments    int     `json:"num_segments"`
		K              int     `json:"k"`
		Seed           uint64  `json:"seed"`
		DistPerSecond  float64 `json:"distances_per_second"`
		NsPerDistance  float64 `json:"ns_per_distance"`
	} `json:"config"`

	SingleSegmentStats    []SingleSegmentStats      `json:"single_segment_stats"`
	ProgressiveStats      []ProgressiveSegmentStats `json:"progressive_stats"`

	Timestamp             string  `json:"timestamp"`
}

// TestBitcountPruning runs both pruning experiments
func TestBitcountPruning(t *testing.T) {
	if *flagBCDocsPath == "" || *flagBCQueriesPath == "" || *flagBCGTPath == "" {
		t.Skip("Skipping: requires -bc-docs, -bc-queries, -bc-gt flags")
	}

	// Load data
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagBCDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagBCQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	gt, err := loadGroundTruth(*flagBCGTPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(gt))

	// Build doc ID set
	docIDSet := make(map[uint64]int)
	for i, doc := range docs {
		docIDSet[doc.ID] = i
	}

	// Determine dimensions
	dims := 0
	for _, doc := range docs {
		if len(doc.Vectors) > 0 && len(doc.Vectors[0]) > 0 {
			dims = len(doc.Vectors[0])
			break
		}
	}
	require.Greater(t, dims, 0, "could not determine vector dimensions")
	t.Logf("Vector dimensions: %d", dims)

	numSegments := (dims + 63) / 64
	t.Logf("Number of 64-bit segments: %d", numSegments)

	// Create RQ1 quantizer
	t.Log("Creating RQ1 quantizer...")
	quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(dims, *flagBCSeed, distancer.NewL2SquaredProvider())
	require.NoError(t, err)

	// Encode all vectors
	t.Log("Encoding vectors...")
	encodeStart := time.Now()

	docCodes := make([][]uint64, len(docs))
	validDocIndices := make([]int, 0, len(docs))
	for i, doc := range docs {
		if len(doc.Vectors) == 0 || len(doc.Vectors[0]) != dims {
			continue
		}
		docCodes[i] = quantizer.Encode(doc.Vectors[0])
		validDocIndices = append(validDocIndices, i)
	}

	queryCodes := make([][]uint64, len(queries))
	for i, q := range queries {
		if len(q.Vectors) == 0 || len(q.Vectors[0]) != dims {
			continue
		}
		queryCodes[i] = quantizer.Encode(q.Vectors[0])
	}

	t.Logf("Encoding complete in %v", time.Since(encodeStart))
	t.Logf("Valid documents: %d", len(validDocIndices))

	// Run timing benchmark
	t.Log("Running distance benchmark...")
	distPerSec, nsPerDist := runDistanceBenchmark(t, docCodes, validDocIndices)
	t.Logf("Distance throughput: %.2f M distances/sec, %.2f ns/distance", distPerSec/1e6, nsPerDist)

	// Build bitcount buckets for each segment
	t.Log("Building bitcount buckets...")
	buckets := buildBitcountBuckets(docCodes, validDocIndices, numSegments)
	t.Logf("Built buckets for %d segments", len(buckets))

	// Create output directory
	err = os.MkdirAll(*flagBCOutputDir, 0755)
	require.NoError(t, err)

	// Run Experiment A: Single-segment pruning
	t.Log("Running Experiment A: Single-segment pruning...")
	singleStats := runSingleSegmentExperiment(t, queries, queryCodes, docs, docCodes, gt, docIDSet,
		validDocIndices, buckets, *flagBCK, numSegments, distPerSec)

	// Run Experiment B: Progressive pruning
	t.Log("Running Experiment B: Progressive pruning...")
	progressiveStats := runProgressiveExperiment(t, queries, queryCodes, docs, docCodes, gt, docIDSet,
		validDocIndices, buckets, *flagBCK, numSegments, distPerSec)

	// Build report
	report := BitcountPruningReport{
		SingleSegmentStats: singleStats,
		ProgressiveStats:   progressiveStats,
		Timestamp:          time.Now().Format(time.RFC3339),
	}
	report.Config.DocsPath = *flagBCDocsPath
	report.Config.QueriesPath = *flagBCQueriesPath
	report.Config.GTPath = *flagBCGTPath
	report.Config.NumDocs = len(validDocIndices)
	report.Config.NumQueries = len(queries)
	report.Config.VectorDims = dims
	report.Config.NumSegments = numSegments
	report.Config.K = *flagBCK
	report.Config.Seed = *flagBCSeed
	report.Config.DistPerSecond = distPerSec
	report.Config.NsPerDistance = nsPerDist

	// Write outputs
	writeAllOutputs(t, *flagBCOutputDir, report)

	// Print summary
	printBitcountSummary(t, report)
}

// runDistanceBenchmark measures actual XOR+popcount throughput
func runDistanceBenchmark(t *testing.T, docCodes [][]uint64, validIndices []int) (distPerSec, nsPerDist float64) {
	if len(validIndices) < 2 {
		return 1e9, 1.0 // Fallback
	}

	// Use first two valid codes for benchmark
	code1 := docCodes[validIndices[0]]
	code2 := docCodes[validIndices[1]]

	if code1 == nil || code2 == nil || len(code1) < 2 || len(code2) < 2 {
		return 1e9, 1.0
	}

	// Get just the bit segments (skip header at index 0)
	bits1 := code1[1:]
	bits2 := code2[1:]
	numSegs := len(bits1)
	if len(bits2) < numSegs {
		numSegs = len(bits2)
	}

	// Warmup
	for i := 0; i < 10000; i++ {
		dist := 0
		for s := 0; s < numSegs; s++ {
			dist += bits.OnesCount64(bits1[s] ^ bits2[s])
		}
		_ = dist
	}

	// Timed run
	iterations := 1000000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		dist := 0
		for s := 0; s < numSegs; s++ {
			dist += bits.OnesCount64(bits1[s] ^ bits2[s])
		}
		_ = dist
	}
	elapsed := time.Since(start)

	// Each iteration is numSegs segment distances
	totalDistances := float64(iterations * numSegs)
	distPerSec = totalDistances / elapsed.Seconds()
	nsPerDist = float64(elapsed.Nanoseconds()) / totalDistances

	return distPerSec, nsPerDist
}

// buildBitcountBuckets organizes doc indices by popcount for each segment
func buildBitcountBuckets(docCodes [][]uint64, validIndices []int, numSegments int) [][]BitcountBucket {
	buckets := make([][]BitcountBucket, numSegments)

	for seg := 0; seg < numSegments; seg++ {
		// Count docs per popcount (0..64)
		countByPopcount := make([][]int, 65)
		for i := 0; i < 65; i++ {
			countByPopcount[i] = make([]int, 0)
		}

		for _, idx := range validIndices {
			code := docCodes[idx]
			if code == nil || len(code) < seg+2 {
				continue
			}
			// Bits start at index 1
			pc := bits.OnesCount64(code[1+seg])
			countByPopcount[pc] = append(countByPopcount[pc], idx)
		}

		segBuckets := make([]BitcountBucket, 0, 65)
		for pc := 0; pc <= 64; pc++ {
			if len(countByPopcount[pc]) > 0 {
				segBuckets = append(segBuckets, BitcountBucket{
					Popcount: pc,
					DocIDs:   countByPopcount[pc],
				})
			}
		}
		buckets[seg] = segBuckets
	}

	return buckets
}

// runSingleSegmentExperiment runs Experiment A
func runSingleSegmentExperiment(
	t *testing.T,
	queries []QueryInput,
	queryCodes [][]uint64,
	docs []DocInput,
	docCodes [][]uint64,
	gt map[string][]uint64,
	docIDSet map[uint64]int,
	validIndices []int,
	buckets [][]BitcountBucket,
	k int,
	numSegments int,
	distPerSec float64,
) []SingleSegmentStats {

	// Only analyze first 8 segments (or all if fewer)
	maxSeg := 8
	if numSegments < maxSeg {
		maxSeg = numSegments
	}

	stats := make([]SingleSegmentStats, maxSeg)

	for seg := 0; seg < maxSeg; seg++ {
		t.Logf("  Segment %d...", seg)

		var computedValues []int
		var skippedValues []int
		var thresholdValues []int
		var bucketsVisitedValues []int

		segBuckets := buckets[seg]

		for qi, q := range queries {
			gtIDs, ok := gt[q.ID]
			if !ok {
				continue
			}

			qCode := queryCodes[qi]
			if qCode == nil || len(qCode) < seg+2 {
				continue
			}

			// Filter GT to valid docs
			filteredGT := make([]int, 0, k)
			for _, gtID := range gtIDs {
				if idx, inSet := docIDSet[gtID]; inSet {
					if docCodes[idx] != nil {
						filteredGT = append(filteredGT, idx)
						if len(filteredGT) >= k {
							break
						}
					}
				}
			}

			if len(filteredGT) == 0 {
				continue
			}

			// Compute threshold: max segment distance among GT docs
			qBit := qCode[1+seg]
			qPopcount := bits.OnesCount64(qBit)
			threshold := 0
			for _, gtIdx := range filteredGT {
				dCode := docCodes[gtIdx]
				dist := bits.OnesCount64(qBit ^ dCode[1+seg])
				if dist > threshold {
					threshold = dist
				}
			}

			// Visit buckets in order of increasing |bucketPopcount - qPopcount|
			computed := 0
			skipped := 0
			bucketsVisited := 0

			// Build visit order
			type bucketDist struct {
				bucket *BitcountBucket
				lowerBound int
			}
			visitOrder := make([]bucketDist, 0, len(segBuckets))
			for i := range segBuckets {
				lb := abs(segBuckets[i].Popcount - qPopcount)
				visitOrder = append(visitOrder, bucketDist{&segBuckets[i], lb})
			}
			sort.Slice(visitOrder, func(i, j int) bool {
				return visitOrder[i].lowerBound < visitOrder[j].lowerBound
			})

			for _, bd := range visitOrder {
				if bd.lowerBound > threshold {
					// Skip entire bucket
					skipped += len(bd.bucket.DocIDs)
				} else {
					// Visit bucket, compute actual distances
					bucketsVisited++
					computed += len(bd.bucket.DocIDs)
				}
			}

			computedValues = append(computedValues, computed)
			skippedValues = append(skippedValues, skipped)
			thresholdValues = append(thresholdValues, threshold)
			bucketsVisitedValues = append(bucketsVisitedValues, bucketsVisited)
		}

		if len(computedValues) == 0 {
			continue
		}

		totalDocs := len(validIndices)

		sort.Ints(computedValues)
		sort.Ints(skippedValues)
		sort.Ints(thresholdValues)
		sort.Ints(bucketsVisitedValues)

		avgComputed := floatAvg(computedValues)
		avgSkipped := floatAvg(skippedValues)

		stats[seg] = SingleSegmentStats{
			SegmentIndex:       seg,
			DimensionsUsed:     (seg + 1) * 64,
			TotalDocs:          totalDocs,
			AvgComputed:        avgComputed,
			P50Computed:        intPercentile(computedValues, 0.50),
			P90Computed:        intPercentile(computedValues, 0.90),
			P95Computed:        intPercentile(computedValues, 0.95),
			P99Computed:        intPercentile(computedValues, 0.99),
			MaxComputed:        computedValues[len(computedValues)-1],
			PctFullScan:        avgComputed / float64(totalDocs) * 100,
			PctSkipped:         avgSkipped / float64(totalDocs) * 100,
			AvgThreshold:       floatAvg(thresholdValues),
			P50Threshold:       intPercentile(thresholdValues, 0.50),
			P95Threshold:       intPercentile(thresholdValues, 0.95),
			AvgBucketsVisited:  floatAvg(bucketsVisitedValues),
			P50BucketsVisited:  intPercentile(bucketsVisitedValues, 0.50),
			P95BucketsVisited:  intPercentile(bucketsVisitedValues, 0.95),
			EstimatedP50Ms:     float64(intPercentile(computedValues, 0.50)) / distPerSec * 1000,
			EstimatedP95Ms:     float64(intPercentile(computedValues, 0.95)) / distPerSec * 1000,
		}
	}

	return stats
}

// runProgressiveExperiment runs Experiment B
// Optimized: processes all segment points in a single pass per query
func runProgressiveExperiment(
	t *testing.T,
	queries []QueryInput,
	queryCodes [][]uint64,
	docs []DocInput,
	docCodes [][]uint64,
	gt map[string][]uint64,
	docIDSet map[uint64]int,
	validIndices []int,
	buckets [][]BitcountBucket,
	k int,
	numSegments int,
	distPerSec float64,
) []ProgressiveSegmentStats {

	// Segments to evaluate
	segmentsToEval := []int{0, 1, 2, 3, 5, 7, 11, 15, 23}
	// Filter to available segments
	filteredSegs := make([]int, 0)
	for _, s := range segmentsToEval {
		if s < numSegments {
			filteredSegs = append(filteredSegs, s)
		}
	}

	// Pre-allocate result collectors for each segment point
	type segmentResults struct {
		alive       []int
		segComputed []int
		cumComputed []int
		skippedLB   []int
		oracleKeep  []int
	}
	results := make([]segmentResults, len(filteredSegs))
	for i := range results {
		results[i] = segmentResults{
			alive:       make([]int, 0, len(queries)),
			segComputed: make([]int, 0, len(queries)),
			cumComputed: make([]int, 0, len(queries)),
			skippedLB:   make([]int, 0, len(queries)),
			oracleKeep:  make([]int, 0, len(queries)),
		}
	}

	totalDocs := len(validIndices)
	maxSegNeeded := filteredSegs[len(filteredSegs)-1]

	t.Logf("  Processing %d queries for segments up to %d...", len(queries), maxSegNeeded)

	// Build segment point set for quick lookup
	segPointSet := make(map[int]int) // seg -> index in filteredSegs
	for i, s := range filteredSegs {
		segPointSet[s] = i
	}

	for qi, q := range queries {
		if qi > 0 && qi%100 == 0 {
			t.Logf("    Query %d/%d...", qi, len(queries))
		}

		gtIDs, ok := gt[q.ID]
		if !ok {
			continue
		}

		qCode := queryCodes[qi]
		if qCode == nil || len(qCode) < maxSegNeeded+2 {
			continue
		}

		// Filter GT
		filteredGT := make([]int, 0, k)
		for _, gtID := range gtIDs {
			if idx, inSet := docIDSet[gtID]; inSet {
				if docCodes[idx] != nil && len(docCodes[idx]) >= maxSegNeeded+2 {
					filteredGT = append(filteredGT, idx)
					if len(filteredGT) >= k {
						break
					}
				}
			}
		}

		if len(filteredGT) == 0 {
			continue
		}

		// Pre-compute FULL accumulated distances for ALL docs (for oracle comparison)
		// This is done once per query, not per segment point
		fullDist := make([]int, len(docCodes))
		for _, idx := range validIndices {
			dCode := docCodes[idx]
			if dCode == nil || len(dCode) < maxSegNeeded+2 {
				continue
			}
			dist := 0
			for seg := 0; seg <= maxSegNeeded; seg++ {
				dist += bits.OnesCount64(qCode[1+seg] ^ dCode[1+seg])
			}
			fullDist[idx] = dist
		}

		// Track accumulated distances and alive status
		accDist := make([]int, len(docCodes)) // docIdx -> accumulated distance
		alive := make([]bool, len(docCodes))
		for _, idx := range validIndices {
			if docCodes[idx] != nil && len(docCodes[idx]) >= maxSegNeeded+2 {
				accDist[idx] = 0
				alive[idx] = true
			}
		}

		gtSet := make(map[int]bool)
		for _, gtIdx := range filteredGT {
			gtSet[gtIdx] = true
		}

		cumComputed := 0
		totalSkippedLB := 0

		for seg := 0; seg <= maxSegNeeded; seg++ {
			qBit := qCode[1+seg]
			qPopcount := bits.OnesCount64(qBit)

			// FIRST: compute actual distances for GT docs to establish threshold
			for _, gtIdx := range filteredGT {
				if alive[gtIdx] {
					dCode := docCodes[gtIdx]
					dBit := dCode[1+seg]
					dist := bits.OnesCount64(qBit ^ dBit)
					accDist[gtIdx] += dist
				}
			}

			// THEN: compute threshold from updated GT distances
			threshold := 0
			for _, gtIdx := range filteredGT {
				if accDist[gtIdx] > threshold {
					threshold = accDist[gtIdx]
				}
			}

			segComputed := 0
			segSkipped := 0

			// Count GT distances computed
			for _, gtIdx := range filteredGT {
				if alive[gtIdx] {
					segComputed++
				}
			}

			// NOW: process other candidates with lower bound pruning
			for _, idx := range validIndices {
				if !alive[idx] || gtSet[idx] {
					continue
				}

				dCode := docCodes[idx]
				dBit := dCode[1+seg]
				dPopcount := bits.OnesCount64(dBit)

				// Lower bound check
				lowerBoundNewAcc := accDist[idx] + abs(qPopcount-dPopcount)
				if lowerBoundNewAcc > threshold {
					// Can eliminate without computing
					alive[idx] = false
					segSkipped++
				} else {
					// Must compute actual distance
					dist := bits.OnesCount64(qBit ^ dBit)
					accDist[idx] += dist
					segComputed++

					// Check if still alive after actual computation
					if accDist[idx] > threshold {
						alive[idx] = false
					}
				}
			}

			cumComputed += segComputed
			totalSkippedLB += segSkipped

			// If this is a segment point we're tracking, record results
			if si, isPoint := segPointSet[seg]; isPoint {
				aliveCount := 0
				for _, idx := range validIndices {
					if alive[idx] {
						aliveCount++
					}
				}

				// Oracle: count docs with full distance up to this segment <= GT threshold
				// Use precomputed partial distances up to this segment
				partialThreshold := threshold // This is the accumulated threshold up to current seg
				oracleKeep := 0
				for _, idx := range validIndices {
					dCode := docCodes[idx]
					if dCode == nil || len(dCode) < seg+2 {
						continue
					}
					// Compute partial distance up to this segment
					partialDist := 0
					for s := 0; s <= seg; s++ {
						partialDist += bits.OnesCount64(qCode[1+s] ^ dCode[1+s])
					}
					if partialDist <= partialThreshold {
						oracleKeep++
					}
				}

				results[si].alive = append(results[si].alive, aliveCount)
				results[si].segComputed = append(results[si].segComputed, segComputed)
				results[si].cumComputed = append(results[si].cumComputed, cumComputed)
				results[si].skippedLB = append(results[si].skippedLB, totalSkippedLB)
				results[si].oracleKeep = append(results[si].oracleKeep, oracleKeep)
			}
		}
	}

	// Aggregate statistics
	stats := make([]ProgressiveSegmentStats, len(filteredSegs))
	for si, maxSeg := range filteredSegs {
		r := results[si]
		if len(r.alive) == 0 {
			continue
		}

		sort.Ints(r.alive)
		sort.Ints(r.segComputed)
		sort.Ints(r.cumComputed)
		sort.Ints(r.skippedLB)
		sort.Ints(r.oracleKeep)

		naive := float64(totalDocs * (maxSeg + 1))
		avgCum := floatAvg(r.cumComputed)

		stats[si] = ProgressiveSegmentStats{
			SegmentIndex:          maxSeg,
			DimensionsUsed:        (maxSeg + 1) * 64,
			AvgAlive:              floatAvg(r.alive),
			P50Alive:              intPercentile(r.alive, 0.50),
			P95Alive:              intPercentile(r.alive, 0.95),
			MaxAlive:              r.alive[len(r.alive)-1],
			AvgSegmentComputed:    floatAvg(r.segComputed),
			P50SegmentComputed:    intPercentile(r.segComputed, 0.50),
			P95SegmentComputed:    intPercentile(r.segComputed, 0.95),
			AvgCumulativeComputed: avgCum,
			P50CumulativeComputed: intPercentile(r.cumComputed, 0.50),
			P95CumulativeComputed: intPercentile(r.cumComputed, 0.95),
			AvgSkippedLowerBound:  floatAvg(r.skippedLB),
			P50SkippedLowerBound:  intPercentile(r.skippedLB, 0.50),
			P95SkippedLowerBound:  intPercentile(r.skippedLB, 0.95),
			PctVsNaive:            avgCum / naive * 100,
			EstimatedP50Ms:        float64(intPercentile(r.cumComputed, 0.50)) / distPerSec * 1000,
			EstimatedP95Ms:        float64(intPercentile(r.cumComputed, 0.95)) / distPerSec * 1000,
			OracleP50RequiredKeep: intPercentile(r.oracleKeep, 0.50),
			OracleP95RequiredKeep: intPercentile(r.oracleKeep, 0.95),
		}
	}

	return stats
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func floatAvg(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0
	for _, v := range values {
		sum += v
	}
	return float64(sum) / float64(len(values))
}

func intPercentile(sortedValues []int, p float64) int {
	if len(sortedValues) == 0 {
		return 0
	}
	idx := int(math.Ceil(p * float64(len(sortedValues)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sortedValues) {
		idx = len(sortedValues) - 1
	}
	return sortedValues[idx]
}

func writeAllOutputs(t *testing.T, outDir string, report BitcountPruningReport) {
	// Write single segment CSV
	csvPath := filepath.Join(outDir, "bitcount_single_segment.csv")
	writeSingleSegmentCSV(t, csvPath, report.SingleSegmentStats)

	// Write progressive CSV
	csvPath = filepath.Join(outDir, "bitcount_progressive.csv")
	writeProgressiveCSV(t, csvPath, report.ProgressiveStats)

	// Write JSON
	jsonPath := filepath.Join(outDir, "bitcount_pruning.json")
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

	// Write Markdown
	mdPath := filepath.Join(outDir, "bitcount_pruning_report.md")
	writeBitcountMarkdown(t, mdPath, report)
}

func writeSingleSegmentCSV(t *testing.T, path string, stats []SingleSegmentStats) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"segment_index", "dimensions_used", "total_docs",
		"avg_computed", "p50_computed", "p90_computed", "p95_computed", "p99_computed", "max_computed",
		"pct_full_scan", "pct_skipped",
		"avg_threshold", "p50_threshold", "p95_threshold",
		"avg_buckets_visited", "p50_buckets_visited", "p95_buckets_visited",
		"estimated_p50_ms", "estimated_p95_ms",
	}
	w.Write(header)

	for _, s := range stats {
		row := []string{
			strconv.Itoa(s.SegmentIndex),
			strconv.Itoa(s.DimensionsUsed),
			strconv.Itoa(s.TotalDocs),
			fmt.Sprintf("%.1f", s.AvgComputed),
			strconv.Itoa(s.P50Computed),
			strconv.Itoa(s.P90Computed),
			strconv.Itoa(s.P95Computed),
			strconv.Itoa(s.P99Computed),
			strconv.Itoa(s.MaxComputed),
			fmt.Sprintf("%.2f", s.PctFullScan),
			fmt.Sprintf("%.2f", s.PctSkipped),
			fmt.Sprintf("%.1f", s.AvgThreshold),
			strconv.Itoa(s.P50Threshold),
			strconv.Itoa(s.P95Threshold),
			fmt.Sprintf("%.1f", s.AvgBucketsVisited),
			strconv.Itoa(s.P50BucketsVisited),
			strconv.Itoa(s.P95BucketsVisited),
			fmt.Sprintf("%.4f", s.EstimatedP50Ms),
			fmt.Sprintf("%.4f", s.EstimatedP95Ms),
		}
		w.Write(row)
	}

	t.Logf("Single-segment CSV written to: %s", path)
}

func writeProgressiveCSV(t *testing.T, path string, stats []ProgressiveSegmentStats) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{
		"segment_index", "dimensions_used",
		"avg_alive", "p50_alive", "p95_alive", "max_alive",
		"avg_segment_computed", "p50_segment_computed", "p95_segment_computed",
		"avg_cumulative_computed", "p50_cumulative_computed", "p95_cumulative_computed",
		"avg_skipped_lb", "p50_skipped_lb", "p95_skipped_lb",
		"pct_vs_naive", "estimated_p50_ms", "estimated_p95_ms",
		"oracle_p50_keep", "oracle_p95_keep",
	}
	w.Write(header)

	for _, s := range stats {
		row := []string{
			strconv.Itoa(s.SegmentIndex),
			strconv.Itoa(s.DimensionsUsed),
			fmt.Sprintf("%.1f", s.AvgAlive),
			strconv.Itoa(s.P50Alive),
			strconv.Itoa(s.P95Alive),
			strconv.Itoa(s.MaxAlive),
			fmt.Sprintf("%.1f", s.AvgSegmentComputed),
			strconv.Itoa(s.P50SegmentComputed),
			strconv.Itoa(s.P95SegmentComputed),
			fmt.Sprintf("%.1f", s.AvgCumulativeComputed),
			strconv.Itoa(s.P50CumulativeComputed),
			strconv.Itoa(s.P95CumulativeComputed),
			fmt.Sprintf("%.1f", s.AvgSkippedLowerBound),
			strconv.Itoa(s.P50SkippedLowerBound),
			strconv.Itoa(s.P95SkippedLowerBound),
			fmt.Sprintf("%.2f", s.PctVsNaive),
			fmt.Sprintf("%.4f", s.EstimatedP50Ms),
			fmt.Sprintf("%.4f", s.EstimatedP95Ms),
			strconv.Itoa(s.OracleP50RequiredKeep),
			strconv.Itoa(s.OracleP95RequiredKeep),
		}
		w.Write(row)
	}

	t.Logf("Progressive CSV written to: %s", path)
}

func writeBitcountMarkdown(t *testing.T, path string, report BitcountPruningReport) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "# Bitcount Bucket Pruning Experiment\n\n")
	fmt.Fprintf(w, "**Date**: %s\n\n", report.Timestamp)

	fmt.Fprintf(w, "## Configuration\n\n")
	fmt.Fprintf(w, "| Parameter | Value |\n")
	fmt.Fprintf(w, "|-----------|-------|\n")
	fmt.Fprintf(w, "| Documents | %d |\n", report.Config.NumDocs)
	fmt.Fprintf(w, "| Queries | %d |\n", report.Config.NumQueries)
	fmt.Fprintf(w, "| Dimensions | %d |\n", report.Config.VectorDims)
	fmt.Fprintf(w, "| Segments | %d |\n", report.Config.NumSegments)
	fmt.Fprintf(w, "| K | %d |\n", report.Config.K)
	fmt.Fprintf(w, "| Distance Throughput | %.2f M/sec |\n", report.Config.DistPerSecond/1e6)
	fmt.Fprintf(w, "| Ns per Distance | %.2f ns |\n\n", report.Config.NsPerDistance)

	fmt.Fprintf(w, "## Experiment A: Single-Segment Bucket Pruning (Segments 0-7)\n\n")
	fmt.Fprintf(w, "| Seg | Dims | Avg Computed | P50 | P95 | Max | %%Full | %%Skip | P95 Thresh | P95 Buckets | Est P50 ms | Est P95 ms |\n")
	fmt.Fprintf(w, "|-----|------|--------------|-----|-----|-----|-------|-------|------------|-------------|------------|------------|\n")

	for _, s := range report.SingleSegmentStats {
		fmt.Fprintf(w, "| %d | %d | %.0f | %d | %d | %d | %.1f | %.1f | %d | %d | %.3f | %.3f |\n",
			s.SegmentIndex, s.DimensionsUsed, s.AvgComputed, s.P50Computed, s.P95Computed, s.MaxComputed,
			s.PctFullScan, s.PctSkipped, s.P95Threshold, s.P95BucketsVisited,
			s.EstimatedP50Ms, s.EstimatedP95Ms)
	}

	fmt.Fprintf(w, "\n## Experiment B: Progressive Accumulated Pruning\n\n")
	fmt.Fprintf(w, "| Dims | Seg | P50 Alive | P95 Alive | P50 Cum Dist | P95 Cum Dist | %%Naive | Est P50 ms | Est P95 ms | Oracle P50 | Oracle P95 |\n")
	fmt.Fprintf(w, "|------|-----|-----------|-----------|--------------|--------------|--------|------------|------------|------------|------------|\n")

	for _, s := range report.ProgressiveStats {
		fmt.Fprintf(w, "| %d | %d | %d | %d | %d | %d | %.1f | %.3f | %.3f | %d | %d |\n",
			s.DimensionsUsed, s.SegmentIndex, s.P50Alive, s.P95Alive,
			s.P50CumulativeComputed, s.P95CumulativeComputed, s.PctVsNaive,
			s.EstimatedP50Ms, s.EstimatedP95Ms,
			s.OracleP50RequiredKeep, s.OracleP95RequiredKeep)
	}

	fmt.Fprintf(w, "\n## Key Findings\n\n")

	// Find best segment for single-segment
	if len(report.SingleSegmentStats) > 0 {
		best := report.SingleSegmentStats[0]
		for _, s := range report.SingleSegmentStats {
			if s.PctSkipped > best.PctSkipped {
				best = s
			}
		}
		fmt.Fprintf(w, "- **Best single-segment pruning**: Segment %d (%d dims) skips %.1f%% of docs\n",
			best.SegmentIndex, best.DimensionsUsed, best.PctSkipped)
	}

	// Find where progressive hits 50ms target
	fmt.Fprintf(w, "\n### Latency Estimates\n\n")
	for _, s := range report.ProgressiveStats {
		if s.EstimatedP95Ms > 0 {
			latencyClass := "30ms-class"
			if s.EstimatedP95Ms > 100 {
				latencyClass = "300ms+"
			} else if s.EstimatedP95Ms > 30 {
				latencyClass = "100ms-class"
			}
			fmt.Fprintf(w, "- At %d dims: P95 estimated %.1f ms (%s)\n",
				s.DimensionsUsed, s.EstimatedP95Ms, latencyClass)
		}
	}

	t.Logf("Markdown report written to: %s", path)
}

func printBitcountSummary(t *testing.T, report BitcountPruningReport) {
	t.Log("\n" + strings.Repeat("=", 80))
	t.Log("BITCOUNT BUCKET PRUNING SUMMARY")
	t.Log(strings.Repeat("=", 80))

	t.Logf("\nDistance throughput: %.2f M/sec (%.2f ns/dist)",
		report.Config.DistPerSecond/1e6, report.Config.NsPerDistance)

	t.Log("\n--- Single-Segment Pruning (Experiment A) ---")
	t.Log("Seg | Dims | P50 Computed | P95 Computed | %Skip | Est P95 ms")
	t.Log("----|------|--------------|--------------|-------|----------")
	for _, s := range report.SingleSegmentStats {
		t.Logf("%3d | %4d | %12d | %12d | %5.1f | %.3f",
			s.SegmentIndex, s.DimensionsUsed, s.P50Computed, s.P95Computed,
			s.PctSkipped, s.EstimatedP95Ms)
	}

	t.Log("\n--- Progressive Pruning (Experiment B) ---")
	t.Log("Dims | P50 Cum Dist | P95 Cum Dist | %Naive | Est P95 ms | Oracle P95")
	t.Log("-----|--------------|--------------|--------|------------|----------")
	for _, s := range report.ProgressiveStats {
		t.Logf("%4d | %12d | %12d | %6.1f | %10.3f | %10d",
			s.DimensionsUsed, s.P50CumulativeComputed, s.P95CumulativeComputed,
			s.PctVsNaive, s.EstimatedP95Ms, s.OracleP95RequiredKeep)
	}

	t.Log(strings.Repeat("=", 80))
}
