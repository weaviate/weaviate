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
// NORM-BOUND PRUNING EXPERIMENT
// =============================================================================
//
// Tests whether adding per-segment norm information can improve pruning
// beyond the bitcount/Hamming lower bound alone.
//
// For each 64-dim segment, we store:
// - RQ1 sign bits (64 bits)
// - Block L2 norm (float32)
//
// Lower-bound variants tested:
// - Hamming-only: abs(popcount(qBits) - popcount(dBits))
// - Norm-only: scaled |qNorm - dNorm|
// - Hamming + alpha*Norm: combined with alpha grid
//
// =============================================================================

var (
	flagNBDocsPath    = flag.String("nb-docs", "", "Path to documents JSONL file")
	flagNBQueriesPath = flag.String("nb-queries", "", "Path to queries JSONL file")
	flagNBGTPath      = flag.String("nb-gt", "", "Path to ground-truth JSONL file")
	flagNBOutputDir   = flag.String("nb-out", "/tmp/rq1_norm_bound_1m", "Output directory")
	flagNBK           = flag.Int("nb-k", 10, "K for recall@k")
	flagNBSeed        = flag.Uint64("nb-seed", 42, "Seed for RQ1 quantizer")
)

// Alpha values to test for combined Hamming+Norm lower bound
var alphaValues = []float64{0.25, 0.5, 1.0, 2.0, 4.0}

// SegmentFeatures holds per-segment features for a vector
type SegmentFeatures struct {
	Bits []uint64  // RQ1 sign bits, one per segment
	Norm []float32 // L2 norm per segment
}

// LowerBoundMethod identifies the lower-bound variant
type LowerBoundMethod struct {
	Name  string
	Alpha float64 // Only used for combined methods
}

// SingleSegmentResult holds results for one method on one segment
type SingleSegmentResult struct {
	Method         string  `json:"method"`
	Alpha          float64 `json:"alpha"`
	SegmentIndex   int     `json:"segment_index"`
	Dims           int     `json:"dims"`
	AvgComputed    float64 `json:"avg_computed"`
	P50Computed    int     `json:"p50_computed"`
	P95Computed    int     `json:"p95_computed"`
	AvgSkipped     float64 `json:"avg_skipped"`
	P50Skipped     int     `json:"p50_skipped"`
	P95Skipped     int     `json:"p95_skipped"`
	SkipPct        float64 `json:"skip_pct"`
	GTFalsePrune   int     `json:"gt_false_prune"` // Should be 0
}

// ProgressiveResult holds results for one method at one segment point
type ProgressiveResult struct {
	Method            string  `json:"method"`
	Alpha             float64 `json:"alpha"`
	SegmentIndex      int     `json:"segment_index"`
	Dims              int     `json:"dims"`
	AvgAlive          float64 `json:"avg_alive"`
	P50Alive          int     `json:"p50_alive"`
	P95Alive          int     `json:"p95_alive"`
	AvgCumComputed    float64 `json:"avg_cum_computed"`
	P50CumComputed    int     `json:"p50_cum_computed"`
	P95CumComputed    int     `json:"p95_cum_computed"`
	AvgSkippedLB      float64 `json:"avg_skipped_lb"`
	P50SkippedLB      int     `json:"p50_skipped_lb"`
	P95SkippedLB      int     `json:"p95_skipped_lb"`
	PctVsNaive        float64 `json:"pct_vs_naive"`
	EstP50Ms          float64 `json:"est_p50_ms"`
	EstP95Ms          float64 `json:"est_p95_ms"`
	ImprovementVsHamm float64 `json:"improvement_vs_hamming"` // % reduction vs Hamming-only
}

// CalibrationResult holds correlation data for one segment
type CalibrationResult struct {
	SegmentIndex     int     `json:"segment_index"`
	CorrHammingExact float64 `json:"corr_hamming_exact"`
	CorrNormExact    float64 `json:"corr_norm_exact"`
	CorrCombinedExact float64 `json:"corr_combined_exact"` // Best alpha
	BestAlpha        float64 `json:"best_alpha"`
}

// NormBoundReport is the full experiment output
type NormBoundReport struct {
	Config struct {
		NumDocs       int     `json:"num_docs"`
		NumQueries    int     `json:"num_queries"`
		VectorDims    int     `json:"vector_dims"`
		NumSegments   int     `json:"num_segments"`
		K             int     `json:"k"`
		DistPerSecond float64 `json:"distances_per_second"`
		AlphaValues   []float64 `json:"alpha_values"`
	} `json:"config"`

	Calibration       []CalibrationResult   `json:"calibration"`
	SingleSegment     []SingleSegmentResult `json:"single_segment"`
	Progressive       []ProgressiveResult   `json:"progressive"`

	Timestamp string `json:"timestamp"`
}

// TestNormBoundPruning runs the norm-bound experiment
func TestNormBoundPruning(t *testing.T) {
	if *flagNBDocsPath == "" || *flagNBQueriesPath == "" || *flagNBGTPath == "" {
		t.Skip("Skipping: requires -nb-docs, -nb-queries, -nb-gt flags")
	}

	// Load data
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagNBDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagNBQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	gt, err := loadGroundTruth(*flagNBGTPath)
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
	require.Greater(t, dims, 0)
	t.Logf("Vector dimensions: %d", dims)

	numSegments := (dims + 63) / 64
	t.Logf("Number of 64-bit segments: %d", numSegments)

	// Create RQ1 quantizer
	t.Log("Creating RQ1 quantizer...")
	quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(dims, *flagNBSeed, distancer.NewL2SquaredProvider())
	require.NoError(t, err)

	// Compute segment features for all docs
	t.Log("Computing segment features for documents...")
	docFeatures := make([]SegmentFeatures, len(docs))
	validDocIndices := make([]int, 0, len(docs))

	for i, doc := range docs {
		if len(doc.Vectors) == 0 || len(doc.Vectors[0]) != dims {
			continue
		}
		vec := doc.Vectors[0]
		code := quantizer.Encode(vec)

		// Extract bits (skip header at index 0)
		docFeatures[i].Bits = code[1:]

		// Compute per-segment norms
		docFeatures[i].Norm = computeSegmentNorms(vec, numSegments)
		validDocIndices = append(validDocIndices, i)
	}
	t.Logf("Valid documents: %d", len(validDocIndices))

	// Compute segment features for queries
	t.Log("Computing segment features for queries...")
	queryFeatures := make([]SegmentFeatures, len(queries))
	for i, q := range queries {
		if len(q.Vectors) == 0 || len(q.Vectors[0]) != dims {
			continue
		}
		vec := q.Vectors[0]
		code := quantizer.Encode(vec)
		queryFeatures[i].Bits = code[1:]
		queryFeatures[i].Norm = computeSegmentNorms(vec, numSegments)
	}

	// Run distance benchmark
	t.Log("Running distance benchmark...")
	distPerSec := runNormBenchmark(t, docFeatures, validDocIndices)
	t.Logf("Distance throughput: %.2f M/sec", distPerSec/1e6)

	// Compute norm scaling factor (calibration)
	t.Log("Running calibration...")
	calibration, normScale := runCalibration(t, queries, queryFeatures, docs, docFeatures,
		gt, docIDSet, validDocIndices, numSegments, *flagNBK)

	// Create output directory
	err = os.MkdirAll(*flagNBOutputDir, 0755)
	require.NoError(t, err)

	// Run single-segment experiment
	t.Log("Running single-segment experiment...")
	singleResults := runSingleSegmentNormExperiment(t, queries, queryFeatures, docs, docFeatures,
		gt, docIDSet, validDocIndices, numSegments, *flagNBK, normScale)

	// Run progressive experiment
	t.Log("Running progressive experiment...")
	progressiveResults := runProgressiveNormExperiment(t, queries, queryFeatures, docs, docFeatures,
		gt, docIDSet, validDocIndices, numSegments, *flagNBK, normScale, distPerSec)

	// Build report
	report := NormBoundReport{
		Calibration:   calibration,
		SingleSegment: singleResults,
		Progressive:   progressiveResults,
		Timestamp:     time.Now().Format(time.RFC3339),
	}
	report.Config.NumDocs = len(validDocIndices)
	report.Config.NumQueries = len(queries)
	report.Config.VectorDims = dims
	report.Config.NumSegments = numSegments
	report.Config.K = *flagNBK
	report.Config.DistPerSecond = distPerSec
	report.Config.AlphaValues = alphaValues

	// Write outputs
	writeNormBoundOutputs(t, *flagNBOutputDir, report)

	// Print summary
	printNormBoundSummary(t, report)
}

// computeSegmentNorms computes L2 norm for each 64-dim segment
func computeSegmentNorms(vec []float32, numSegments int) []float32 {
	norms := make([]float32, numSegments)
	for seg := 0; seg < numSegments; seg++ {
		start := seg * 64
		end := start + 64
		if end > len(vec) {
			end = len(vec)
		}

		var sumSq float64
		for i := start; i < end; i++ {
			sumSq += float64(vec[i]) * float64(vec[i])
		}
		norms[seg] = float32(math.Sqrt(sumSq))
	}
	return norms
}

// runNormBenchmark measures XOR+popcount throughput
func runNormBenchmark(t *testing.T, docFeatures []SegmentFeatures, validIndices []int) float64 {
	if len(validIndices) < 2 {
		return 1e9
	}

	f1 := docFeatures[validIndices[0]]
	f2 := docFeatures[validIndices[1]]
	if len(f1.Bits) == 0 || len(f2.Bits) == 0 {
		return 1e9
	}

	numSegs := len(f1.Bits)

	// Warmup
	for i := 0; i < 10000; i++ {
		dist := 0
		for s := 0; s < numSegs; s++ {
			dist += bits.OnesCount64(f1.Bits[s] ^ f2.Bits[s])
		}
		_ = dist
	}

	// Timed run
	iterations := 1000000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		dist := 0
		for s := 0; s < numSegs; s++ {
			dist += bits.OnesCount64(f1.Bits[s] ^ f2.Bits[s])
		}
		_ = dist
	}
	elapsed := time.Since(start)

	totalDistances := float64(iterations * numSegs)
	return totalDistances / elapsed.Seconds()
}

// runCalibration estimates correlation between lower bounds and exact distances
func runCalibration(
	t *testing.T,
	queries []QueryInput,
	queryFeatures []SegmentFeatures,
	docs []DocInput,
	docFeatures []SegmentFeatures,
	gt map[string][]uint64,
	docIDSet map[uint64]int,
	validIndices []int,
	numSegments int,
	k int,
) ([]CalibrationResult, float64) {

	// Sample a subset for calibration (first 50 queries, first 10000 docs)
	maxQueries := 50
	if len(queries) < maxQueries {
		maxQueries = len(queries)
	}
	maxDocs := 10000
	if len(validIndices) < maxDocs {
		maxDocs = len(validIndices)
	}
	sampleIndices := validIndices[:maxDocs]

	results := make([]CalibrationResult, min(numSegments, 8))

	// Estimate overall norm scale: we want norm difference to be comparable to Hamming
	// Hamming distance range: 0-64 per segment
	// We'll scale norm difference so that typical range is also ~0-64
	var totalNormDiff float64
	var normCount int

	for seg := 0; seg < min(numSegments, 8); seg++ {
		var hammingVals, normVals, exactVals []float64

		for qi := 0; qi < maxQueries; qi++ {
			q := queries[qi]
			qf := queryFeatures[qi]
			if len(qf.Bits) <= seg {
				continue
			}

			_, ok := gt[q.ID]
			if !ok {
				continue
			}

			qBit := qf.Bits[seg]
			qNorm := qf.Norm[seg]
			qPop := bits.OnesCount64(qBit)

			for _, idx := range sampleIndices {
				df := docFeatures[idx]
				if len(df.Bits) <= seg {
					continue
				}

				dBit := df.Bits[seg]
				dNorm := df.Norm[seg]
				dPop := bits.OnesCount64(dBit)

				// Exact Hamming distance for this segment
				exactHamming := float64(bits.OnesCount64(qBit ^ dBit))

				// Hamming lower bound
				hammingLB := float64(abs(qPop - dPop))

				// Norm difference
				normDiff := math.Abs(float64(qNorm - dNorm))

				totalNormDiff += normDiff
				normCount++

				hammingVals = append(hammingVals, hammingLB)
				normVals = append(normVals, normDiff)
				exactVals = append(exactVals, exactHamming)
			}
		}

		if len(exactVals) > 0 {
			results[seg].SegmentIndex = seg
			results[seg].CorrHammingExact = correlation(hammingVals, exactVals)
			results[seg].CorrNormExact = correlation(normVals, exactVals)

			// Find best alpha
			bestCorr := results[seg].CorrHammingExact
			bestAlpha := 0.0
			for _, alpha := range alphaValues {
				combined := make([]float64, len(hammingVals))
				for i := range combined {
					combined[i] = hammingVals[i] + alpha*normVals[i]
				}
				corr := correlation(combined, exactVals)
				if corr > bestCorr {
					bestCorr = corr
					bestAlpha = alpha
				}
			}
			results[seg].CorrCombinedExact = bestCorr
			results[seg].BestAlpha = bestAlpha
		}
	}

	// Compute norm scale: scale so average norm diff maps to ~10 (in Hamming units)
	avgNormDiff := totalNormDiff / float64(normCount)
	normScale := 10.0 / avgNormDiff
	t.Logf("Calibration: avg norm diff = %.4f, norm scale = %.4f", avgNormDiff, normScale)

	return results, normScale
}

// correlation computes Pearson correlation coefficient
func correlation(x, y []float64) float64 {
	if len(x) != len(y) || len(x) == 0 {
		return 0
	}

	n := float64(len(x))
	var sumX, sumY, sumXY, sumX2, sumY2 float64
	for i := range x {
		sumX += x[i]
		sumY += y[i]
		sumXY += x[i] * y[i]
		sumX2 += x[i] * x[i]
		sumY2 += y[i] * y[i]
	}

	num := n*sumXY - sumX*sumY
	den := math.Sqrt((n*sumX2 - sumX*sumX) * (n*sumY2 - sumY*sumY))
	if den == 0 {
		return 0
	}
	return num / den
}

// runSingleSegmentNormExperiment runs single-segment pruning with different methods
func runSingleSegmentNormExperiment(
	t *testing.T,
	queries []QueryInput,
	queryFeatures []SegmentFeatures,
	docs []DocInput,
	docFeatures []SegmentFeatures,
	gt map[string][]uint64,
	docIDSet map[uint64]int,
	validIndices []int,
	numSegments int,
	k int,
	normScale float64,
) []SingleSegmentResult {

	methods := []LowerBoundMethod{
		{Name: "hamming", Alpha: 0},
		{Name: "norm", Alpha: 0},
	}
	for _, alpha := range alphaValues {
		methods = append(methods, LowerBoundMethod{Name: "combined", Alpha: alpha})
	}

	maxSeg := min(numSegments, 8)
	results := make([]SingleSegmentResult, 0, len(methods)*maxSeg)

	for seg := 0; seg < maxSeg; seg++ {
		t.Logf("  Segment %d...", seg)

		for _, method := range methods {
			var computedVals, skippedVals []int
			gtFalsePrune := 0

			for qi, q := range queries {
				gtIDs, ok := gt[q.ID]
				if !ok {
					continue
				}

				qf := queryFeatures[qi]
				if len(qf.Bits) <= seg {
					continue
				}

				// Filter GT
				filteredGT := make([]int, 0, k)
				for _, gtID := range gtIDs {
					if idx, inSet := docIDSet[gtID]; inSet {
						if len(docFeatures[idx].Bits) > seg {
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

				qBit := qf.Bits[seg]
				qNorm := qf.Norm[seg]
				qPop := bits.OnesCount64(qBit)

				// Compute threshold from GT exact distances
				threshold := 0
				for _, gtIdx := range filteredGT {
					df := docFeatures[gtIdx]
					dist := bits.OnesCount64(qBit ^ df.Bits[seg])
					if dist > threshold {
						threshold = dist
					}
				}

				gtSet := make(map[int]bool)
				for _, idx := range filteredGT {
					gtSet[idx] = true
				}

				computed := 0
				skipped := 0

				for _, idx := range validIndices {
					df := docFeatures[idx]
					if len(df.Bits) <= seg {
						continue
					}

					dBit := df.Bits[seg]
					dNorm := df.Norm[seg]
					dPop := bits.OnesCount64(dBit)

					// Compute lower bound based on method
					var lb float64
					switch method.Name {
					case "hamming":
						lb = float64(abs(qPop - dPop))
					case "norm":
						lb = math.Abs(float64(qNorm-dNorm)) * normScale
					case "combined":
						hammingLB := float64(abs(qPop - dPop))
						normLB := math.Abs(float64(qNorm-dNorm)) * normScale
						lb = hammingLB + method.Alpha*normLB
					}

					if lb > float64(threshold) {
						skipped++
						// Check if GT was incorrectly pruned
						if gtSet[idx] {
							gtFalsePrune++
						}
					} else {
						computed++
					}
				}

				computedVals = append(computedVals, computed)
				skippedVals = append(skippedVals, skipped)
			}

			if len(computedVals) == 0 {
				continue
			}

			sort.Ints(computedVals)
			sort.Ints(skippedVals)

			totalDocs := len(validIndices)
			avgSkipped := floatAvg(skippedVals)

			results = append(results, SingleSegmentResult{
				Method:       method.Name,
				Alpha:        method.Alpha,
				SegmentIndex: seg,
				Dims:         (seg + 1) * 64,
				AvgComputed:  floatAvg(computedVals),
				P50Computed:  intPercentile(computedVals, 0.50),
				P95Computed:  intPercentile(computedVals, 0.95),
				AvgSkipped:   avgSkipped,
				P50Skipped:   intPercentile(skippedVals, 0.50),
				P95Skipped:   intPercentile(skippedVals, 0.95),
				SkipPct:      avgSkipped / float64(totalDocs) * 100,
				GTFalsePrune: gtFalsePrune,
			})
		}
	}

	return results
}

// runProgressiveNormExperiment runs progressive pruning with different methods
func runProgressiveNormExperiment(
	t *testing.T,
	queries []QueryInput,
	queryFeatures []SegmentFeatures,
	docs []DocInput,
	docFeatures []SegmentFeatures,
	gt map[string][]uint64,
	docIDSet map[uint64]int,
	validIndices []int,
	numSegments int,
	k int,
	normScale float64,
	distPerSec float64,
) []ProgressiveResult {

	methods := []LowerBoundMethod{
		{Name: "hamming", Alpha: 0},
		{Name: "norm", Alpha: 0},
	}
	for _, alpha := range alphaValues {
		methods = append(methods, LowerBoundMethod{Name: "combined", Alpha: alpha})
	}

	segmentsToEval := []int{0, 1, 3, 7, 11, 15, 23}
	filteredSegs := make([]int, 0)
	for _, s := range segmentsToEval {
		if s < numSegments {
			filteredSegs = append(filteredSegs, s)
		}
	}

	totalDocs := len(validIndices)
	results := make([]ProgressiveResult, 0, len(methods)*len(filteredSegs))

	// Store Hamming-only results for comparison
	hammingCumP50 := make(map[int]int) // seg -> P50 cumulative

	for _, method := range methods {
		t.Logf("  Method: %s (alpha=%.2f)...", method.Name, method.Alpha)

		// Pre-allocate per-segment collectors
		type segCollector struct {
			alive      []int
			cumComputed []int
			skippedLB  []int
		}
		collectors := make([]segCollector, len(filteredSegs))
		for i := range collectors {
			collectors[i] = segCollector{
				alive:       make([]int, 0, len(queries)),
				cumComputed: make([]int, 0, len(queries)),
				skippedLB:   make([]int, 0, len(queries)),
			}
		}

		segPointSet := make(map[int]int)
		for i, s := range filteredSegs {
			segPointSet[s] = i
		}

		maxSegNeeded := filteredSegs[len(filteredSegs)-1]

		for qi, q := range queries {
			if qi > 0 && qi%100 == 0 {
				t.Logf("    Query %d/%d...", qi, len(queries))
			}

			gtIDs, ok := gt[q.ID]
			if !ok {
				continue
			}

			qf := queryFeatures[qi]
			if len(qf.Bits) <= maxSegNeeded {
				continue
			}

			// Filter GT
			filteredGT := make([]int, 0, k)
			for _, gtID := range gtIDs {
				if idx, inSet := docIDSet[gtID]; inSet {
					if len(docFeatures[idx].Bits) > maxSegNeeded {
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

			// Track state
			accDist := make([]int, len(docFeatures))
			alive := make([]bool, len(docFeatures))
			for _, idx := range validIndices {
				if len(docFeatures[idx].Bits) > maxSegNeeded {
					alive[idx] = true
				}
			}

			gtSet := make(map[int]bool)
			for _, idx := range filteredGT {
				gtSet[idx] = true
			}

			cumComputed := 0
			totalSkipped := 0

			for seg := 0; seg <= maxSegNeeded; seg++ {
				qBit := qf.Bits[seg]
				qNorm := qf.Norm[seg]
				qPop := bits.OnesCount64(qBit)

				// First compute GT distances
				for _, gtIdx := range filteredGT {
					if alive[gtIdx] {
						df := docFeatures[gtIdx]
						dist := bits.OnesCount64(qBit ^ df.Bits[seg])
						accDist[gtIdx] += dist
					}
				}

				// Compute threshold
				threshold := 0
				for _, gtIdx := range filteredGT {
					if accDist[gtIdx] > threshold {
						threshold = accDist[gtIdx]
					}
				}

				segComputed := 0
				segSkipped := 0

				// Count GT computed
				for _, gtIdx := range filteredGT {
					if alive[gtIdx] {
						segComputed++
					}
				}

				// Process other candidates
				for _, idx := range validIndices {
					if !alive[idx] || gtSet[idx] {
						continue
					}

					df := docFeatures[idx]
					dBit := df.Bits[seg]
					dNorm := df.Norm[seg]
					dPop := bits.OnesCount64(dBit)

					// Compute lower bound
					var lb float64
					switch method.Name {
					case "hamming":
						lb = float64(accDist[idx] + abs(qPop-dPop))
					case "norm":
						lb = float64(accDist[idx]) + math.Abs(float64(qNorm-dNorm))*normScale
					case "combined":
						hammingLB := float64(abs(qPop - dPop))
						normLB := math.Abs(float64(qNorm-dNorm)) * normScale
						lb = float64(accDist[idx]) + hammingLB + method.Alpha*normLB
					}

					if lb > float64(threshold) {
						alive[idx] = false
						segSkipped++
					} else {
						// Compute actual distance
						dist := bits.OnesCount64(qBit ^ dBit)
						accDist[idx] += dist
						segComputed++

						if accDist[idx] > threshold {
							alive[idx] = false
						}
					}
				}

				cumComputed += segComputed
				totalSkipped += segSkipped

				// Record at segment points
				if si, isPoint := segPointSet[seg]; isPoint {
					aliveCount := 0
					for _, idx := range validIndices {
						if alive[idx] {
							aliveCount++
						}
					}
					collectors[si].alive = append(collectors[si].alive, aliveCount)
					collectors[si].cumComputed = append(collectors[si].cumComputed, cumComputed)
					collectors[si].skippedLB = append(collectors[si].skippedLB, totalSkipped)
				}
			}
		}

		// Aggregate results
		for si, maxSeg := range filteredSegs {
			c := collectors[si]
			if len(c.alive) == 0 {
				continue
			}

			sort.Ints(c.alive)
			sort.Ints(c.cumComputed)
			sort.Ints(c.skippedLB)

			naive := float64(totalDocs * (maxSeg + 1))
			p50Cum := intPercentile(c.cumComputed, 0.50)
			p95Cum := intPercentile(c.cumComputed, 0.95)

			// Compute improvement vs Hamming
			improvement := 0.0
			if method.Name == "hamming" {
				hammingCumP50[maxSeg] = p50Cum
			} else if hammingP50, ok := hammingCumP50[maxSeg]; ok && hammingP50 > 0 {
				improvement = (1.0 - float64(p50Cum)/float64(hammingP50)) * 100
			}

			results = append(results, ProgressiveResult{
				Method:            method.Name,
				Alpha:             method.Alpha,
				SegmentIndex:      maxSeg,
				Dims:              (maxSeg + 1) * 64,
				AvgAlive:          floatAvg(c.alive),
				P50Alive:          intPercentile(c.alive, 0.50),
				P95Alive:          intPercentile(c.alive, 0.95),
				AvgCumComputed:    floatAvg(c.cumComputed),
				P50CumComputed:    p50Cum,
				P95CumComputed:    p95Cum,
				AvgSkippedLB:      floatAvg(c.skippedLB),
				P50SkippedLB:      intPercentile(c.skippedLB, 0.50),
				P95SkippedLB:      intPercentile(c.skippedLB, 0.95),
				PctVsNaive:        floatAvg(c.cumComputed) / naive * 100,
				EstP50Ms:          float64(p50Cum) / distPerSec * 1000,
				EstP95Ms:          float64(p95Cum) / distPerSec * 1000,
				ImprovementVsHamm: improvement,
			})
		}
	}

	return results
}

func writeNormBoundOutputs(t *testing.T, outDir string, report NormBoundReport) {
	// Write single-segment CSV
	csvPath := filepath.Join(outDir, "norm_bound_single_segment.csv")
	writeSingleSegmentNormCSV(t, csvPath, report.SingleSegment)

	// Write progressive CSV
	csvPath = filepath.Join(outDir, "norm_bound_progressive.csv")
	writeProgressiveNormCSV(t, csvPath, report.Progressive)

	// Write JSON
	jsonPath := filepath.Join(outDir, "norm_bound.json")
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
	mdPath := filepath.Join(outDir, "norm_bound_report.md")
	writeNormBoundMarkdown(t, mdPath, report)
}

func writeSingleSegmentNormCSV(t *testing.T, path string, results []SingleSegmentResult) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{"method", "alpha", "segment", "dims", "avg_computed", "p50_computed", "p95_computed",
		"avg_skipped", "p50_skipped", "p95_skipped", "skip_pct", "gt_false_prune"}
	w.Write(header)

	for _, r := range results {
		row := []string{
			r.Method,
			fmt.Sprintf("%.2f", r.Alpha),
			strconv.Itoa(r.SegmentIndex),
			strconv.Itoa(r.Dims),
			fmt.Sprintf("%.1f", r.AvgComputed),
			strconv.Itoa(r.P50Computed),
			strconv.Itoa(r.P95Computed),
			fmt.Sprintf("%.1f", r.AvgSkipped),
			strconv.Itoa(r.P50Skipped),
			strconv.Itoa(r.P95Skipped),
			fmt.Sprintf("%.2f", r.SkipPct),
			strconv.Itoa(r.GTFalsePrune),
		}
		w.Write(row)
	}

	t.Logf("Single-segment CSV written to: %s", path)
}

func writeProgressiveNormCSV(t *testing.T, path string, results []ProgressiveResult) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create CSV: %v", err)
		return
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	header := []string{"method", "alpha", "segment", "dims", "avg_alive", "p50_alive", "p95_alive",
		"avg_cum_computed", "p50_cum_computed", "p95_cum_computed",
		"pct_vs_naive", "est_p50_ms", "est_p95_ms", "improvement_vs_hamming"}
	w.Write(header)

	for _, r := range results {
		row := []string{
			r.Method,
			fmt.Sprintf("%.2f", r.Alpha),
			strconv.Itoa(r.SegmentIndex),
			strconv.Itoa(r.Dims),
			fmt.Sprintf("%.1f", r.AvgAlive),
			strconv.Itoa(r.P50Alive),
			strconv.Itoa(r.P95Alive),
			fmt.Sprintf("%.1f", r.AvgCumComputed),
			strconv.Itoa(r.P50CumComputed),
			strconv.Itoa(r.P95CumComputed),
			fmt.Sprintf("%.2f", r.PctVsNaive),
			fmt.Sprintf("%.4f", r.EstP50Ms),
			fmt.Sprintf("%.4f", r.EstP95Ms),
			fmt.Sprintf("%.2f", r.ImprovementVsHamm),
		}
		w.Write(row)
	}

	t.Logf("Progressive CSV written to: %s", path)
}

func writeNormBoundMarkdown(t *testing.T, path string, report NormBoundReport) {
	f, err := os.Create(path)
	if err != nil {
		t.Errorf("Failed to create markdown: %v", err)
		return
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "# Norm-Bound Pruning Experiment\n\n")
	fmt.Fprintf(w, "**Date**: %s\n\n", report.Timestamp)

	fmt.Fprintf(w, "## Configuration\n\n")
	fmt.Fprintf(w, "| Parameter | Value |\n")
	fmt.Fprintf(w, "|-----------|-------|\n")
	fmt.Fprintf(w, "| Documents | %d |\n", report.Config.NumDocs)
	fmt.Fprintf(w, "| Queries | %d |\n", report.Config.NumQueries)
	fmt.Fprintf(w, "| Dimensions | %d |\n", report.Config.VectorDims)
	fmt.Fprintf(w, "| Segments | %d |\n", report.Config.NumSegments)
	fmt.Fprintf(w, "| K | %d |\n", report.Config.K)
	fmt.Fprintf(w, "| Alpha values | %v |\n\n", report.Config.AlphaValues)

	// Calibration
	fmt.Fprintf(w, "## Calibration (Correlation with Exact Distance)\n\n")
	fmt.Fprintf(w, "| Seg | Corr(Hamming) | Corr(Norm) | Corr(Best Combined) | Best Alpha |\n")
	fmt.Fprintf(w, "|-----|---------------|------------|---------------------|------------|\n")
	for _, c := range report.Calibration {
		fmt.Fprintf(w, "| %d | %.3f | %.3f | %.3f | %.2f |\n",
			c.SegmentIndex, c.CorrHammingExact, c.CorrNormExact, c.CorrCombinedExact, c.BestAlpha)
	}

	// Single-segment comparison
	fmt.Fprintf(w, "\n## Single-Segment Pruning Comparison\n\n")
	fmt.Fprintf(w, "| Seg | Method | Alpha | P50 Computed | P95 Computed | Skip %% | GT False Prune |\n")
	fmt.Fprintf(w, "|-----|--------|-------|--------------|--------------|--------|----------------|\n")
	for _, r := range report.SingleSegment {
		fmt.Fprintf(w, "| %d | %s | %.2f | %d | %d | %.2f | %d |\n",
			r.SegmentIndex, r.Method, r.Alpha, r.P50Computed, r.P95Computed, r.SkipPct, r.GTFalsePrune)
	}

	// Progressive comparison by method
	fmt.Fprintf(w, "\n## Progressive Pruning Comparison\n\n")

	// Group by method for easier reading
	methodOrder := []string{"hamming", "norm"}
	for _, alpha := range alphaValues {
		methodOrder = append(methodOrder, fmt.Sprintf("combined_%.2f", alpha))
	}

	fmt.Fprintf(w, "### Summary at 1536 dims (full)\n\n")
	fmt.Fprintf(w, "| Method | Alpha | P50 Cum Dist | P95 Cum Dist | %%Naive | Improvement vs Hamming |\n")
	fmt.Fprintf(w, "|--------|-------|--------------|--------------|--------|------------------------|\n")
	for _, r := range report.Progressive {
		if r.Dims == 1536 {
			fmt.Fprintf(w, "| %s | %.2f | %d | %d | %.2f | %.2f%% |\n",
				r.Method, r.Alpha, r.P50CumComputed, r.P95CumComputed, r.PctVsNaive, r.ImprovementVsHamm)
		}
	}

	// Early segment focus
	fmt.Fprintf(w, "\n### Early Segment Focus (Seg 0 and 1)\n\n")
	fmt.Fprintf(w, "| Seg | Method | Alpha | Skip %% (Single) | P50 Cum (Progressive) |\n")
	fmt.Fprintf(w, "|-----|--------|-------|-----------------|----------------------|\n")
	for _, r := range report.SingleSegment {
		if r.SegmentIndex <= 1 {
			// Find matching progressive
			for _, p := range report.Progressive {
				if p.SegmentIndex == r.SegmentIndex && p.Method == r.Method && p.Alpha == r.Alpha {
					fmt.Fprintf(w, "| %d | %s | %.2f | %.2f | %d |\n",
						r.SegmentIndex, r.Method, r.Alpha, r.SkipPct, p.P50CumComputed)
					break
				}
			}
		}
	}

	// Decision summary
	fmt.Fprintf(w, "\n## Decision Summary\n\n")

	// Find best single-segment improvement at seg 0
	bestSeg0Skip := 0.0
	bestSeg0Method := ""
	for _, r := range report.SingleSegment {
		if r.SegmentIndex == 0 && r.SkipPct > bestSeg0Skip && r.GTFalsePrune == 0 {
			bestSeg0Skip = r.SkipPct
			bestSeg0Method = fmt.Sprintf("%s (alpha=%.2f)", r.Method, r.Alpha)
		}
	}

	fmt.Fprintf(w, "- Best segment 0 skip: **%.2f%%** with %s\n", bestSeg0Skip, bestSeg0Method)

	if bestSeg0Skip >= 50 {
		fmt.Fprintf(w, "- **STRONG SIGNAL**: Skip >= 50%% at segment 0\n")
	} else if bestSeg0Skip >= 20 {
		fmt.Fprintf(w, "- **VERY INTERESTING**: Skip >= 20%% at segment 0\n")
	} else if bestSeg0Skip >= 10 {
		fmt.Fprintf(w, "- **INTERESTING**: Skip >= 10%% at segment 0\n")
	} else {
		fmt.Fprintf(w, "- **MINIMAL IMPROVEMENT**: Skip < 10%% at segment 0 - norm bound may not be worth the complexity\n")
	}

	t.Logf("Markdown written to: %s", path)
}

func printNormBoundSummary(t *testing.T, report NormBoundReport) {
	t.Log("\n" + strings.Repeat("=", 80))
	t.Log("NORM-BOUND PRUNING SUMMARY")
	t.Log(strings.Repeat("=", 80))

	t.Log("\n--- Calibration ---")
	for _, c := range report.Calibration {
		t.Logf("Seg %d: Corr(Hamm)=%.3f, Corr(Norm)=%.3f, Corr(Best)=%.3f (alpha=%.2f)",
			c.SegmentIndex, c.CorrHammingExact, c.CorrNormExact, c.CorrCombinedExact, c.BestAlpha)
	}

	t.Log("\n--- Single-Segment Skip % (Segment 0) ---")
	for _, r := range report.SingleSegment {
		if r.SegmentIndex == 0 {
			t.Logf("%s (alpha=%.2f): Skip %.2f%%, GT false prune: %d",
				r.Method, r.Alpha, r.SkipPct, r.GTFalsePrune)
		}
	}

	t.Log("\n--- Progressive at Full Dims (1536) ---")
	for _, r := range report.Progressive {
		if r.Dims == 1536 {
			t.Logf("%s (alpha=%.2f): P50=%d, P95=%d, %%naive=%.2f, improvement=%.2f%%",
				r.Method, r.Alpha, r.P50CumComputed, r.P95CumComputed, r.PctVsNaive, r.ImprovementVsHamm)
		}
	}

	t.Log(strings.Repeat("=", 80))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
