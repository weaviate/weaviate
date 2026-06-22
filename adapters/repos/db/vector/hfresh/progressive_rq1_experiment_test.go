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
// PROGRESSIVE RQ1 RETENTION EXPERIMENT
// =============================================================================
//
// This experiment measures the oracle progressive-retention curve for RQ1/BQ
// compressed segments. It answers: after how many 64-bit segments can we
// reduce 100K candidates to 300-1000 while preserving all GT@10?
//
// For each dataset/query:
// 1. Compress all base vectors and queries using RQ1 (1-bit-per-dimension)
// 2. For each accumulated segment index s (segment = 64 dimensions):
//    - Compute accumulated Hamming distance using segments 0..s
//    - Find threshold = max(accDist[gtID] for gtID in GT@10)
//    - Count requiredKeepLTE = vectors where accDist <= threshold
// 3. Aggregate statistics by segment
//
// =============================================================================

var (
	flagRQ1DocsPath    = flag.String("rq1-docs", "", "Path to documents JSONL file")
	flagRQ1QueriesPath = flag.String("rq1-queries", "", "Path to queries JSONL file")
	flagRQ1GTPath      = flag.String("rq1-gt", "", "Path to ground-truth JSONL file")
	flagRQ1OutputDir   = flag.String("rq1-out", "/tmp/rq1_progressive", "Output directory for results")
	flagRQ1MaxDocs     = flag.Int("rq1-max-docs", 100000, "Maximum documents to use (sample if larger)")
	flagRQ1K           = flag.Int("rq1-k", 10, "K for recall@k")
	flagRQ1Seed        = flag.Uint64("rq1-seed", 42, "Seed for RQ1 quantizer")
)

// SegmentStats holds statistics for a single segment index
type SegmentStats struct {
	SegmentIndex     int     `json:"segment_index"`
	DimensionsUsed   int     `json:"dimensions_used"`
	AvgRequiredKeep  float64 `json:"avg_required_keep"`
	P50RequiredKeep  int     `json:"p50_required_keep"`
	P90RequiredKeep  int     `json:"p90_required_keep"`
	P95RequiredKeep  int     `json:"p95_required_keep"`
	P99RequiredKeep  int     `json:"p99_required_keep"`
	MaxRequiredKeep  int     `json:"max_required_keep"`
	AvgKeepPercent   float64 `json:"avg_keep_percent"`
	AvgTies          float64 `json:"avg_ties_at_threshold"`
	P95Ties          int     `json:"p95_ties_at_threshold"`
	PctLTE300        float64 `json:"pct_queries_lte_300"`
	PctLTE500        float64 `json:"pct_queries_lte_500"`
	PctLTE1000       float64 `json:"pct_queries_lte_1000"`
	PctLTE5000       float64 `json:"pct_queries_lte_5000"`
	PctLTE10000      float64 `json:"pct_queries_lte_10000"`
}

// QuerySegmentResult holds per-query, per-segment results
type QuerySegmentResult struct {
	QueryID          string
	SegmentIndex     int
	RequiredKeepLTE  int
	RequiredKeepLT   int
	TiesAtThreshold  int
	Threshold        int
}

// ProgressiveRQ1Report is the full experiment output
type ProgressiveRQ1Report struct {
	Config struct {
		DocsPath       string `json:"docs_path"`
		QueriesPath    string `json:"queries_path"`
		GTPath         string `json:"gt_path"`
		NumDocs        int    `json:"num_docs"`
		NumQueries     int    `json:"num_queries"`
		VectorDims     int    `json:"vector_dims"`
		NumSegments    int    `json:"num_segments"`
		K              int    `json:"k"`
		Seed           uint64 `json:"seed"`
	} `json:"config"`

	// Per-segment statistics
	SegmentStats []SegmentStats `json:"segment_stats"`

	// Milestones: first segment where threshold is reached
	Milestones struct {
		P50LTE500  int `json:"p50_lte_500"`
		P95LTE500  int `json:"p95_lte_500"`
		P50LTE1000 int `json:"p50_lte_1000"`
		P95LTE1000 int `json:"p95_lte_1000"`
		P50LTE5000 int `json:"p50_lte_5000"`
		P95LTE5000 int `json:"p95_lte_5000"`
	} `json:"milestones"`

	// Timing
	EncodingTimeMs  float64 `json:"encoding_time_ms"`
	EvaluationTimeMs float64 `json:"evaluation_time_ms"`
	Timestamp       string  `json:"timestamp"`
}

// TestProgressiveRQ1Retention runs the progressive retention experiment
func TestProgressiveRQ1Retention(t *testing.T) {
	if *flagRQ1DocsPath == "" || *flagRQ1QueriesPath == "" || *flagRQ1GTPath == "" {
		t.Skip("Skipping: requires -rq1-docs, -rq1-queries, -rq1-gt flags")
	}

	// Load data using existing infrastructure
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagRQ1DocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagRQ1QueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	gt, err := loadGroundTruth(*flagRQ1GTPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(gt))

	// Sample docs if needed
	maxDocs := *flagRQ1MaxDocs
	if len(docs) > maxDocs {
		t.Logf("Sampling %d documents from %d", maxDocs, len(docs))
		docs = sampleDocs(docs, maxDocs, *flagRQ1Seed)
	}

	// Build doc ID set for filtering GT
	docIDSet := make(map[uint64]int) // docID -> index in docs
	for i, doc := range docs {
		docIDSet[doc.ID] = i
	}

	// Determine dimensions from first doc with vectors
	dims := 0
	for _, doc := range docs {
		if len(doc.Vectors) > 0 && len(doc.Vectors[0]) > 0 {
			dims = len(doc.Vectors[0])
			break
		}
	}
	require.Greater(t, dims, 0, "could not determine vector dimensions")
	t.Logf("Vector dimensions: %d", dims)

	// Number of 64-bit segments
	numSegments := (dims + 63) / 64
	t.Logf("Number of 64-bit segments: %d", numSegments)

	// Create RQ1 quantizer
	t.Log("Creating RQ1 quantizer...")
	quantizer, err := compressionhelpers.NewBinaryRotationalQuantizer(dims, *flagRQ1Seed, distancer.NewL2SquaredProvider())
	require.NoError(t, err)

	// Encode all document vectors
	t.Log("Encoding document vectors...")
	encodeStart := time.Now()

	// For multi-vector docs, we need to encode each token and track which segments they use
	// For this experiment, we'll use the FDE approach: encode the full doc and compute distance
	// Actually, for simplicity let's use the first vector of each doc (or FDE-style flattening)
	// The user mentioned "base vectors" which suggests single-vector scenario

	// Let's flatten multi-vectors into single FDE representation for this experiment
	docCodes := make([][]uint64, len(docs))
	for i, doc := range docs {
		if len(doc.Vectors) == 0 {
			continue
		}
		// Use first vector for simplicity (or we could average/flatten)
		// For multi-vector, we'd need a different approach
		// Let's use the first vector as representative
		vec := doc.Vectors[0]
		if len(vec) != dims {
			// Skip mismatched dimensions
			continue
		}
		docCodes[i] = quantizer.Encode(vec)
	}

	// Encode query vectors
	queryCodes := make([][]uint64, len(queries))
	for i, q := range queries {
		if len(q.Vectors) == 0 {
			continue
		}
		vec := q.Vectors[0]
		if len(vec) != dims {
			continue
		}
		queryCodes[i] = quantizer.Encode(vec)
	}

	encodingTime := time.Since(encodeStart)
	t.Logf("Encoding complete in %.2fms", float64(encodingTime.Milliseconds()))

	// Compute exact ground truth distances for verification
	// (We use the provided GT, but filter to docs in our sample)

	// Evaluate progressive retention
	t.Log("Evaluating progressive retention...")
	evalStart := time.Now()

	// Per-query, per-segment results
	allResults := make([][]QuerySegmentResult, len(queries))

	k := *flagRQ1K

	for qi, q := range queries {
		gtIDs, ok := gt[q.ID]
		if !ok {
			continue
		}

		qCode := queryCodes[qi]
		if qCode == nil {
			continue
		}

		// Filter GT to docs in our sample and limit to k
		filteredGT := make([]uint64, 0, k)
		for _, gtID := range gtIDs {
			if _, inSample := docIDSet[gtID]; inSample {
				filteredGT = append(filteredGT, gtID)
				if len(filteredGT) >= k {
					break
				}
			}
		}

		if len(filteredGT) == 0 {
			continue
		}

		// For each segment, compute accumulated distances and retention stats
		segmentResults := make([]QuerySegmentResult, numSegments)

		for s := 0; s < numSegments; s++ {
			// Compute accumulated Hamming distance for all docs using segments 0..s
			// Each segment = 64 bits = 1 uint64 (plus header in RQ1 code)

			// Find threshold: max accumulated distance among GT docs
			threshold := 0
			for _, gtID := range filteredGT {
				docIdx, ok := docIDSet[gtID]
				if !ok {
					continue
				}
				dCode := docCodes[docIdx]
				if dCode == nil {
					continue
				}
				dist := accumulatedHammingDistance(qCode, dCode, s+1)
				if dist > threshold {
					threshold = dist
				}
			}

			// Count how many docs have accDist <= threshold and < threshold
			countLTE := 0
			countLT := 0
			for di, dCode := range docCodes {
				if dCode == nil {
					continue
				}
				_ = di
				dist := accumulatedHammingDistance(qCode, dCode, s+1)
				if dist <= threshold {
					countLTE++
				}
				if dist < threshold {
					countLT++
				}
			}

			segmentResults[s] = QuerySegmentResult{
				QueryID:         q.ID,
				SegmentIndex:    s,
				RequiredKeepLTE: countLTE,
				RequiredKeepLT:  countLT,
				TiesAtThreshold: countLTE - countLT,
				Threshold:       threshold,
			}
		}

		allResults[qi] = segmentResults
	}

	evalTime := time.Since(evalStart)
	t.Logf("Evaluation complete in %.2fms", float64(evalTime.Milliseconds()))

	// Aggregate statistics by segment
	t.Log("Aggregating statistics...")
	segmentStats := aggregateSegmentStats(allResults, numSegments, len(docs))

	// Find milestones
	milestones := findMilestones(segmentStats)

	// Build report
	report := ProgressiveRQ1Report{
		SegmentStats:     segmentStats,
		EncodingTimeMs:   float64(encodingTime.Milliseconds()),
		EvaluationTimeMs: float64(evalTime.Milliseconds()),
		Timestamp:        time.Now().Format(time.RFC3339),
	}
	report.Config.DocsPath = *flagRQ1DocsPath
	report.Config.QueriesPath = *flagRQ1QueriesPath
	report.Config.GTPath = *flagRQ1GTPath
	report.Config.NumDocs = len(docs)
	report.Config.NumQueries = len(queries)
	report.Config.VectorDims = dims
	report.Config.NumSegments = numSegments
	report.Config.K = k
	report.Config.Seed = *flagRQ1Seed
	report.Milestones = milestones

	// Create output directory
	err = os.MkdirAll(*flagRQ1OutputDir, 0755)
	require.NoError(t, err)

	// Write CSV
	csvPath := filepath.Join(*flagRQ1OutputDir, "progressive_rq1.csv")
	err = writeSegmentStatsCSV(csvPath, segmentStats)
	require.NoError(t, err)
	t.Logf("CSV written to: %s", csvPath)

	// Write JSON report
	jsonPath := filepath.Join(*flagRQ1OutputDir, "progressive_rq1.json")
	jsonData, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	err = os.WriteFile(jsonPath, jsonData, 0644)
	require.NoError(t, err)
	t.Logf("JSON report written to: %s", jsonPath)

	// Write Markdown summary
	mdPath := filepath.Join(*flagRQ1OutputDir, "progressive_rq1.md")
	err = writeMarkdownReport(mdPath, report)
	require.NoError(t, err)
	t.Logf("Markdown report written to: %s", mdPath)

	// Print summary
	printSummary(t, report)
}

// accumulatedHammingDistance computes Hamming distance using first numSegments uint64 blocks
// Note: RQ1 code format is [header_uint64, bit_block_0, bit_block_1, ...]
// The header (index 0) contains Step and SquaredNorm, bits start at index 1
func accumulatedHammingDistance(qCode, dCode []uint64, numSegments int) int {
	// Bits start at index 1 (after header)
	qBits := qCode[1:]
	dBits := dCode[1:]

	if numSegments > len(qBits) {
		numSegments = len(qBits)
	}
	if numSegments > len(dBits) {
		numSegments = len(dBits)
	}

	dist := 0
	for i := 0; i < numSegments; i++ {
		dist += bits.OnesCount64(qBits[i] ^ dBits[i])
	}
	return dist
}

// sampleDocs deterministically samples n docs from the slice
func sampleDocs(docs []DocInput, n int, seed uint64) []DocInput {
	if n >= len(docs) {
		return docs
	}

	// Use deterministic sampling: take every k-th doc
	step := len(docs) / n
	sampled := make([]DocInput, 0, n)
	for i := 0; i < len(docs) && len(sampled) < n; i += step {
		sampled = append(sampled, docs[i])
	}

	// Fill remaining if needed
	for i := 0; len(sampled) < n && i < len(docs); i++ {
		if i%step != 0 { // Skip already added
			sampled = append(sampled, docs[i])
		}
	}

	return sampled[:n]
}

// aggregateSegmentStats computes statistics across all queries for each segment
func aggregateSegmentStats(allResults [][]QuerySegmentResult, numSegments int, numDocs int) []SegmentStats {
	stats := make([]SegmentStats, numSegments)

	for s := 0; s < numSegments; s++ {
		// Collect all requiredKeepLTE values for this segment
		keepValues := make([]int, 0)
		tieValues := make([]int, 0)

		for _, queryResults := range allResults {
			if queryResults == nil || s >= len(queryResults) {
				continue
			}
			keepValues = append(keepValues, queryResults[s].RequiredKeepLTE)
			tieValues = append(tieValues, queryResults[s].TiesAtThreshold)
		}

		if len(keepValues) == 0 {
			continue
		}

		// Sort for percentiles
		sort.Ints(keepValues)
		sort.Ints(tieValues)

		n := len(keepValues)

		// Compute statistics
		stats[s].SegmentIndex = s
		stats[s].DimensionsUsed = (s + 1) * 64
		stats[s].AvgRequiredKeep = average(keepValues)
		stats[s].P50RequiredKeep = percentile(keepValues, 0.50)
		stats[s].P90RequiredKeep = percentile(keepValues, 0.90)
		stats[s].P95RequiredKeep = percentile(keepValues, 0.95)
		stats[s].P99RequiredKeep = percentile(keepValues, 0.99)
		stats[s].MaxRequiredKeep = keepValues[n-1]
		stats[s].AvgKeepPercent = stats[s].AvgRequiredKeep / float64(numDocs) * 100
		stats[s].AvgTies = average(tieValues)
		stats[s].P95Ties = percentile(tieValues, 0.95)

		// Count queries meeting thresholds
		countLTE300 := countLTE(keepValues, 300)
		countLTE500 := countLTE(keepValues, 500)
		countLTE1000 := countLTE(keepValues, 1000)
		countLTE5000 := countLTE(keepValues, 5000)
		countLTE10000 := countLTE(keepValues, 10000)

		stats[s].PctLTE300 = float64(countLTE300) / float64(n) * 100
		stats[s].PctLTE500 = float64(countLTE500) / float64(n) * 100
		stats[s].PctLTE1000 = float64(countLTE1000) / float64(n) * 100
		stats[s].PctLTE5000 = float64(countLTE5000) / float64(n) * 100
		stats[s].PctLTE10000 = float64(countLTE10000) / float64(n) * 100
	}

	return stats
}

func average(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0
	for _, v := range values {
		sum += v
	}
	return float64(sum) / float64(len(values))
}

func percentile(sortedValues []int, p float64) int {
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

func countLTE(sortedValues []int, threshold int) int {
	// Binary search for first value > threshold
	idx := sort.SearchInts(sortedValues, threshold+1)
	return idx
}

func findMilestones(stats []SegmentStats) struct {
	P50LTE500  int `json:"p50_lte_500"`
	P95LTE500  int `json:"p95_lte_500"`
	P50LTE1000 int `json:"p50_lte_1000"`
	P95LTE1000 int `json:"p95_lte_1000"`
	P50LTE5000 int `json:"p50_lte_5000"`
	P95LTE5000 int `json:"p95_lte_5000"`
} {
	m := struct {
		P50LTE500  int `json:"p50_lte_500"`
		P95LTE500  int `json:"p95_lte_500"`
		P50LTE1000 int `json:"p50_lte_1000"`
		P95LTE1000 int `json:"p95_lte_1000"`
		P50LTE5000 int `json:"p50_lte_5000"`
		P95LTE5000 int `json:"p95_lte_5000"`
	}{-1, -1, -1, -1, -1, -1}

	for _, s := range stats {
		if m.P50LTE500 < 0 && s.P50RequiredKeep <= 500 {
			m.P50LTE500 = s.SegmentIndex
		}
		if m.P95LTE500 < 0 && s.P95RequiredKeep <= 500 {
			m.P95LTE500 = s.SegmentIndex
		}
		if m.P50LTE1000 < 0 && s.P50RequiredKeep <= 1000 {
			m.P50LTE1000 = s.SegmentIndex
		}
		if m.P95LTE1000 < 0 && s.P95RequiredKeep <= 1000 {
			m.P95LTE1000 = s.SegmentIndex
		}
		if m.P50LTE5000 < 0 && s.P50RequiredKeep <= 5000 {
			m.P50LTE5000 = s.SegmentIndex
		}
		if m.P95LTE5000 < 0 && s.P95RequiredKeep <= 5000 {
			m.P95LTE5000 = s.SegmentIndex
		}
	}

	return m
}

func writeSegmentStatsCSV(path string, stats []SegmentStats) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Header
	header := []string{
		"segment_index", "dimensions_used",
		"avg_required_keep", "p50_required_keep", "p90_required_keep",
		"p95_required_keep", "p99_required_keep", "max_required_keep",
		"avg_keep_percent", "avg_ties", "p95_ties",
		"pct_lte_300", "pct_lte_500", "pct_lte_1000", "pct_lte_5000", "pct_lte_10000",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	for _, s := range stats {
		row := []string{
			strconv.Itoa(s.SegmentIndex),
			strconv.Itoa(s.DimensionsUsed),
			fmt.Sprintf("%.1f", s.AvgRequiredKeep),
			strconv.Itoa(s.P50RequiredKeep),
			strconv.Itoa(s.P90RequiredKeep),
			strconv.Itoa(s.P95RequiredKeep),
			strconv.Itoa(s.P99RequiredKeep),
			strconv.Itoa(s.MaxRequiredKeep),
			fmt.Sprintf("%.2f", s.AvgKeepPercent),
			fmt.Sprintf("%.1f", s.AvgTies),
			strconv.Itoa(s.P95Ties),
			fmt.Sprintf("%.1f", s.PctLTE300),
			fmt.Sprintf("%.1f", s.PctLTE500),
			fmt.Sprintf("%.1f", s.PctLTE1000),
			fmt.Sprintf("%.1f", s.PctLTE5000),
			fmt.Sprintf("%.1f", s.PctLTE10000),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func writeMarkdownReport(path string, report ProgressiveRQ1Report) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bufio.NewWriter(f)
	defer w.Flush()

	fmt.Fprintf(w, "# Progressive RQ1 Retention Experiment\n\n")
	fmt.Fprintf(w, "**Date**: %s\n\n", report.Timestamp)

	fmt.Fprintf(w, "## Configuration\n\n")
	fmt.Fprintf(w, "| Parameter | Value |\n")
	fmt.Fprintf(w, "|-----------|-------|\n")
	fmt.Fprintf(w, "| Documents | %d |\n", report.Config.NumDocs)
	fmt.Fprintf(w, "| Queries | %d |\n", report.Config.NumQueries)
	fmt.Fprintf(w, "| Dimensions | %d |\n", report.Config.VectorDims)
	fmt.Fprintf(w, "| Segments (64-bit blocks) | %d |\n", report.Config.NumSegments)
	fmt.Fprintf(w, "| K | %d |\n", report.Config.K)
	fmt.Fprintf(w, "| RQ1 Seed | %d |\n\n", report.Config.Seed)

	fmt.Fprintf(w, "## Milestones\n\n")
	fmt.Fprintf(w, "First segment index where threshold is reached:\n\n")
	fmt.Fprintf(w, "| Metric | Segment | Dimensions |\n")
	fmt.Fprintf(w, "|--------|---------|------------|\n")
	fmt.Fprintf(w, "| P50 ≤ 500 | %d | %d |\n", report.Milestones.P50LTE500, (report.Milestones.P50LTE500+1)*64)
	fmt.Fprintf(w, "| P95 ≤ 500 | %d | %d |\n", report.Milestones.P95LTE500, (report.Milestones.P95LTE500+1)*64)
	fmt.Fprintf(w, "| P50 ≤ 1000 | %d | %d |\n", report.Milestones.P50LTE1000, (report.Milestones.P50LTE1000+1)*64)
	fmt.Fprintf(w, "| P95 ≤ 1000 | %d | %d |\n", report.Milestones.P95LTE1000, (report.Milestones.P95LTE1000+1)*64)
	fmt.Fprintf(w, "| P50 ≤ 5000 | %d | %d |\n", report.Milestones.P50LTE5000, (report.Milestones.P50LTE5000+1)*64)
	fmt.Fprintf(w, "| P95 ≤ 5000 | %d | %d |\n\n", report.Milestones.P95LTE5000, (report.Milestones.P95LTE5000+1)*64)

	fmt.Fprintf(w, "## Compact Summary (Key Segments)\n\n")
	fmt.Fprintf(w, "| Dims | Seg | P50 Keep | P95 Keep | Max Keep | Avg Keep%% | %%≤500 | %%≤1000 | P95 Ties |\n")
	fmt.Fprintf(w, "|------|-----|----------|----------|----------|-----------|-------|--------|----------|\n")

	// Key segment dimensions: 64, 128, 192, 256, 320, 384, 512
	keyDims := []int{64, 128, 192, 256, 320, 384, 512}
	for _, d := range keyDims {
		segIdx := (d / 64) - 1
		if segIdx >= 0 && segIdx < len(report.SegmentStats) {
			s := report.SegmentStats[segIdx]
			fmt.Fprintf(w, "| %d | %d | %d | %d | %d | %.1f | %.0f | %.0f | %d |\n",
				s.DimensionsUsed, s.SegmentIndex, s.P50RequiredKeep, s.P95RequiredKeep,
				s.MaxRequiredKeep, s.AvgKeepPercent, s.PctLTE500, s.PctLTE1000, s.P95Ties)
		}
	}

	fmt.Fprintf(w, "\n## Timing\n\n")
	fmt.Fprintf(w, "- Encoding: %.2f ms\n", report.EncodingTimeMs)
	fmt.Fprintf(w, "- Evaluation: %.2f ms\n\n", report.EvaluationTimeMs)

	fmt.Fprintf(w, "## Full Segment Table\n\n")
	fmt.Fprintf(w, "| Seg | Dims | Avg Keep | P50 | P90 | P95 | P99 | Max | Keep%% | Avg Ties | P95 Ties |\n")
	fmt.Fprintf(w, "|-----|------|----------|-----|-----|-----|-----|-----|-------|----------|----------|\n")

	for _, s := range report.SegmentStats {
		fmt.Fprintf(w, "| %d | %d | %.0f | %d | %d | %d | %d | %d | %.1f | %.0f | %d |\n",
			s.SegmentIndex, s.DimensionsUsed, s.AvgRequiredKeep,
			s.P50RequiredKeep, s.P90RequiredKeep, s.P95RequiredKeep, s.P99RequiredKeep,
			s.MaxRequiredKeep, s.AvgKeepPercent, s.AvgTies, s.P95Ties)
	}

	return nil
}

func printSummary(t *testing.T, report ProgressiveRQ1Report) {
	t.Log("\n" + strings.Repeat("=", 80))
	t.Log("PROGRESSIVE RQ1 RETENTION SUMMARY")
	t.Log(strings.Repeat("=", 80))

	t.Logf("Dataset: %d docs, %d queries, %d dims, %d segments",
		report.Config.NumDocs, report.Config.NumQueries, report.Config.VectorDims, report.Config.NumSegments)

	t.Log("\nMilestones (first segment reaching threshold):")
	t.Logf("  P50 ≤ 500:  segment %d (%d dims)", report.Milestones.P50LTE500, (report.Milestones.P50LTE500+1)*64)
	t.Logf("  P95 ≤ 500:  segment %d (%d dims)", report.Milestones.P95LTE500, (report.Milestones.P95LTE500+1)*64)
	t.Logf("  P50 ≤ 1000: segment %d (%d dims)", report.Milestones.P50LTE1000, (report.Milestones.P50LTE1000+1)*64)
	t.Logf("  P95 ≤ 1000: segment %d (%d dims)", report.Milestones.P95LTE1000, (report.Milestones.P95LTE1000+1)*64)

	t.Log("\nKey segment statistics:")
	t.Log("  Dims  | P50 Keep | P95 Keep | Max Keep | %≤500  | %≤1000")
	t.Log("  ------|----------|----------|----------|--------|--------")

	keyDims := []int{64, 128, 256, 512}
	for _, d := range keyDims {
		segIdx := (d / 64) - 1
		if segIdx >= 0 && segIdx < len(report.SegmentStats) {
			s := report.SegmentStats[segIdx]
			t.Logf("  %4d  | %8d | %8d | %8d | %5.0f%% | %5.0f%%",
				s.DimensionsUsed, s.P50RequiredKeep, s.P95RequiredKeep,
				s.MaxRequiredKeep, s.PctLTE500, s.PctLTE1000)
		}
	}

	t.Log(strings.Repeat("=", 80))
}
