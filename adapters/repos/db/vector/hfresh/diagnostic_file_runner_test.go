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
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// =============================================================================
// INPUT FILE FORMATS
// =============================================================================
//
// All input files use JSONL format (one JSON object per line).
//
// DOCUMENTS FILE (docs.jsonl):
// Each line contains a document with its multi-vectors:
//   {"id": 0, "vectors": [[0.1, 0.2, ...], [0.3, 0.4, ...], ...]}
//   {"id": 1, "vectors": [[0.5, 0.6, ...], [0.7, 0.8, ...], ...]}
//
// QUERIES FILE (queries.jsonl):
// Each line contains a query with its multi-vectors:
//   {"id": "q0", "vectors": [[0.1, 0.2, ...], [0.3, 0.4, ...], ...]}
//   {"id": "q1", "vectors": [[0.5, 0.6, ...], [0.7, 0.8, ...], ...]}
//
// GROUND TRUTH FILE (gt.jsonl):
// Each line contains ground-truth doc IDs for a query (in order matching queries file):
//   {"query_id": "q0", "doc_ids": [5, 12, 3, 8, ...]}
//   {"query_id": "q1", "doc_ids": [1, 7, 9, 2, ...]}
//
// OUTPUT REPORT (report.json):
// JSON file with recall attribution breakdown per query and aggregate stats.
//
// =============================================================================

// Command-line flags for the diagnostic runner
var (
	flagDocsPath         = flag.String("docs", "", "Path to documents JSONL file")
	flagQueriesPath      = flag.String("queries", "", "Path to queries JSONL file")
	flagGroundTruthPath  = flag.String("groundtruth", "", "Path to ground-truth JSONL file")
	flagK                = flag.Int("k", 10, "Number of results to return per query")
	flagSearchProbe      = flag.Int("searchprobe", 64, "Number of centroids to probe")
	flagRescoreLimit     = flag.Int("rescorelimit", 100, "Maximum candidates for rescoring")
	flagOutputPath       = flag.String("out", "", "Path to output report JSON file")
	flagVerbose          = flag.Bool("verbose", false, "Enable verbose logging")
	flagSearchProbeRange = flag.String("searchprobe-range", "", "Comma-separated searchProbe values for sweep (e.g., 64,128,256,512)")
	flagSweepConfigs     = flag.String("sweep-configs", "", "Comma-separated probe:rescore pairs (e.g., 64:100,128:256,256:512,512:1024)")
)

// =============================================================================
// INPUT DATA STRUCTURES
// =============================================================================

// DocInput represents a document in the input JSONL file.
type DocInput struct {
	ID      uint64      `json:"id"`
	Vectors [][]float32 `json:"vectors"`
}

// QueryInput represents a query in the input JSONL file.
type QueryInput struct {
	ID      string      `json:"id"`
	Vectors [][]float32 `json:"vectors"`
}

// GroundTruthInput represents ground-truth for a query.
type GroundTruthInput struct {
	QueryID string   `json:"query_id"`
	DocIDs  []uint64 `json:"doc_ids"`
}

// =============================================================================
// OUTPUT DATA STRUCTURES
// =============================================================================

// QueryResult contains trace and attribution for a single query.
type QueryResult struct {
	QueryID           string   `json:"query_id"`
	K                 int      `json:"k"`
	GroundTruthIDs    []uint64 `json:"ground_truth_ids"`
	ReturnedIDs       []uint64 `json:"returned_ids"`
	SelectedCentroids []uint64 `json:"selected_centroids"`
	ApproxTopIDs      []uint64 `json:"approx_top_ids"`

	// Scan statistics
	TotalScanned     int `json:"total_scanned"`
	UniqueEnumerated int `json:"unique_enumerated"`
	SkippedDeleted   int `json:"skipped_deleted"`
	SkippedDuplicate int `json:"skipped_duplicate"`
	SkippedAllowList int `json:"skipped_allow_list"`

	// Attribution breakdown
	RoutingFailures []uint64 `json:"routing_failures"`
	ApproxFailures  []uint64 `json:"approx_failures"`
	ExactFailures   []uint64 `json:"exact_failures"`
	Successes       []uint64 `json:"successes"`

	// Per-query recall
	Recall float64 `json:"recall"`

	// Timing
	SearchDurationMs float64 `json:"search_duration_ms"`
}

// DiagnosticOutput is the full output report.
type DiagnosticOutput struct {
	// Configuration
	Config struct {
		DocsPath        string `json:"docs_path"`
		QueriesPath     string `json:"queries_path"`
		GroundTruthPath string `json:"ground_truth_path"`
		K               int    `json:"k"`
		SearchProbe     int    `json:"search_probe"`
		RescoreLimit    int    `json:"rescore_limit"`
	} `json:"config"`

	// Dataset stats
	NumDocs    int `json:"num_docs"`
	NumQueries int `json:"num_queries"`
	VectorDims int `json:"vector_dims"`

	// Index stats
	NumPostings       int     `json:"num_postings"`
	IndexBuildTimeSec float64 `json:"index_build_time_sec"`

	// Aggregate attribution
	TotalGroundTruth int `json:"total_ground_truth"`
	TotalRouting     int `json:"total_routing_failures"`
	TotalApprox      int `json:"total_approx_failures"`
	TotalExact       int `json:"total_exact_failures"`
	TotalSuccesses   int `json:"total_successes"`

	// Aggregate percentages
	RoutingFailureRate float64 `json:"routing_failure_rate"`
	ApproxFailureRate  float64 `json:"approx_failure_rate"`
	ExactFailureRate   float64 `json:"exact_failure_rate"`
	SuccessRate        float64 `json:"success_rate"`

	// Recall metrics
	OverallRecall  float64 `json:"overall_recall"`
	AverageRecall  float64 `json:"average_recall"`
	MedianRecall   float64 `json:"median_recall"`
	MinRecall      float64 `json:"min_recall"`
	MaxRecall      float64 `json:"max_recall"`
	RecallAt1      float64 `json:"recall_at_1"`
	RecallAt5      float64 `json:"recall_at_5"`
	RecallAt10     float64 `json:"recall_at_10"`

	// Per-query results
	Queries []QueryResult `json:"queries"`

	// Timestamp
	Timestamp string `json:"timestamp"`
}

// =============================================================================
// FILE LOADING
// =============================================================================

func loadDocs(path string) ([]DocInput, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open docs file: %w", err)
	}
	defer f.Close()

	var docs []DocInput
	scanner := bufio.NewScanner(f)
	// Increase buffer size for large vectors
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var doc DocInput
		if err := json.Unmarshal(line, &doc); err != nil {
			return nil, fmt.Errorf("parse doc at line %d: %w", lineNum, err)
		}
		docs = append(docs, doc)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan docs file: %w", err)
	}

	return docs, nil
}

func loadQueries(path string) ([]QueryInput, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open queries file: %w", err)
	}
	defer f.Close()

	var queries []QueryInput
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var q QueryInput
		if err := json.Unmarshal(line, &q); err != nil {
			return nil, fmt.Errorf("parse query at line %d: %w", lineNum, err)
		}
		queries = append(queries, q)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan queries file: %w", err)
	}

	return queries, nil
}

func loadGroundTruth(path string) (map[string][]uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open ground-truth file: %w", err)
	}
	defer f.Close()

	gt := make(map[string][]uint64)
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var g GroundTruthInput
		if err := json.Unmarshal(line, &g); err != nil {
			return nil, fmt.Errorf("parse ground-truth at line %d: %w", lineNum, err)
		}
		gt[g.QueryID] = g.DocIDs
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan ground-truth file: %w", err)
	}

	return gt, nil
}

// =============================================================================
// INDEX BUILDING
// =============================================================================

type fileRunnerIndex struct {
	index       *HFresh
	mvStore     map[uint64][][]float32
	logger      logrus.FieldLogger
	scheduler   *queue.Scheduler
	cleanupFunc func()
}

func createFileRunnerIndex(t *testing.T, searchProbe, rescoreLimit int, verbose bool) *fileRunnerIndex {
	t.Helper()

	var logger *logrus.Logger
	if verbose {
		logger = logrus.New()
		logger.SetLevel(logrus.DebugLevel)
		logger.SetOutput(os.Stderr)
	} else {
		logger, _ = test.NewNullLogger()
	}

	cfg := DefaultConfig()
	mvStore := make(map[uint64][][]float32)

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	cfg.Scheduler = scheduler
	cfg.RootPath = t.TempDir()

	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh_file_runner",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger
	cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
		vecs, ok := mvStore[id]
		if !ok {
			return nil, fmt.Errorf("multi-vector not found for id %d", id)
		}
		return vecs, nil
	}

	scheduler.Start()

	uc := ent.NewDefaultUserConfig()
	uc.Multivector.Enabled = true
	uc.Multivector.MuveraConfig.Enabled = true
	uc.Multivector.MuveraConfig.KSim = enthnsw.DefaultMultivectorKSim
	uc.Multivector.MuveraConfig.DProjections = enthnsw.DefaultMultivectorDProjections
	uc.Multivector.MuveraConfig.Repetitions = enthnsw.DefaultMultivectorRepetitions
	uc.SearchProbe = uint32(searchProbe)
	uc.RQ.RescoreLimit = rescoreLimit

	store := testinghelpers.NewDummyStore(t)

	index, err := New(cfg, uc, store)
	require.NoError(t, err)
	index.multivectorForIdThunk = cfg.MultiVectorForIDThunk

	cleanup := func() {
		_ = index.Shutdown(context.Background())
		scheduler.Close(context.Background())
	}

	return &fileRunnerIndex{
		index:       index,
		mvStore:     mvStore,
		logger:      logger,
		scheduler:   scheduler,
		cleanupFunc: cleanup,
	}
}

func (f *fileRunnerIndex) insertDoc(ctx context.Context, docID uint64, vectors [][]float32) error {
	f.mvStore[docID] = vectors
	return f.index.AddMulti(ctx, docID, vectors)
}

func (f *fileRunnerIndex) cleanup() {
	if f.cleanupFunc != nil {
		f.cleanupFunc()
	}
}

// =============================================================================
// RECALL ATTRIBUTION
// =============================================================================

func classifyFailures(
	groundTruth []uint64,
	selectedCentroids []uint64,
	approxTopIDs []uint64,
	returnedIDs []uint64,
	membership map[uint64][]uint64,
) (routing, approx, exact, success []uint64) {
	selectedSet := make(map[uint64]bool, len(selectedCentroids))
	for _, c := range selectedCentroids {
		selectedSet[c] = true
	}

	approxSet := make(map[uint64]bool, len(approxTopIDs))
	for _, id := range approxTopIDs {
		approxSet[id] = true
	}

	returnedSet := make(map[uint64]bool, len(returnedIDs))
	for _, id := range returnedIDs {
		returnedSet[id] = true
	}

	for _, gtID := range groundTruth {
		docPostings := membership[gtID]
		inSelectedPosting := false
		for _, postingID := range docPostings {
			if selectedSet[postingID] {
				inSelectedPosting = true
				break
			}
		}

		if !inSelectedPosting {
			routing = append(routing, gtID)
			continue
		}

		if !approxSet[gtID] {
			approx = append(approx, gtID)
			continue
		}

		if !returnedSet[gtID] {
			exact = append(exact, gtID)
			continue
		}

		success = append(success, gtID)
	}

	return
}

// =============================================================================
// MAIN DIAGNOSTIC RUNNER
// =============================================================================

// TestHFreshMuveraDiagnostic is the main diagnostic test runner.
// Run with:
//
//	go test -v -run TestHFreshMuveraDiagnostic ./adapters/repos/db/vector/hfresh/... -args \
//	  -docs=/path/to/docs.jsonl \
//	  -queries=/path/to/queries.jsonl \
//	  -groundtruth=/path/to/gt.jsonl \
//	  -k=10 \
//	  -out=/tmp/report.json
func TestHFreshMuveraDiagnostic(t *testing.T) {
	// Parse flags if not already parsed
	if !flag.Parsed() {
		flag.Parse()
	}

	// Check required flags
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags. " +
			"Run with: go test -v -run TestHFreshMuveraDiagnostic ./adapters/repos/db/vector/hfresh/... -args " +
			"-docs=/path/docs.jsonl -queries=/path/queries.jsonl -groundtruth=/path/gt.jsonl -k=10 -out=/tmp/report.json")
		return
	}

	ctx := context.Background()

	// Initialize output
	output := &DiagnosticOutput{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	output.Config.DocsPath = *flagDocsPath
	output.Config.QueriesPath = *flagQueriesPath
	output.Config.GroundTruthPath = *flagGroundTruthPath
	output.Config.K = *flagK
	output.Config.SearchProbe = *flagSearchProbe
	output.Config.RescoreLimit = *flagRescoreLimit

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err, "failed to load documents")
	t.Logf("Loaded %d documents", len(docs))
	output.NumDocs = len(docs)

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err, "failed to load queries")
	t.Logf("Loaded %d queries", len(queries))
	output.NumQueries = len(queries)

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err, "failed to load ground-truth")
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	// Determine vector dimensions
	if len(docs) > 0 && len(docs[0].Vectors) > 0 {
		output.VectorDims = len(docs[0].Vectors[0])
	}

	// Create index
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, *flagSearchProbe, *flagRescoreLimit, *flagVerbose)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err, "failed to insert doc %d", doc.ID)

		if (i+1)%1000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}

	// Wait for background operations to settle
	time.Sleep(200 * time.Millisecond)

	output.IndexBuildTimeSec = time.Since(insertStart).Seconds()
	output.NumPostings = runner.index.PostingMap.Size()
	t.Logf("Index built in %.2fs, %d postings", output.IndexBuildTimeSec, output.NumPostings)

	// Build posting membership map
	t.Log("Building posting membership map...")
	membership := make(map[uint64][]uint64)
	for postingID, metadata := range runner.index.PostingMap.Iter() {
		for docID := range metadata.Iter() {
			membership[docID] = append(membership[docID], postingID)
		}
	}
	t.Logf("Posting membership built for %d documents", len(membership))

	// Run queries
	t.Log("Running queries...")
	var allRecalls []float64

	for i, query := range queries {
		gt, ok := groundTruth[query.ID]
		if !ok {
			t.Logf("Warning: no ground-truth for query %s, skipping", query.ID)
			continue
		}

		// Limit ground-truth to k
		if len(gt) > *flagK {
			gt = gt[:*flagK]
		}

		// Create trace collector
		collector := NewSearchTraceCollector(query.ID)
		tracedCtx := ContextWithTraceCollector(ctx, collector)

		// Run search
		searchStart := time.Now()
		returnedIDs, _, err := runner.index.SearchByMultiVector(tracedCtx, query.Vectors, *flagK, nil)
		searchDuration := time.Since(searchStart)
		require.NoError(t, err, "search failed for query %s", query.ID)

		// Get trace
		trace := collector.Trace()

		// Classify failures
		routing, approx, exact, success := classifyFailures(
			gt,
			trace.SelectedCentroids,
			trace.ApproxTopIDs,
			returnedIDs,
			membership,
		)

		// Compute recall
		recall := 0.0
		if len(gt) > 0 {
			recall = float64(len(success)) / float64(len(gt))
		}
		allRecalls = append(allRecalls, recall)

		// Build query result
		qr := QueryResult{
			QueryID:           query.ID,
			K:                 *flagK,
			GroundTruthIDs:    gt,
			ReturnedIDs:       returnedIDs,
			SelectedCentroids: trace.SelectedCentroids,
			ApproxTopIDs:      trace.ApproxTopIDs,
			TotalScanned:      trace.ScanStats.TotalScanned,
			UniqueEnumerated:  trace.ScanStats.UniqueEnumerated,
			SkippedDeleted:    trace.ScanStats.SkippedDeleted,
			SkippedDuplicate:  trace.ScanStats.SkippedDuplicate,
			SkippedAllowList:  trace.ScanStats.SkippedAllowList,
			RoutingFailures:   routing,
			ApproxFailures:    approx,
			ExactFailures:     exact,
			Successes:         success,
			Recall:            recall,
			SearchDurationMs:  float64(searchDuration.Microseconds()) / 1000.0,
		}
		output.Queries = append(output.Queries, qr)

		// Update aggregates
		output.TotalGroundTruth += len(gt)
		output.TotalRouting += len(routing)
		output.TotalApprox += len(approx)
		output.TotalExact += len(exact)
		output.TotalSuccesses += len(success)

		if (i+1)%100 == 0 || i == len(queries)-1 {
			t.Logf("Processed %d/%d queries", i+1, len(queries))
		}
	}

	// Compute aggregate metrics
	if output.TotalGroundTruth > 0 {
		output.RoutingFailureRate = float64(output.TotalRouting) / float64(output.TotalGroundTruth)
		output.ApproxFailureRate = float64(output.TotalApprox) / float64(output.TotalGroundTruth)
		output.ExactFailureRate = float64(output.TotalExact) / float64(output.TotalGroundTruth)
		output.SuccessRate = float64(output.TotalSuccesses) / float64(output.TotalGroundTruth)
		output.OverallRecall = output.SuccessRate
	}

	if len(allRecalls) > 0 {
		// Average recall
		sum := 0.0
		for _, r := range allRecalls {
			sum += r
		}
		output.AverageRecall = sum / float64(len(allRecalls))

		// Sort for percentiles
		sorted := make([]float64, len(allRecalls))
		copy(sorted, allRecalls)
		sort.Float64s(sorted)

		output.MinRecall = sorted[0]
		output.MaxRecall = sorted[len(sorted)-1]
		output.MedianRecall = sorted[len(sorted)/2]

		// Recall@N (percentage of queries with recall >= N/K)
		recallAt := func(n int) float64 {
			threshold := float64(n) / float64(*flagK)
			count := 0
			for _, r := range allRecalls {
				if r >= threshold {
					count++
				}
			}
			return float64(count) / float64(len(allRecalls))
		}
		output.RecallAt1 = recallAt(1)
		output.RecallAt5 = recallAt(5)
		output.RecallAt10 = recallAt(10)
	}

	// Print summary
	t.Log("")
	t.Log("=============================================================================")
	t.Log("                    HFRESH+MUVERA RECALL ATTRIBUTION REPORT")
	t.Log("=============================================================================")
	t.Log("")
	t.Logf("Dataset: %d documents, %d queries, %d dimensions", output.NumDocs, output.NumQueries, output.VectorDims)
	t.Logf("Index:   %d postings, built in %.2fs", output.NumPostings, output.IndexBuildTimeSec)
	t.Logf("Config:  k=%d, searchProbe=%d, rescoreLimit=%d", *flagK, *flagSearchProbe, *flagRescoreLimit)
	t.Log("")
	t.Log("FAILURE ATTRIBUTION:")
	t.Logf("  Total Ground Truth:    %d", output.TotalGroundTruth)
	t.Logf("  Routing Failures:      %d (%.2f%%)", output.TotalRouting, output.RoutingFailureRate*100)
	t.Logf("  Approx Failures:       %d (%.2f%%)", output.TotalApprox, output.ApproxFailureRate*100)
	t.Logf("  Exact Failures:        %d (%.2f%%)", output.TotalExact, output.ExactFailureRate*100)
	t.Logf("  Successes:             %d (%.2f%%)", output.TotalSuccesses, output.SuccessRate*100)
	t.Log("")
	t.Log("RECALL METRICS:")
	t.Logf("  Overall Recall:        %.4f", output.OverallRecall)
	t.Logf("  Average Query Recall:  %.4f", output.AverageRecall)
	t.Logf("  Median Query Recall:   %.4f", output.MedianRecall)
	t.Logf("  Min Query Recall:      %.4f", output.MinRecall)
	t.Logf("  Max Query Recall:      %.4f", output.MaxRecall)
	t.Logf("  Recall@1:              %.4f", output.RecallAt1)
	t.Logf("  Recall@5:              %.4f", output.RecallAt5)
	t.Logf("  Recall@10:             %.4f", output.RecallAt10)
	t.Log("")
	t.Log("=============================================================================")

	// Write output file
	if *flagOutputPath != "" {
		data, err := json.MarshalIndent(output, "", "  ")
		require.NoError(t, err, "failed to marshal output")

		err = os.WriteFile(*flagOutputPath, data, 0644)
		require.NoError(t, err, "failed to write output file")

		t.Logf("Report written to: %s", *flagOutputPath)
	}
}

// =============================================================================
// SAMPLE DATA GENERATOR
// =============================================================================

// TestGenerateSampleData generates sample input files for testing the diagnostic runner.
// Run with:
//
//	go test -v -run TestGenerateSampleData ./adapters/repos/db/vector/hfresh/... -args \
//	  -out=/tmp/sample_data
func TestGenerateSampleData(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	outDir := *flagOutputPath
	if outDir == "" {
		outDir = t.TempDir()
	}

	const (
		numDocs       = 100
		numQueries    = 10
		vectorsPerDoc = 4
		dims          = 128
		k             = 10
	)

	// Generate docs
	docsPath := outDir + "/docs.jsonl"
	docsFile, err := os.Create(docsPath)
	require.NoError(t, err)

	for i := 0; i < numDocs; i++ {
		doc := DocInput{
			ID:      uint64(i),
			Vectors: make([][]float32, vectorsPerDoc),
		}
		for j := 0; j < vectorsPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = float32(i*1000+j*100+k) / 100000.0
			}
			doc.Vectors[j] = vec
		}
		data, _ := json.Marshal(doc)
		docsFile.Write(data)
		docsFile.WriteString("\n")
	}
	docsFile.Close()
	t.Logf("Generated docs: %s", docsPath)

	// Generate queries (use first few docs as queries for testing)
	queriesPath := outDir + "/queries.jsonl"
	queriesFile, err := os.Create(queriesPath)
	require.NoError(t, err)

	for i := 0; i < numQueries; i++ {
		query := QueryInput{
			ID:      fmt.Sprintf("q%d", i),
			Vectors: make([][]float32, vectorsPerDoc),
		}
		for j := 0; j < vectorsPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				vec[k] = float32(i*1000+j*100+k) / 100000.0
			}
			query.Vectors[j] = vec
		}
		data, _ := json.Marshal(query)
		queriesFile.Write(data)
		queriesFile.WriteString("\n")
	}
	queriesFile.Close()
	t.Logf("Generated queries: %s", queriesPath)

	// Generate ground-truth (for synthetic data, GT is the query doc itself + neighbors)
	gtPath := outDir + "/gt.jsonl"
	gtFile, err := os.Create(gtPath)
	require.NoError(t, err)

	for i := 0; i < numQueries; i++ {
		gt := GroundTruthInput{
			QueryID: fmt.Sprintf("q%d", i),
			DocIDs:  make([]uint64, k),
		}
		for j := 0; j < k; j++ {
			// Ground truth: the query doc itself + next docs
			gt.DocIDs[j] = uint64((i + j) % numDocs)
		}
		data, _ := json.Marshal(gt)
		gtFile.Write(data)
		gtFile.WriteString("\n")
	}
	gtFile.Close()
	t.Logf("Generated ground-truth: %s", gtPath)

	t.Log("")
	t.Log("Sample data generated. Run diagnostic with:")
	t.Logf("  go test -v -run TestHFreshMuveraDiagnostic ./adapters/repos/db/vector/hfresh/... -args \\")
	t.Logf("    -docs=%s \\", docsPath)
	t.Logf("    -queries=%s \\", queriesPath)
	t.Logf("    -groundtruth=%s \\", gtPath)
	t.Logf("    -k=10 -out=%s/report.json", outDir)
}

// =============================================================================
// PROBE SWEEP TEST
// =============================================================================

// ProbeSweepResult contains results for a single searchProbe value.
type ProbeSweepResult struct {
	SearchProbe          int     `json:"search_probe"`
	Recall               float64 `json:"recall"`
	RoutingFailureRate   float64 `json:"routing_failure_rate"`
	ApproxFailureRate    float64 `json:"approx_failure_rate"`
	ExactFailureRate     float64 `json:"exact_failure_rate"`
	AvgQueryMs           float64 `json:"avg_query_ms"`
	AvgSelectedCentroids float64 `json:"avg_selected_centroids"`
	AvgScannedVectors    float64 `json:"avg_scanned_vectors"`
	TotalGroundTruth     int     `json:"total_ground_truth"`
	RoutingFailures      int     `json:"routing_failures"`
	ApproxFailures       int     `json:"approx_failures"`
	ExactFailures        int     `json:"exact_failures"`
	Successes            int     `json:"successes"`
}

// ProbeSweepReport contains the full sweep comparison.
type ProbeSweepReport struct {
	Config struct {
		DocsPath        string `json:"docs_path"`
		QueriesPath     string `json:"queries_path"`
		GroundTruthPath string `json:"ground_truth_path"`
		K               int    `json:"k"`
		RescoreLimit    int    `json:"rescore_limit"`
		SearchProbes    []int  `json:"search_probes"`
	} `json:"config"`
	NumDocs          int                `json:"num_docs"`
	NumQueries       int                `json:"num_queries"`
	NumPostings      int                `json:"num_postings"`
	IndexBuildTimeSec float64           `json:"index_build_time_sec"`
	Results          []ProbeSweepResult `json:"results"`
	Timestamp        string             `json:"timestamp"`
}

// TestHFreshMuveraDiagnosticProbeSweep runs the diagnostic over multiple searchProbe values.
// The index is built once and reused for all searchProbe values.
//
// Run with:
//
//	go test -v -run TestHFreshMuveraDiagnosticProbeSweep ./adapters/repos/db/vector/hfresh/... -args \
//	  -docs=/tmp/hfresh_diag_lotte_100q/docs.jsonl \
//	  -queries=/tmp/hfresh_diag_lotte_100q/queries.jsonl \
//	  -groundtruth=/tmp/hfresh_diag_lotte_100q/gt.jsonl \
//	  -k=100 \
//	  -rescorelimit=100 \
//	  -searchprobe-range=64,128,256,512 \
//	  -out=/tmp/hfresh_diag_lotte_100q/probe_sweep.json
func TestHFreshMuveraDiagnosticProbeSweep(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	// Check required flags
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
		return
	}

	if *flagSearchProbeRange == "" {
		t.Skip("Skipping: requires -searchprobe-range flag (e.g., -searchprobe-range=64,128,256,512)")
		return
	}

	// Parse searchProbe range
	probeStrs := strings.Split(*flagSearchProbeRange, ",")
	var probeValues []int
	for _, s := range probeStrs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		v, err := strconv.Atoi(s)
		require.NoError(t, err, "invalid searchprobe value: %s", s)
		probeValues = append(probeValues, v)
	}
	require.NotEmpty(t, probeValues, "no valid searchprobe values in range")

	ctx := context.Background()

	// Initialize report
	report := &ProbeSweepReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	report.Config.DocsPath = *flagDocsPath
	report.Config.QueriesPath = *flagQueriesPath
	report.Config.GroundTruthPath = *flagGroundTruthPath
	report.Config.K = *flagK
	report.Config.RescoreLimit = *flagRescoreLimit
	report.Config.SearchProbes = probeValues

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))
	report.NumDocs = len(docs)

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))
	report.NumQueries = len(queries)

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	// Create index with initial searchProbe (will be changed per sweep)
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, probeValues[0], *flagRescoreLimit, *flagVerbose)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}

	// Wait for background operations
	time.Sleep(500 * time.Millisecond)

	report.IndexBuildTimeSec = time.Since(insertStart).Seconds()
	report.NumPostings = runner.index.PostingMap.Size()
	t.Logf("Index built in %.2fs, %d postings", report.IndexBuildTimeSec, report.NumPostings)

	// Build posting membership map (once)
	t.Log("Building posting membership map...")
	membership := make(map[uint64][]uint64)
	for postingID, metadata := range runner.index.PostingMap.Iter() {
		for docID := range metadata.Iter() {
			membership[docID] = append(membership[docID], postingID)
		}
	}
	t.Logf("Posting membership built for %d documents", len(membership))

	// Run sweep for each searchProbe value
	t.Log("")
	t.Log("Starting searchProbe sweep...")
	t.Log("")

	for _, probeValue := range probeValues {
		t.Logf("=== Running with searchProbe=%d ===", probeValue)

		// Update searchProbe atomically
		atomic.StoreUint32(&runner.index.searchProbe, uint32(probeValue))

		// Run all queries
		result := ProbeSweepResult{
			SearchProbe: probeValue,
		}

		var totalQueryTimeMs float64
		var totalSelectedCentroids int
		var totalScannedVectors int

		for _, query := range queries {
			gt, ok := groundTruth[query.ID]
			if !ok {
				continue
			}

			// Limit ground-truth to k
			if len(gt) > *flagK {
				gt = gt[:*flagK]
			}

			// Run traced search
			collector := NewSearchTraceCollector(query.ID)
			tracedCtx := ContextWithTraceCollector(ctx, collector)

			searchStart := time.Now()
			returnedIDs, _, err := runner.index.SearchByMultiVector(tracedCtx, query.Vectors, *flagK, nil)
			searchDuration := time.Since(searchStart)
			require.NoError(t, err)

			trace := collector.Trace()

			// Classify failures
			routing, approx, exact, success := classifyFailures(
				gt,
				trace.SelectedCentroids,
				trace.ApproxTopIDs,
				returnedIDs,
				membership,
			)

			// Accumulate stats
			result.TotalGroundTruth += len(gt)
			result.RoutingFailures += len(routing)
			result.ApproxFailures += len(approx)
			result.ExactFailures += len(exact)
			result.Successes += len(success)

			totalQueryTimeMs += float64(searchDuration.Microseconds()) / 1000.0
			totalSelectedCentroids += len(trace.SelectedCentroids)
			totalScannedVectors += trace.ScanStats.TotalScanned
		}

		// Compute rates
		numQueries := float64(len(queries))
		if result.TotalGroundTruth > 0 {
			result.Recall = float64(result.Successes) / float64(result.TotalGroundTruth)
			result.RoutingFailureRate = float64(result.RoutingFailures) / float64(result.TotalGroundTruth)
			result.ApproxFailureRate = float64(result.ApproxFailures) / float64(result.TotalGroundTruth)
			result.ExactFailureRate = float64(result.ExactFailures) / float64(result.TotalGroundTruth)
		}
		if numQueries > 0 {
			result.AvgQueryMs = totalQueryTimeMs / numQueries
			result.AvgSelectedCentroids = float64(totalSelectedCentroids) / numQueries
			result.AvgScannedVectors = float64(totalScannedVectors) / numQueries
		}

		report.Results = append(report.Results, result)

		t.Logf("  Recall: %.4f | Routing: %.2f%% | Approx: %.2f%% | Exact: %.2f%% | Avg centroids: %.1f | Avg scanned: %.0f | Avg ms: %.2f",
			result.Recall,
			result.RoutingFailureRate*100,
			result.ApproxFailureRate*100,
			result.ExactFailureRate*100,
			result.AvgSelectedCentroids,
			result.AvgScannedVectors,
			result.AvgQueryMs)
	}

	// Print summary table
	t.Log("")
	t.Log("================================================================================")
	t.Log("                         SEARCHPROBE SWEEP RESULTS")
	t.Log("================================================================================")
	t.Log("")
	t.Logf("%-12s | %-8s | %-12s | %-12s | %-12s | %-10s | %-12s | %-12s",
		"searchProbe", "recall", "routing_%", "approx_%", "exact_%", "avg_ms", "avg_centroids", "avg_scanned")
	t.Log("-------------|----------|--------------|--------------|--------------|------------|--------------|-------------")

	for _, r := range report.Results {
		t.Logf("%-12d | %-8.4f | %-12.2f | %-12.2f | %-12.2f | %-10.2f | %-12.1f | %-12.0f",
			r.SearchProbe,
			r.Recall,
			r.RoutingFailureRate*100,
			r.ApproxFailureRate*100,
			r.ExactFailureRate*100,
			r.AvgQueryMs,
			r.AvgSelectedCentroids,
			r.AvgScannedVectors)
	}

	t.Log("")
	t.Log("================================================================================")

	// Write output file
	if *flagOutputPath != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		require.NoError(t, err)

		err = os.WriteFile(*flagOutputPath, data, 0644)
		require.NoError(t, err)

		t.Logf("Report written to: %s", *flagOutputPath)
	}
}

// =============================================================================
// COMBINED SEARCHPROBE × RESCORELIMIT SWEEP
// =============================================================================

// SweepConfig represents a single searchProbe × rescoreLimit configuration.
type SweepConfig struct {
	SearchProbe  int `json:"search_probe"`
	RescoreLimit int `json:"rescore_limit"`
}

// CombinedSweepResult contains results for a single searchProbe × rescoreLimit configuration.
type CombinedSweepResult struct {
	SearchProbe  int `json:"search_probe"`
	RescoreLimit int `json:"rescore_limit"`

	// Core metrics
	Recall             float64 `json:"recall"`
	RoutingFailureRate float64 `json:"routing_failure_rate"`
	ApproxFailureRate  float64 `json:"approx_failure_rate"`
	ExactFailureRate   float64 `json:"exact_failure_rate"`

	// Timing
	AvgQueryMs float64 `json:"avg_query_ms"`

	// Scan statistics
	AvgSelectedCentroids float64 `json:"avg_selected_centroids"`
	AvgScannedVectors    float64 `json:"avg_scanned_vectors"`
	AvgCandidatesRescore float64 `json:"avg_candidates_rescore"`

	// Derived metric: rescore utilization = candidates_rescore / scanned_vectors
	RescoreUtilization float64 `json:"rescore_utilization"`

	// Raw counts
	TotalGroundTruth int `json:"total_ground_truth"`
	RoutingFailures  int `json:"routing_failures"`
	ApproxFailures   int `json:"approx_failures"`
	ExactFailures    int `json:"exact_failures"`
	Successes        int `json:"successes"`
}

// CombinedSweepReport contains the full combined sweep comparison.
type CombinedSweepReport struct {
	Config struct {
		DocsPath        string        `json:"docs_path"`
		QueriesPath     string        `json:"queries_path"`
		GroundTruthPath string        `json:"ground_truth_path"`
		K               int           `json:"k"`
		Configs         []SweepConfig `json:"configs"`
	} `json:"config"`
	NumDocs           int                   `json:"num_docs"`
	NumQueries        int                   `json:"num_queries"`
	NumPostings       int                   `json:"num_postings"`
	IndexBuildTimeSec float64               `json:"index_build_time_sec"`
	Results           []CombinedSweepResult `json:"results"`
	Timestamp         string                `json:"timestamp"`
}

// TestHFreshMuveraDiagnosticCombinedSweep runs the diagnostic over multiple
// searchProbe × rescoreLimit configurations. The index is built once and reused.
//
// Run with:
//
//	go test -v -run TestHFreshMuveraDiagnosticCombinedSweep ./adapters/repos/db/vector/hfresh/... -args \
//	  -docs=/tmp/hfresh_diag_lotte_100q/docs.jsonl \
//	  -queries=/tmp/hfresh_diag_lotte_100q/queries.jsonl \
//	  -groundtruth=/tmp/hfresh_diag_lotte_100q/gt.jsonl \
//	  -k=100 \
//	  -sweep-configs=64:100,128:256,256:512,512:1024,512:2048 \
//	  -out=/tmp/hfresh_diag_lotte_100q/combined_sweep.json
func TestHFreshMuveraDiagnosticCombinedSweep(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	// Check required flags
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
		return
	}

	if *flagSweepConfigs == "" {
		t.Skip("Skipping: requires -sweep-configs flag (e.g., -sweep-configs=64:100,128:256,256:512,512:1024)")
		return
	}

	// Parse sweep configs
	configStrs := strings.Split(*flagSweepConfigs, ",")
	var configs []SweepConfig
	for _, s := range configStrs {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		parts := strings.Split(s, ":")
		require.Len(t, parts, 2, "invalid config format: %s (expected probe:rescore)", s)

		probe, err := strconv.Atoi(strings.TrimSpace(parts[0]))
		require.NoError(t, err, "invalid searchProbe in config: %s", s)

		rescore, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		require.NoError(t, err, "invalid rescoreLimit in config: %s", s)

		configs = append(configs, SweepConfig{
			SearchProbe:  probe,
			RescoreLimit: rescore,
		})
	}
	require.NotEmpty(t, configs, "no valid configs in sweep-configs")

	ctx := context.Background()

	// Initialize report
	report := &CombinedSweepReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	report.Config.DocsPath = *flagDocsPath
	report.Config.QueriesPath = *flagQueriesPath
	report.Config.GroundTruthPath = *flagGroundTruthPath
	report.Config.K = *flagK
	report.Config.Configs = configs

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))
	report.NumDocs = len(docs)

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))
	report.NumQueries = len(queries)

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	// Create index with first config's searchProbe and rescoreLimit
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, configs[0].SearchProbe, configs[0].RescoreLimit, *flagVerbose)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}

	// Wait for background operations
	time.Sleep(500 * time.Millisecond)

	report.IndexBuildTimeSec = time.Since(insertStart).Seconds()
	report.NumPostings = runner.index.PostingMap.Size()
	t.Logf("Index built in %.2fs, %d postings", report.IndexBuildTimeSec, report.NumPostings)

	// Build posting membership map (once)
	t.Log("Building posting membership map...")
	membership := make(map[uint64][]uint64)
	for postingID, metadata := range runner.index.PostingMap.Iter() {
		for docID := range metadata.Iter() {
			membership[docID] = append(membership[docID], postingID)
		}
	}
	t.Logf("Posting membership built for %d documents", len(membership))

	// Run sweep for each configuration
	t.Log("")
	t.Log("Starting combined searchProbe × rescoreLimit sweep...")
	t.Log("")

	for _, cfg := range configs {
		t.Logf("=== Running with searchProbe=%d, rescoreLimit=%d ===", cfg.SearchProbe, cfg.RescoreLimit)

		// Update searchProbe and rescoreLimit atomically
		atomic.StoreUint32(&runner.index.searchProbe, uint32(cfg.SearchProbe))
		atomic.StoreUint32(&runner.index.rescoreLimit, uint32(cfg.RescoreLimit))

		// Run all queries
		result := CombinedSweepResult{
			SearchProbe:  cfg.SearchProbe,
			RescoreLimit: cfg.RescoreLimit,
		}

		var totalQueryTimeMs float64
		var totalSelectedCentroids int
		var totalScannedVectors int
		var totalCandidatesRescore int

		for _, query := range queries {
			gt, ok := groundTruth[query.ID]
			if !ok {
				continue
			}

			// Limit ground-truth to k
			if len(gt) > *flagK {
				gt = gt[:*flagK]
			}

			// Run traced search
			collector := NewSearchTraceCollector(query.ID)
			tracedCtx := ContextWithTraceCollector(ctx, collector)

			searchStart := time.Now()
			returnedIDs, _, err := runner.index.SearchByMultiVector(tracedCtx, query.Vectors, *flagK, nil)
			searchDuration := time.Since(searchStart)
			require.NoError(t, err)

			trace := collector.Trace()

			// Classify failures
			routing, approx, exact, success := classifyFailures(
				gt,
				trace.SelectedCentroids,
				trace.ApproxTopIDs,
				returnedIDs,
				membership,
			)

			// Accumulate stats
			result.TotalGroundTruth += len(gt)
			result.RoutingFailures += len(routing)
			result.ApproxFailures += len(approx)
			result.ExactFailures += len(exact)
			result.Successes += len(success)

			totalQueryTimeMs += float64(searchDuration.Microseconds()) / 1000.0
			totalSelectedCentroids += len(trace.SelectedCentroids)
			totalScannedVectors += trace.ScanStats.TotalScanned
			totalCandidatesRescore += len(trace.ApproxTopIDs)
		}

		// Compute rates
		numQueries := float64(len(queries))
		if result.TotalGroundTruth > 0 {
			result.Recall = float64(result.Successes) / float64(result.TotalGroundTruth)
			result.RoutingFailureRate = float64(result.RoutingFailures) / float64(result.TotalGroundTruth)
			result.ApproxFailureRate = float64(result.ApproxFailures) / float64(result.TotalGroundTruth)
			result.ExactFailureRate = float64(result.ExactFailures) / float64(result.TotalGroundTruth)
		}
		if numQueries > 0 {
			result.AvgQueryMs = totalQueryTimeMs / numQueries
			result.AvgSelectedCentroids = float64(totalSelectedCentroids) / numQueries
			result.AvgScannedVectors = float64(totalScannedVectors) / numQueries
			result.AvgCandidatesRescore = float64(totalCandidatesRescore) / numQueries
		}

		// Compute rescore utilization
		if result.AvgScannedVectors > 0 {
			result.RescoreUtilization = result.AvgCandidatesRescore / result.AvgScannedVectors
		}

		report.Results = append(report.Results, result)

		t.Logf("  Recall: %.4f | Routing: %.2f%% | Approx: %.2f%% | Exact: %.2f%% | Rescore util: %.4f | Avg ms: %.2f",
			result.Recall,
			result.RoutingFailureRate*100,
			result.ApproxFailureRate*100,
			result.ExactFailureRate*100,
			result.RescoreUtilization,
			result.AvgQueryMs)
	}

	// Print summary table
	t.Log("")
	t.Log("================================================================================")
	t.Log("                   COMBINED SEARCHPROBE × RESCORELIMIT SWEEP")
	t.Log("================================================================================")
	t.Log("")
	t.Logf("%-8s | %-8s | %-8s | %-10s | %-10s | %-10s | %-12s | %-12s | %-12s | %-10s",
		"probe", "rescore", "recall", "routing_%", "approx_%", "exact_%", "rescore_util", "avg_rescore", "avg_scanned", "avg_ms")
	t.Log("---------|----------|----------|------------|------------|------------|--------------|--------------|--------------|----------")

	for _, r := range report.Results {
		t.Logf("%-8d | %-8d | %-8.4f | %-10.2f | %-10.2f | %-10.2f | %-12.4f | %-12.1f | %-12.0f | %-10.2f",
			r.SearchProbe,
			r.RescoreLimit,
			r.Recall,
			r.RoutingFailureRate*100,
			r.ApproxFailureRate*100,
			r.ExactFailureRate*100,
			r.RescoreUtilization,
			r.AvgCandidatesRescore,
			r.AvgScannedVectors,
			r.AvgQueryMs)
	}

	t.Log("")
	t.Log("================================================================================")

	// Write output file
	if *flagOutputPath != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		require.NoError(t, err)

		err = os.WriteFile(*flagOutputPath, data, 0644)
		require.NoError(t, err)

		t.Logf("Report written to: %s", *flagOutputPath)
	}
}

// =============================================================================
// POSTING STRUCTURE ANALYSIS
// =============================================================================

// PostingCoveragePoint represents GT coverage at a specific posting depth.
type PostingCoveragePoint struct {
	PostingDepth int     `json:"posting_depth"`
	GTCoverage   float64 `json:"gt_coverage"`
	GTDocsFound  int     `json:"gt_docs_found"`
	TotalGTDocs  int     `json:"total_gt_docs"`
}

// PostingDiversityStats contains diversity metrics for selected postings.
type PostingDiversityStats struct {
	NumPostingsSelected   int     `json:"num_postings_selected"`
	TotalVectorsScanned   int     `json:"total_vectors_scanned"`
	UniqueDocsScanned     int     `json:"unique_docs_scanned"`
	AvgDocsPerPosting     float64 `json:"avg_docs_per_posting"`
	P50DocsPerPosting     int     `json:"p50_docs_per_posting"`
	P95DocsPerPosting     int     `json:"p95_docs_per_posting"`
	MaxDocsPerPosting     int     `json:"max_docs_per_posting"`
	DuplicateRate         float64 `json:"duplicate_rate"`
	GTDocFraction         float64 `json:"gt_doc_fraction"`
	GTDocsInSelectedPosts int     `json:"gt_docs_in_selected_posts"`
}

// GTPostingDistribution contains info about where GT docs live in posting space.
type GTPostingDistribution struct {
	AvgDistinctGTPostings float64 `json:"avg_distinct_gt_postings"`
	AvgMinGTPostingRank   float64 `json:"avg_min_gt_posting_rank"`
	P50MinGTPostingRank   int     `json:"p50_min_gt_posting_rank"`
	P95MinGTPostingRank   int     `json:"p95_min_gt_posting_rank"`
	MaxMinGTPostingRank   int     `json:"max_min_gt_posting_rank"`
	// Distribution of GT posting ranks
	GTPostingRankP50 int `json:"gt_posting_rank_p50"`
	GTPostingRankP95 int `json:"gt_posting_rank_p95"`
}

// StageWiseRecall contains recall at each pipeline stage.
type StageWiseRecall struct {
	SearchProbe     int     `json:"search_probe"`
	RescoreLimit    int     `json:"rescore_limit"`
	ReachableRecall float64 `json:"reachable_recall"`
	ApproxTopRecall float64 `json:"approx_top_recall"`
	FinalRecall     float64 `json:"final_recall"`
}

// QueryPostingAnalysis contains per-query posting structure analysis.
type QueryPostingAnalysis struct {
	QueryID             string `json:"query_id"`
	NumGTDocs           int    `json:"num_gt_docs"`
	DistinctGTPostings  int    `json:"distinct_gt_postings"`
	MinGTPostingRank    int    `json:"min_gt_posting_rank"`
	MedianGTPostingRank int    `json:"median_gt_posting_rank"`
	MaxGTPostingRank    int    `json:"max_gt_posting_rank"`
	// Coverage at various depths
	Coverage1    float64 `json:"coverage_1"`
	Coverage8    float64 `json:"coverage_8"`
	Coverage64   float64 `json:"coverage_64"`
	Coverage256  float64 `json:"coverage_256"`
	Coverage512  float64 `json:"coverage_512"`
	Coverage1024 float64 `json:"coverage_1024"`
}

// PostingStructureReport contains the full posting structure analysis.
type PostingStructureReport struct {
	Config struct {
		DocsPath        string `json:"docs_path"`
		QueriesPath     string `json:"queries_path"`
		GroundTruthPath string `json:"ground_truth_path"`
		K               int    `json:"k"`
	} `json:"config"`
	NumDocs     int `json:"num_docs"`
	NumQueries  int `json:"num_queries"`
	NumPostings int `json:"num_postings"`

	// Aggregated coverage curve (average over queries)
	CoverageCurve []PostingCoveragePoint `json:"coverage_curve"`

	// Posting diversity at various searchProbe levels
	DiversityAtProbe64  PostingDiversityStats `json:"diversity_at_probe_64"`
	DiversityAtProbe256 PostingDiversityStats `json:"diversity_at_probe_256"`
	DiversityAtProbe512 PostingDiversityStats `json:"diversity_at_probe_512"`

	// GT posting distribution
	GTDistribution GTPostingDistribution `json:"gt_distribution"`

	// Stage-wise recall at various configurations
	StageRecalls []StageWiseRecall `json:"stage_recalls"`

	// Per-query analysis (optional, can be large)
	PerQueryAnalysis []QueryPostingAnalysis `json:"per_query_analysis,omitempty"`

	Timestamp string `json:"timestamp"`
}

// TestHFreshPostingStructureAnalysis analyzes the posting structure to understand
// why MUVERA/FDE may need high searchProbe/rescoreLimit.
//
// Run with:
//
//	go test -v -timeout 30m -run TestHFreshPostingStructureAnalysis ./adapters/repos/db/vector/hfresh/... -args \
//	  -docs=/tmp/hfresh_diag_lotte_100q/docs.jsonl \
//	  -queries=/tmp/hfresh_diag_lotte_100q/queries.jsonl \
//	  -groundtruth=/tmp/hfresh_diag_lotte_100q/gt.jsonl \
//	  -k=100 \
//	  -out=/tmp/hfresh_diag_lotte_100q/posting_structure.json
func TestHFreshPostingStructureAnalysis(t *testing.T) {
	if !flag.Parsed() {
		flag.Parse()
	}

	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
		return
	}

	ctx := context.Background()

	// Initialize report
	report := &PostingStructureReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	report.Config.DocsPath = *flagDocsPath
	report.Config.QueriesPath = *flagQueriesPath
	report.Config.GroundTruthPath = *flagGroundTruthPath
	report.Config.K = *flagK

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))
	report.NumDocs = len(docs)

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))
	report.NumQueries = len(queries)

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	// Create index with searchProbe=1024 to get full centroid rankings
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, 1024, 100, *flagVerbose)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(500 * time.Millisecond)
	t.Logf("Index built in %.2fs", time.Since(insertStart).Seconds())

	report.NumPostings = runner.index.PostingMap.Size()
	t.Logf("Total postings: %d", report.NumPostings)

	// Build posting membership map: docID -> list of postingIDs
	t.Log("Building posting membership map...")
	docToPostings := make(map[uint64][]uint64)
	postingToDocCount := make(map[uint64]int)
	for postingID, metadata := range runner.index.PostingMap.Iter() {
		docCount := 0
		for docID := range metadata.Iter() {
			docToPostings[docID] = append(docToPostings[docID], postingID)
			docCount++
		}
		postingToDocCount[postingID] = docCount
	}
	t.Logf("Posting membership built for %d documents", len(docToPostings))

	// Posting coverage depths to analyze
	coverageDepths := []int{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}

	// Initialize coverage accumulators
	coverageSums := make([]float64, len(coverageDepths))
	coverageCounts := make([]int, len(coverageDepths))

	// Per-query GT posting rank data
	var allMinGTRanks []int
	var allDistinctGTPostings []int

	// Stage-wise recall accumulators for different configs
	stageConfigs := []struct {
		probe   int
		rescore int
	}{
		{64, 100},
		{64, 350},
		{256, 512},
		{512, 1024},
		{512, 2048},
	}
	stageReachable := make([]int, len(stageConfigs))
	stageApprox := make([]int, len(stageConfigs))
	stageFinal := make([]int, len(stageConfigs))
	stageTotalGT := make([]int, len(stageConfigs))

	// Diversity accumulators at key probe levels
	probeDepthsForDiversity := []int{64, 256, 512}
	diversityStats := make([]struct {
		totalVectors  int
		uniqueDocs    int
		gtDocs        int
		postingSizes  []int
		duplicateVecs int
		queryCount    int
	}, len(probeDepthsForDiversity))

	t.Log("")
	t.Log("Analyzing posting structure...")
	t.Log("")

	for qi, query := range queries {
		gt, ok := groundTruth[query.ID]
		if !ok {
			continue
		}
		if len(gt) > *flagK {
			gt = gt[:*flagK]
		}

		// Build GT doc set for this query
		gtSet := make(map[uint64]bool)
		for _, id := range gt {
			gtSet[id] = true
		}

		// Encode query and get full centroid ranking
		queryFlat := runner.index.muveraEncoder.EncodeQuery(query.Vectors)

		// Get ALL centroids ranked by distance
		allCentroids, err := runner.index.Centroids.Search(queryFlat, report.NumPostings, nil)
		require.NoError(t, err)

		// Build centroid rank map
		centroidRank := make(map[uint64]int)
		for i, c := range allCentroids.data {
			centroidRank[c.ID] = i + 1 // 1-indexed rank
		}

		// For each GT doc, find its best (lowest) posting rank
		var gtPostingRanks []int
		gtPostingSet := make(map[uint64]bool)

		for gtDocID := range gtSet {
			postings, ok := docToPostings[gtDocID]
			if !ok {
				continue
			}

			bestRank := report.NumPostings + 1
			for _, postingID := range postings {
				if rank, ok := centroidRank[postingID]; ok && rank < bestRank {
					bestRank = rank
				}
				gtPostingSet[postingID] = true
			}
			if bestRank <= report.NumPostings {
				gtPostingRanks = append(gtPostingRanks, bestRank)
			}
		}

		if len(gtPostingRanks) > 0 {
			sort.Ints(gtPostingRanks)
			allMinGTRanks = append(allMinGTRanks, gtPostingRanks[0]) // min rank for this query
			allDistinctGTPostings = append(allDistinctGTPostings, len(gtPostingSet))
		}

		// Compute coverage at each depth
		perQueryAnalysis := QueryPostingAnalysis{
			QueryID:            query.ID,
			NumGTDocs:          len(gtSet),
			DistinctGTPostings: len(gtPostingSet),
		}

		if len(gtPostingRanks) > 0 {
			perQueryAnalysis.MinGTPostingRank = gtPostingRanks[0]
			perQueryAnalysis.MedianGTPostingRank = gtPostingRanks[len(gtPostingRanks)/2]
			perQueryAnalysis.MaxGTPostingRank = gtPostingRanks[len(gtPostingRanks)-1]
		}

		for di, depth := range coverageDepths {
			if depth > len(allCentroids.data) {
				depth = len(allCentroids.data)
			}

			// Get postings at this depth
			selectedPostingIDs := make([]uint64, 0, depth)
			for i := 0; i < depth && i < len(allCentroids.data); i++ {
				selectedPostingIDs = append(selectedPostingIDs, allCentroids.data[i].ID)
			}

			// Find GT docs reachable at this depth
			gtFound := 0
			for gtDocID := range gtSet {
				postings, ok := docToPostings[gtDocID]
				if !ok {
					continue
				}
				for _, postingID := range postings {
					found := false
					for _, selID := range selectedPostingIDs {
						if postingID == selID {
							found = true
							break
						}
					}
					if found {
						gtFound++
						break
					}
				}
			}

			coverage := float64(gtFound) / float64(len(gtSet))
			coverageSums[di] += coverage
			coverageCounts[di]++

			// Store in per-query analysis
			switch depth {
			case 1:
				perQueryAnalysis.Coverage1 = coverage
			case 8:
				perQueryAnalysis.Coverage8 = coverage
			case 64:
				perQueryAnalysis.Coverage64 = coverage
			case 256:
				perQueryAnalysis.Coverage256 = coverage
			case 512:
				perQueryAnalysis.Coverage512 = coverage
			case 1024:
				perQueryAnalysis.Coverage1024 = coverage
			}
		}

		// Compute diversity metrics at key probe levels
		for di, probeDepth := range probeDepthsForDiversity {
			if probeDepth > len(allCentroids.data) {
				probeDepth = len(allCentroids.data)
			}

			selectedPostingIDs := make([]uint64, 0, probeDepth)
			for i := 0; i < probeDepth && i < len(allCentroids.data); i++ {
				selectedPostingIDs = append(selectedPostingIDs, allCentroids.data[i].ID)
			}

			// Get posting sizes and compute diversity
			totalVecs := 0
			uniqueDocs := make(map[uint64]bool)
			gtDocsFound := 0

			for _, postingID := range selectedPostingIDs {
				metadata, err := runner.index.PostingMap.Get(ctx, postingID)
				if err != nil || metadata == nil {
					continue
				}
				postingSize := 0
				for docID := range metadata.Iter() {
					if !uniqueDocs[docID] {
						uniqueDocs[docID] = true
						if gtSet[docID] {
							gtDocsFound++
						}
					}
					postingSize++
					totalVecs++
				}
				diversityStats[di].postingSizes = append(diversityStats[di].postingSizes, postingSize)
			}

			diversityStats[di].totalVectors += totalVecs
			diversityStats[di].uniqueDocs += len(uniqueDocs)
			diversityStats[di].gtDocs += gtDocsFound
			diversityStats[di].duplicateVecs += totalVecs - len(uniqueDocs)
			diversityStats[di].queryCount++
		}

		// Stage-wise recall analysis
		for si, cfg := range stageConfigs {
			probeDepth := cfg.probe
			if probeDepth > len(allCentroids.data) {
				probeDepth = len(allCentroids.data)
			}

			selectedPostingIDs := make([]uint64, 0, probeDepth)
			for i := 0; i < probeDepth && i < len(allCentroids.data); i++ {
				selectedPostingIDs = append(selectedPostingIDs, allCentroids.data[i].ID)
			}

			// Reachable = GT docs in selected postings
			reachable := 0
			for gtDocID := range gtSet {
				postings, ok := docToPostings[gtDocID]
				if !ok {
					continue
				}
				for _, postingID := range postings {
					found := false
					for _, selID := range selectedPostingIDs {
						if postingID == selID {
							found = true
							break
						}
					}
					if found {
						reachable++
						break
					}
				}
			}
			stageReachable[si] += reachable
			stageTotalGT[si] += len(gtSet)

			// For approx and final, we need to actually run the search
			atomic.StoreUint32(&runner.index.searchProbe, uint32(cfg.probe))
			atomic.StoreUint32(&runner.index.rescoreLimit, uint32(cfg.rescore))

			collector := NewSearchTraceCollector(query.ID)
			tracedCtx := ContextWithTraceCollector(ctx, collector)
			returnedIDs, _, err := runner.index.SearchByMultiVector(tracedCtx, query.Vectors, *flagK, nil)
			require.NoError(t, err)

			trace := collector.Trace()

			// Count GT docs in approx top
			approxGT := 0
			approxSet := make(map[uint64]bool)
			for _, id := range trace.ApproxTopIDs {
				approxSet[id] = true
			}
			for gtDocID := range gtSet {
				if approxSet[gtDocID] {
					approxGT++
				}
			}
			stageApprox[si] += approxGT

			// Count GT docs in final results
			finalGT := 0
			for _, id := range returnedIDs {
				if gtSet[id] {
					finalGT++
				}
			}
			stageFinal[si] += finalGT
		}

		report.PerQueryAnalysis = append(report.PerQueryAnalysis, perQueryAnalysis)

		if (qi+1)%20 == 0 || qi == len(queries)-1 {
			t.Logf("Analyzed %d/%d queries", qi+1, len(queries))
		}
	}

	// Build coverage curve
	for di, depth := range coverageDepths {
		if coverageCounts[di] > 0 {
			report.CoverageCurve = append(report.CoverageCurve, PostingCoveragePoint{
				PostingDepth: depth,
				GTCoverage:   coverageSums[di] / float64(coverageCounts[di]),
				GTDocsFound:  int(coverageSums[di] * float64(*flagK)),
				TotalGTDocs:  coverageCounts[di] * (*flagK),
			})
		}
	}

	// Build diversity stats
	for di, probeDepth := range probeDepthsForDiversity {
		stats := diversityStats[di]
		if stats.queryCount == 0 {
			continue
		}

		avgTotalVecs := float64(stats.totalVectors) / float64(stats.queryCount)
		avgUniqueDocs := float64(stats.uniqueDocs) / float64(stats.queryCount)
		avgGTDocs := float64(stats.gtDocs) / float64(stats.queryCount)

		// Compute posting size percentiles
		sort.Ints(stats.postingSizes)
		var p50, p95, maxSize int
		if len(stats.postingSizes) > 0 {
			p50 = stats.postingSizes[len(stats.postingSizes)/2]
			p95 = stats.postingSizes[int(float64(len(stats.postingSizes))*0.95)]
			maxSize = stats.postingSizes[len(stats.postingSizes)-1]
		}

		div := PostingDiversityStats{
			NumPostingsSelected:   probeDepth,
			TotalVectorsScanned:   int(avgTotalVecs),
			UniqueDocsScanned:     int(avgUniqueDocs),
			AvgDocsPerPosting:     avgTotalVecs / float64(probeDepth),
			P50DocsPerPosting:     p50,
			P95DocsPerPosting:     p95,
			MaxDocsPerPosting:     maxSize,
			DuplicateRate:         float64(stats.duplicateVecs) / float64(stats.totalVectors),
			GTDocFraction:         avgGTDocs / avgUniqueDocs,
			GTDocsInSelectedPosts: int(avgGTDocs),
		}

		switch probeDepth {
		case 64:
			report.DiversityAtProbe64 = div
		case 256:
			report.DiversityAtProbe256 = div
		case 512:
			report.DiversityAtProbe512 = div
		}
	}

	// Build GT distribution stats
	if len(allMinGTRanks) > 0 {
		sort.Ints(allMinGTRanks)
		sort.Ints(allDistinctGTPostings)

		sum := 0
		for _, r := range allMinGTRanks {
			sum += r
		}
		sumPostings := 0
		for _, p := range allDistinctGTPostings {
			sumPostings += p
		}

		report.GTDistribution = GTPostingDistribution{
			AvgDistinctGTPostings: float64(sumPostings) / float64(len(allDistinctGTPostings)),
			AvgMinGTPostingRank:   float64(sum) / float64(len(allMinGTRanks)),
			P50MinGTPostingRank:   allMinGTRanks[len(allMinGTRanks)/2],
			P95MinGTPostingRank:   allMinGTRanks[int(float64(len(allMinGTRanks))*0.95)],
			MaxMinGTPostingRank:   allMinGTRanks[len(allMinGTRanks)-1],
		}
	}

	// Build stage-wise recall
	for si, cfg := range stageConfigs {
		if stageTotalGT[si] > 0 {
			report.StageRecalls = append(report.StageRecalls, StageWiseRecall{
				SearchProbe:     cfg.probe,
				RescoreLimit:    cfg.rescore,
				ReachableRecall: float64(stageReachable[si]) / float64(stageTotalGT[si]),
				ApproxTopRecall: float64(stageApprox[si]) / float64(stageTotalGT[si]),
				FinalRecall:     float64(stageFinal[si]) / float64(stageTotalGT[si]),
			})
		}
	}

	// Print results
	t.Log("")
	t.Log("================================================================================")
	t.Log("                    POSTING STRUCTURE ANALYSIS RESULTS")
	t.Log("================================================================================")
	t.Log("")

	// Coverage curve
	t.Log("1. POSTING-LEVEL GT COVERAGE CURVE")
	t.Log("-----------------------------------")
	t.Logf("%-12s | %-12s", "PostingDepth", "GTCoverage")
	t.Log("-------------|-------------")
	for _, pt := range report.CoverageCurve {
		t.Logf("%-12d | %-12.4f", pt.PostingDepth, pt.GTCoverage)
	}
	t.Log("")

	// Find depths for 50%, 75%, 90%, 95% coverage
	t.Log("Postings needed for coverage thresholds:")
	thresholds := []float64{0.50, 0.75, 0.90, 0.95}
	for _, thresh := range thresholds {
		for _, pt := range report.CoverageCurve {
			if pt.GTCoverage >= thresh {
				t.Logf("  %.0f%% coverage: %d postings", thresh*100, pt.PostingDepth)
				break
			}
		}
	}
	t.Log("")

	// Diversity metrics
	t.Log("2. POSTING DIVERSITY METRICS")
	t.Log("----------------------------")
	t.Logf("Probe=64:  %d unique docs from %d vectors (%.1f%% duplicate), GT fraction: %.4f",
		report.DiversityAtProbe64.UniqueDocsScanned,
		report.DiversityAtProbe64.TotalVectorsScanned,
		report.DiversityAtProbe64.DuplicateRate*100,
		report.DiversityAtProbe64.GTDocFraction)
	t.Logf("Probe=256: %d unique docs from %d vectors (%.1f%% duplicate), GT fraction: %.4f",
		report.DiversityAtProbe256.UniqueDocsScanned,
		report.DiversityAtProbe256.TotalVectorsScanned,
		report.DiversityAtProbe256.DuplicateRate*100,
		report.DiversityAtProbe256.GTDocFraction)
	t.Logf("Probe=512: %d unique docs from %d vectors (%.1f%% duplicate), GT fraction: %.4f",
		report.DiversityAtProbe512.UniqueDocsScanned,
		report.DiversityAtProbe512.TotalVectorsScanned,
		report.DiversityAtProbe512.DuplicateRate*100,
		report.DiversityAtProbe512.GTDocFraction)
	t.Log("")

	// GT distribution
	t.Log("3. GROUND-TRUTH POSTING DISTRIBUTION")
	t.Log("-------------------------------------")
	t.Logf("Avg distinct postings containing GT docs: %.1f", report.GTDistribution.AvgDistinctGTPostings)
	t.Logf("Avg min rank of posting containing a GT doc: %.1f", report.GTDistribution.AvgMinGTPostingRank)
	t.Logf("P50/P95/Max min GT posting rank: %d / %d / %d",
		report.GTDistribution.P50MinGTPostingRank,
		report.GTDistribution.P95MinGTPostingRank,
		report.GTDistribution.MaxMinGTPostingRank)
	t.Log("")

	// Stage-wise recall
	t.Log("4. STAGE-WISE RECALL BREAKDOWN")
	t.Log("-------------------------------")
	t.Logf("%-8s | %-8s | %-12s | %-12s | %-12s", "Probe", "Rescore", "Reachable", "ApproxTop", "Final")
	t.Log("---------|----------|--------------|--------------|-------------")
	for _, sr := range report.StageRecalls {
		t.Logf("%-8d | %-8d | %-12.4f | %-12.4f | %-12.4f",
			sr.SearchProbe, sr.RescoreLimit, sr.ReachableRecall, sr.ApproxTopRecall, sr.FinalRecall)
	}
	t.Log("")

	t.Log("================================================================================")

	// Write output file
	if *flagOutputPath != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		require.NoError(t, err)

		err = os.WriteFile(*flagOutputPath, data, 0644)
		require.NoError(t, err)

		t.Logf("Report written to: %s", *flagOutputPath)
	}
}

// =============================================================================
// FDE RANKING CORRELATION ANALYSIS
// =============================================================================

// FDERankingResult contains FDE ranking metrics for a single query.
type FDERankingResult struct {
	QueryID         string    `json:"query_id"`
	GTDocIDs        []uint64  `json:"gt_doc_ids"`
	GTFDERanks      []int     `json:"gt_fde_ranks"`      // FDE rank for each GT doc
	GTFDEDistances  []float32 `json:"gt_fde_distances"`  // FDE distance for each GT doc
	BestGTRank      int       `json:"best_gt_rank"`      // Lowest (best) rank among GT docs
	WorstGTRank     int       `json:"worst_gt_rank"`     // Highest (worst) rank among GT docs
	MedianGTRank    int       `json:"median_gt_rank"`    // Median rank among GT docs
	AvgGTRank       float64   `json:"avg_gt_rank"`       // Average rank among GT docs
	CoverageAt100   float64   `json:"coverage_at_100"`   // Fraction of GT in FDE top-100
	CoverageAt256   float64   `json:"coverage_at_256"`   // Fraction of GT in FDE top-256
	CoverageAt512   float64   `json:"coverage_at_512"`   // Fraction of GT in FDE top-512
	CoverageAt1024  float64   `json:"coverage_at_1024"`  // Fraction of GT in FDE top-1024
	CoverageAt2048  float64   `json:"coverage_at_2048"`  // Fraction of GT in FDE top-2048
	CoverageAt4096  float64   `json:"coverage_at_4096"`  // Fraction of GT in FDE top-4096
	CoverageAt8192  float64   `json:"coverage_at_8192"`  // Fraction of GT in FDE top-8192
	CoverageAt16384 float64   `json:"coverage_at_16384"` // Fraction of GT in FDE top-16384
	CoverageAt32768 float64   `json:"coverage_at_32768"` // Fraction of GT in FDE top-32768
	GTNotFound      int       `json:"gt_not_found"`      // Number of GT docs not found in index
}

// FDERankingCoveragePoint represents coverage at a single FDE rank threshold.
type FDERankingCoveragePoint struct {
	Threshold      int     `json:"threshold"`
	AvgCoverage    float64 `json:"avg_coverage"`
	MedianCoverage float64 `json:"median_coverage"`
	MinCoverage    float64 `json:"min_coverage"`
	MaxCoverage    float64 `json:"max_coverage"`
}

// FDERankingReport is the full FDE ranking analysis report.
type FDERankingReport struct {
	Config struct {
		DocsPath        string `json:"docs_path"`
		QueriesPath     string `json:"queries_path"`
		GroundTruthPath string `json:"ground_truth_path"`
		K               int    `json:"k"`
	} `json:"config"`

	NumDocs           int     `json:"num_docs"`
	NumQueries        int     `json:"num_queries"`
	NumPostings       int     `json:"num_postings"`
	IndexBuildTimeSec float64 `json:"index_build_time_sec"`
	FDEDimensions     int     `json:"fde_dimensions"`

	// Coverage curve
	CoverageCurve []FDERankingCoveragePoint `json:"coverage_curve"`

	// Aggregate GT rank statistics
	AvgGTRank       float64 `json:"avg_gt_rank"`
	MedianGTRank    int     `json:"median_gt_rank"`
	P50BestGTRank   int     `json:"p50_best_gt_rank"`
	P95BestGTRank   int     `json:"p95_best_gt_rank"`
	P50WorstGTRank  int     `json:"p50_worst_gt_rank"`
	P95WorstGTRank  int     `json:"p95_worst_gt_rank"`
	MaxWorstGTRank  int     `json:"max_worst_gt_rank"`
	GTNotFoundTotal int     `json:"gt_not_found_total"`

	// Per-query results
	Queries []FDERankingResult `json:"queries"`

	Timestamp string `json:"timestamp"`
}

// docFDERank holds document ID and its FDE distance for sorting.
type docFDERank struct {
	docID    uint64
	distance float32
}

// TestHFreshFDERankingCorrelation measures the correlation between FDE ranking
// and MaxSim ground truth. It computes for each query:
// - FDE distance from query to all documents
// - FDE rank of each ground-truth document
// - Coverage of GT docs at various FDE rank thresholds
//
// Run with:
//
//	go test -v -timeout 60m -run TestHFreshFDERankingCorrelation ./adapters/repos/db/vector/hfresh/... -args \
//	  -docs=/tmp/hfresh_diag_lotte_100q/docs.jsonl \
//	  -queries=/tmp/hfresh_diag_lotte_100q/queries.jsonl \
//	  -groundtruth=/tmp/hfresh_diag_lotte_100q/gt.jsonl \
//	  -k=100 \
//	  -out=/tmp/hfresh_diag_lotte_100q/fde_ranking.json
func TestHFreshFDERankingCorrelation(t *testing.T) {
	// Parse flags
	if !flag.Parsed() {
		flag.Parse()
	}

	// Check required flags
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
		return
	}

	ctx := context.Background()

	// Initialize report
	report := &FDERankingReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	report.Config.DocsPath = *flagDocsPath
	report.Config.QueriesPath = *flagQueriesPath
	report.Config.GroundTruthPath = *flagGroundTruthPath
	report.Config.K = *flagK

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err, "failed to load documents")
	t.Logf("Loaded %d documents", len(docs))
	report.NumDocs = len(docs)

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err, "failed to load queries")
	t.Logf("Loaded %d queries", len(queries))
	report.NumQueries = len(queries)

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err, "failed to load ground-truth")
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	// Create index (use default probe/rescore since we're not using search)
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, 64, 100, *flagVerbose)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err, "failed to insert doc %d", doc.ID)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}

	// Wait for background operations to settle
	time.Sleep(200 * time.Millisecond)

	report.IndexBuildTimeSec = time.Since(insertStart).Seconds()
	report.NumPostings = runner.index.PostingMap.Size()
	t.Logf("Index built in %.2fs, %d postings", report.IndexBuildTimeSec, report.NumPostings)

	// Build docID -> FDE vector map by retrieving from storage
	t.Log("Loading FDE vectors for all documents...")
	muveraBucket := runner.index.id + "_muvera_vectors"
	docFDEVectors := make(map[uint64][]float32)
	var fdeDims int

	for _, doc := range docs {
		fdeVec, err := runner.index.muveraEncoder.GetMuveraVectorForID(doc.ID, muveraBucket)
		if err != nil {
			t.Logf("Warning: failed to get FDE vector for doc %d: %v", doc.ID, err)
			continue
		}
		docFDEVectors[doc.ID] = fdeVec
		if fdeDims == 0 {
			fdeDims = len(fdeVec)
		}
	}
	t.Logf("Loaded FDE vectors for %d documents (dims=%d)", len(docFDEVectors), fdeDims)
	report.FDEDimensions = fdeDims

	// Create L2 distance provider
	l2Provider := distancer.NewL2SquaredProvider()

	// Coverage thresholds to measure
	thresholds := []int{100, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768}

	// Pre-allocate ranking slice
	allDocRanks := make([]docFDERank, 0, len(docFDEVectors))

	t.Log("")
	t.Log("Analyzing FDE ranking correlation...")
	t.Log("")

	// Process each query
	var allGTRanks []int
	var allBestRanks []int
	var allWorstRanks []int
	coveragesAtThreshold := make(map[int][]float64)
	for _, th := range thresholds {
		coveragesAtThreshold[th] = make([]float64, 0, len(queries))
	}

	for qi, query := range queries {
		gt, ok := groundTruth[query.ID]
		if !ok {
			t.Logf("Warning: no ground-truth for query %s, skipping", query.ID)
			continue
		}

		// Limit ground-truth to k
		if len(gt) > *flagK {
			gt = gt[:*flagK]
		}

		// Encode query to FDE
		queryFDE := runner.index.muveraEncoder.EncodeQuery(query.Vectors)

		// Compute FDE distance to all documents
		allDocRanks = allDocRanks[:0]
		for docID, docFDE := range docFDEVectors {
			dist, err := l2Provider.SingleDist(queryFDE, docFDE)
			if err != nil {
				continue
			}
			allDocRanks = append(allDocRanks, docFDERank{docID: docID, distance: dist})
		}

		// Sort by FDE distance (lower = more similar)
		sort.Slice(allDocRanks, func(i, j int) bool {
			return allDocRanks[i].distance < allDocRanks[j].distance
		})

		// Build rank lookup
		rankLookup := make(map[uint64]int, len(allDocRanks))
		for rank, dr := range allDocRanks {
			rankLookup[dr.docID] = rank + 1 // 1-indexed
		}

		// Find FDE rank of each GT doc
		qResult := FDERankingResult{
			QueryID:        query.ID,
			GTDocIDs:       gt,
			GTFDERanks:     make([]int, len(gt)),
			GTFDEDistances: make([]float32, len(gt)),
			BestGTRank:     len(allDocRanks) + 1,
			WorstGTRank:    0,
		}

		var rankSum float64
		var foundCount int
		var gtRanksForMedian []int

		for i, gtDocID := range gt {
			rank, found := rankLookup[gtDocID]
			if !found {
				qResult.GTFDERanks[i] = -1
				qResult.GTFDEDistances[i] = -1
				qResult.GTNotFound++
				continue
			}

			qResult.GTFDERanks[i] = rank
			// Get distance
			if rank > 0 && rank <= len(allDocRanks) {
				qResult.GTFDEDistances[i] = allDocRanks[rank-1].distance
			}

			if rank < qResult.BestGTRank {
				qResult.BestGTRank = rank
			}
			if rank > qResult.WorstGTRank {
				qResult.WorstGTRank = rank
			}

			rankSum += float64(rank)
			foundCount++
			gtRanksForMedian = append(gtRanksForMedian, rank)
			allGTRanks = append(allGTRanks, rank)
		}

		if foundCount > 0 {
			qResult.AvgGTRank = rankSum / float64(foundCount)

			// Median
			sort.Ints(gtRanksForMedian)
			if len(gtRanksForMedian)%2 == 0 {
				mid := len(gtRanksForMedian) / 2
				qResult.MedianGTRank = (gtRanksForMedian[mid-1] + gtRanksForMedian[mid]) / 2
			} else {
				qResult.MedianGTRank = gtRanksForMedian[len(gtRanksForMedian)/2]
			}

			allBestRanks = append(allBestRanks, qResult.BestGTRank)
			allWorstRanks = append(allWorstRanks, qResult.WorstGTRank)
		}

		// Compute coverage at each threshold
		for _, th := range thresholds {
			gtInThreshold := 0
			for _, rank := range qResult.GTFDERanks {
				if rank > 0 && rank <= th {
					gtInThreshold++
				}
			}
			coverage := float64(gtInThreshold) / float64(len(gt))
			coveragesAtThreshold[th] = append(coveragesAtThreshold[th], coverage)

			switch th {
			case 100:
				qResult.CoverageAt100 = coverage
			case 256:
				qResult.CoverageAt256 = coverage
			case 512:
				qResult.CoverageAt512 = coverage
			case 1024:
				qResult.CoverageAt1024 = coverage
			case 2048:
				qResult.CoverageAt2048 = coverage
			case 4096:
				qResult.CoverageAt4096 = coverage
			case 8192:
				qResult.CoverageAt8192 = coverage
			case 16384:
				qResult.CoverageAt16384 = coverage
			case 32768:
				qResult.CoverageAt32768 = coverage
			}
		}

		report.Queries = append(report.Queries, qResult)
		report.GTNotFoundTotal += qResult.GTNotFound

		if (qi+1)%20 == 0 || qi == len(queries)-1 {
			t.Logf("Analyzed %d/%d queries", qi+1, len(queries))
		}
	}

	// Compute aggregate statistics
	if len(allGTRanks) > 0 {
		var rankSum float64
		for _, r := range allGTRanks {
			rankSum += float64(r)
		}
		report.AvgGTRank = rankSum / float64(len(allGTRanks))

		sort.Ints(allGTRanks)
		if len(allGTRanks)%2 == 0 {
			mid := len(allGTRanks) / 2
			report.MedianGTRank = (allGTRanks[mid-1] + allGTRanks[mid]) / 2
		} else {
			report.MedianGTRank = allGTRanks[len(allGTRanks)/2]
		}
	}

	if len(allBestRanks) > 0 {
		sort.Ints(allBestRanks)
		report.P50BestGTRank = allBestRanks[len(allBestRanks)/2]
		p95Idx := int(float64(len(allBestRanks)) * 0.95)
		if p95Idx >= len(allBestRanks) {
			p95Idx = len(allBestRanks) - 1
		}
		report.P95BestGTRank = allBestRanks[p95Idx]
	}

	if len(allWorstRanks) > 0 {
		sort.Ints(allWorstRanks)
		report.P50WorstGTRank = allWorstRanks[len(allWorstRanks)/2]
		p95Idx := int(float64(len(allWorstRanks)) * 0.95)
		if p95Idx >= len(allWorstRanks) {
			p95Idx = len(allWorstRanks) - 1
		}
		report.P95WorstGTRank = allWorstRanks[p95Idx]
		report.MaxWorstGTRank = allWorstRanks[len(allWorstRanks)-1]
	}

	// Build coverage curve
	for _, th := range thresholds {
		coverages := coveragesAtThreshold[th]
		if len(coverages) == 0 {
			continue
		}

		var sum float64
		for _, c := range coverages {
			sum += c
		}
		avgCov := sum / float64(len(coverages))

		sort.Float64s(coverages)
		medianCov := coverages[len(coverages)/2]
		minCov := coverages[0]
		maxCov := coverages[len(coverages)-1]

		report.CoverageCurve = append(report.CoverageCurve, FDERankingCoveragePoint{
			Threshold:      th,
			AvgCoverage:    avgCov,
			MedianCoverage: medianCov,
			MinCoverage:    minCov,
			MaxCoverage:    maxCov,
		})
	}

	// Print results
	t.Log("")
	t.Log("================================================================================")
	t.Log("                     FDE RANKING CORRELATION ANALYSIS")
	t.Log("================================================================================")
	t.Log("")

	t.Log("1. FDE RANKING GT COVERAGE CURVE")
	t.Log("---------------------------------")
	t.Logf("%-10s | %-12s | %-12s | %-8s | %-8s", "FDE Top-N", "AvgCoverage", "MedCoverage", "Min", "Max")
	t.Log("-----------|--------------|--------------|----------|----------")
	for _, cp := range report.CoverageCurve {
		t.Logf("%-10d | %-12.4f | %-12.4f | %-8.4f | %-8.4f",
			cp.Threshold, cp.AvgCoverage, cp.MedianCoverage, cp.MinCoverage, cp.MaxCoverage)
	}
	t.Log("")

	t.Log("2. GT RANK DISTRIBUTION IN FDE ORDERING")
	t.Log("----------------------------------------")
	t.Logf("Average GT FDE rank: %.1f", report.AvgGTRank)
	t.Logf("Median GT FDE rank: %d", report.MedianGTRank)
	t.Logf("Best GT rank (P50/P95): %d / %d", report.P50BestGTRank, report.P95BestGTRank)
	t.Logf("Worst GT rank (P50/P95/Max): %d / %d / %d",
		report.P50WorstGTRank, report.P95WorstGTRank, report.MaxWorstGTRank)
	if report.GTNotFoundTotal > 0 {
		t.Logf("GT docs not found in index: %d", report.GTNotFoundTotal)
	}
	t.Log("")

	t.Log("3. INTERPRETATION")
	t.Log("-----------------")
	// Compute key interpretive metrics
	cov100 := 0.0
	cov1024 := 0.0
	cov8192 := 0.0
	for _, cp := range report.CoverageCurve {
		switch cp.Threshold {
		case 100:
			cov100 = cp.AvgCoverage
		case 1024:
			cov1024 = cp.AvgCoverage
		case 8192:
			cov8192 = cp.AvgCoverage
		}
	}

	if cov100 > 0.5 {
		t.Log("- FDE ranking shows STRONG correlation with MaxSim relevance")
		t.Log("  (>50% of GT docs in FDE top-100)")
	} else if cov1024 > 0.5 {
		t.Log("- FDE ranking shows MODERATE correlation with MaxSim relevance")
		t.Logf("  (%.1f%% of GT docs in FDE top-100, %.1f%% in top-1024)", cov100*100, cov1024*100)
	} else if cov8192 > 0.5 {
		t.Log("- FDE ranking shows WEAK correlation with MaxSim relevance")
		t.Logf("  (%.1f%% of GT docs in FDE top-100, %.1f%% in top-1024, %.1f%% in top-8192)",
			cov100*100, cov1024*100, cov8192*100)
	} else {
		t.Log("- FDE ranking shows VERY WEAK correlation with MaxSim relevance")
		t.Logf("  (Only %.1f%% of GT docs even in FDE top-8192)", cov8192*100)
	}

	if report.AvgGTRank > float64(report.NumDocs)/10 {
		t.Logf("- GT docs are on average buried at rank %.0f (%.1f%% of dataset)",
			report.AvgGTRank, report.AvgGTRank*100/float64(report.NumDocs))
	}
	t.Log("")

	t.Log("================================================================================")

	// Write output file
	outputPath := *flagOutputPath
	if outputPath == "" {
		// Use default path in same directory as docs
		dir := "/tmp/hfresh_diag_lotte_100q"
		outputPath = dir + "/fde_ranking.json"
	}

	data, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(outputPath, data, 0644)
	require.NoError(t, err)

	t.Logf("Report written to: %s", outputPath)
}

// =============================================================================
// HNSW+MUVERA vs HFresh+MUVERA COMPARISON
// =============================================================================

// ComparisonResult holds results for a single candidate budget comparison.
type ComparisonResult struct {
	CandidateBudget int `json:"candidate_budget"`

	// FDE baseline (from FDE ranking analysis)
	FDEGTCoverage float64 `json:"fde_gt_coverage"`

	// HNSW+MUVERA results
	HNSWCandidateGTCoverage float64 `json:"hnsw_candidate_gt_coverage"`
	HNSWRecall              float64 `json:"hnsw_recall"`
	HNSWAvgMs               float64 `json:"hnsw_avg_ms"`
	HNSWAvgCandidates       float64 `json:"hnsw_avg_candidates"`

	// HFresh+MUVERA results
	HFreshReachableGTCoverage float64 `json:"hfresh_reachable_gt_coverage"`
	HFreshRecall              float64 `json:"hfresh_recall"`
	HFreshAvgMs               float64 `json:"hfresh_avg_ms"`
	HFreshSearchProbe         int     `json:"hfresh_search_probe"`
}

// ComparisonReport is the full comparison report.
type ComparisonReport struct {
	Config struct {
		DocsPath        string `json:"docs_path"`
		QueriesPath     string `json:"queries_path"`
		GroundTruthPath string `json:"ground_truth_path"`
		K               int    `json:"k"`
	} `json:"config"`

	NumDocs    int `json:"num_docs"`
	NumQueries int `json:"num_queries"`

	// Per-budget comparison results
	Results []ComparisonResult `json:"results"`

	Timestamp string `json:"timestamp"`
}

// TestHNSWvsHFreshMuveraComparison compares HNSW+MUVERA against HFresh+MUVERA
// at various candidate budgets to understand if the recall gap is inherent to
// MUVERA/FDE or specific to HFresh clustering.
//
// Run with:
//
//	go test -v -timeout 120m -run TestHNSWvsHFreshMuveraComparison ./adapters/repos/db/vector/hfresh/... -args \
//	  -docs=/tmp/hfresh_diag_lotte_100q/docs.jsonl \
//	  -queries=/tmp/hfresh_diag_lotte_100q/queries.jsonl \
//	  -groundtruth=/tmp/hfresh_diag_lotte_100q/gt.jsonl \
//	  -k=100 \
//	  -out=/tmp/hfresh_diag_lotte_100q/hnsw_vs_hfresh.json
func TestHNSWvsHFreshMuveraComparison(t *testing.T) {
	// Parse flags
	if !flag.Parsed() {
		flag.Parse()
	}

	// Check required flags
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
		return
	}

	ctx := context.Background()

	// Initialize report
	report := &ComparisonReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	report.Config.DocsPath = *flagDocsPath
	report.Config.QueriesPath = *flagQueriesPath
	report.Config.GroundTruthPath = *flagGroundTruthPath
	report.Config.K = *flagK

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err, "failed to load documents")
	t.Logf("Loaded %d documents", len(docs))
	report.NumDocs = len(docs)

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err, "failed to load queries")
	t.Logf("Loaded %d queries", len(queries))
	report.NumQueries = len(queries)

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err, "failed to load ground-truth")
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	// Candidate budgets to test (1x, 2x, 4x, 8x, 16x, 32x, 64x of k=100)
	budgets := []int{100, 200, 400, 800, 1600, 3200, 6400}

	// FDE baseline coverage from the FDE ranking analysis
	// These are approximate values based on the previous experiment
	fdeBaseline := map[int]float64{
		100:   0.31,
		200:   0.40, // interpolated
		256:   0.46,
		400:   0.52, // interpolated
		512:   0.56,
		800:   0.62, // interpolated
		1024:  0.68,
		1600:  0.73, // interpolated
		2048:  0.77,
		3200:  0.82, // interpolated
		4096:  0.85,
		6400:  0.89, // interpolated
		8192:  0.92,
		16384: 0.96,
		32768: 0.99,
	}

	// =========================================================================
	// PART 1: Build and test HNSW+MUVERA
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("PART 1: Building HNSW+MUVERA Index")
	t.Log("================================================================================")

	hnswIndex, hnswCleanup := createHNSWMuveraIndex(t, docs)
	defer hnswCleanup()

	// Run HNSW queries at each budget
	t.Log("")
	t.Log("Running HNSW+MUVERA queries with detailed instrumentation...")
	t.Log("")

	// Print header for audit table
	t.Logf("%-8s | %-10s | %-10s | %-10s | %-6s | %-10s | %-10s",
		"Budget", "Requested", "Returned", "Unique", "EF", "Coverage", "Recall")
	t.Log("---------|------------|------------|------------|--------|------------|----------")

	hnswResults := make(map[int]struct {
		candidateCoverage float64
		recall            float64
		avgMs             float64
		avgCandidates     float64
		avgReturned       float64
		avgUnique         float64
		efUsed            int
	})

	for _, budget := range budgets {
		var totalCandidateCoverage float64
		var totalRecall float64
		var totalMs float64
		var totalCandidates float64
		var totalReturned float64
		var totalUnique float64
		var queryCount int
		var efUsed int

		for _, query := range queries {
			gt, ok := groundTruth[query.ID]
			if !ok {
				continue
			}
			if len(gt) > *flagK {
				gt = gt[:*flagK]
			}

			gtSet := make(map[uint64]bool, len(gt))
			for _, id := range gt {
				gtSet[id] = true
			}

			start := time.Now()
			ids, _, candidateCount, err := hnswIndex.SearchByMultiVectorWithCandidateBudget(ctx, query.Vectors, *flagK, budget, nil)
			elapsed := time.Since(start)
			require.NoError(t, err)

			// Get candidate set with detailed stats for auditing
			candidates, rawReturned, uniqueCount, ef, err := hnswIndex.GetMuveraCandidateSetWithStats(ctx, query.Vectors, budget, nil)
			require.NoError(t, err)
			efUsed = ef

			candidateSet := make(map[uint64]bool, len(candidates))
			for _, id := range candidates {
				candidateSet[id] = true
			}

			// Count GT in candidates
			gtInCandidates := 0
			for _, gtID := range gt {
				if candidateSet[gtID] {
					gtInCandidates++
				}
			}

			// Count GT in results
			gtInResults := 0
			for _, id := range ids {
				if gtSet[id] {
					gtInResults++
				}
			}

			totalCandidateCoverage += float64(gtInCandidates) / float64(len(gt))
			totalRecall += float64(gtInResults) / float64(len(gt))
			totalMs += float64(elapsed.Milliseconds())
			totalCandidates += float64(candidateCount)
			totalReturned += float64(rawReturned)
			totalUnique += float64(uniqueCount)
			queryCount++
		}

		avgCoverage := totalCandidateCoverage / float64(queryCount)
		avgRecall := totalRecall / float64(queryCount)
		avgReturned := totalReturned / float64(queryCount)
		avgUnique := totalUnique / float64(queryCount)

		hnswResults[budget] = struct {
			candidateCoverage float64
			recall            float64
			avgMs             float64
			avgCandidates     float64
			avgReturned       float64
			avgUnique         float64
			efUsed            int
		}{
			candidateCoverage: avgCoverage,
			recall:            avgRecall,
			avgMs:             totalMs / float64(queryCount),
			avgCandidates:     totalCandidates / float64(queryCount),
			avgReturned:       avgReturned,
			avgUnique:         avgUnique,
			efUsed:            efUsed,
		}

		t.Logf("%-8d | %-10d | %-10.1f | %-10.1f | %-6d | %-10.4f | %-10.4f",
			budget, budget, avgReturned, avgUnique, efUsed, avgCoverage, avgRecall)
	}

	t.Log("")

	// =========================================================================
	// PART 2: Build and test HFresh+MUVERA
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("PART 2: Building HFresh+MUVERA Index")
	t.Log("================================================================================")

	// Use high searchProbe to minimize routing failures
	hfreshSearchProbe := 1024
	hfreshRunner := createFileRunnerIndex(t, hfreshSearchProbe, budgets[len(budgets)-1], *flagVerbose)
	defer hfreshRunner.cleanup()

	// Insert documents
	t.Log("Inserting documents into HFresh...")
	for i, doc := range docs {
		err := hfreshRunner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err, "failed to insert doc %d", doc.ID)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)
	t.Logf("HFresh index built with %d postings", hfreshRunner.index.PostingMap.Size())

	// Build posting membership map
	membership := make(map[uint64][]uint64)
	for postingID, metadata := range hfreshRunner.index.PostingMap.Iter() {
		for docID := range metadata.Iter() {
			membership[docID] = append(membership[docID], postingID)
		}
	}

	// Run HFresh queries at each budget
	t.Log("")
	t.Log("Running HFresh+MUVERA queries...")
	hfreshResults := make(map[int]struct {
		reachableCoverage float64
		recall            float64
		avgMs             float64
	})

	for _, budget := range budgets {
		t.Logf("  Testing budget=%d (rescoreLimit=%d, searchProbe=%d)...", budget, budget, hfreshSearchProbe)

		// Update rescoreLimit atomically
		atomic.StoreUint32(&hfreshRunner.index.rescoreLimit, uint32(budget))

		var totalReachableCoverage float64
		var totalRecall float64
		var totalMs float64
		var queryCount int

		for _, query := range queries {
			gt, ok := groundTruth[query.ID]
			if !ok {
				continue
			}
			if len(gt) > *flagK {
				gt = gt[:*flagK]
			}

			gtSet := make(map[uint64]bool, len(gt))
			for _, id := range gt {
				gtSet[id] = true
			}

			// Create trace collector
			collector := NewSearchTraceCollector(query.ID)
			tracedCtx := ContextWithTraceCollector(ctx, collector)

			start := time.Now()
			ids, _, err := hfreshRunner.index.SearchByMultiVector(tracedCtx, query.Vectors, *flagK, nil)
			elapsed := time.Since(start)
			require.NoError(t, err)

			trace := collector.Trace()

			// Compute reachable coverage
			selectedSet := make(map[uint64]bool, len(trace.SelectedCentroids))
			for _, c := range trace.SelectedCentroids {
				selectedSet[c] = true
			}

			gtReachable := 0
			for _, gtID := range gt {
				docPostings := membership[gtID]
				for _, postingID := range docPostings {
					if selectedSet[postingID] {
						gtReachable++
						break
					}
				}
			}

			// Count GT in results
			gtInResults := 0
			for _, id := range ids {
				if gtSet[id] {
					gtInResults++
				}
			}

			totalReachableCoverage += float64(gtReachable) / float64(len(gt))
			totalRecall += float64(gtInResults) / float64(len(gt))
			totalMs += float64(elapsed.Milliseconds())
			queryCount++
		}

		hfreshResults[budget] = struct {
			reachableCoverage float64
			recall            float64
			avgMs             float64
		}{
			reachableCoverage: totalReachableCoverage / float64(queryCount),
			recall:            totalRecall / float64(queryCount),
			avgMs:             totalMs / float64(queryCount),
		}

		t.Logf("    Recall=%.4f, ReachableCoverage=%.4f, AvgMs=%.2f",
			hfreshResults[budget].recall, hfreshResults[budget].reachableCoverage, hfreshResults[budget].avgMs)
	}

	// =========================================================================
	// PART 3: Build comparison table
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("                     HNSW+MUVERA vs HFresh+MUVERA COMPARISON")
	t.Log("================================================================================")
	t.Log("")

	t.Logf("%-8s | %-12s | %-14s | %-12s | %-14s | %-12s | %-10s | %-10s",
		"Budget", "FDE_GT_Cov", "HNSW_Cand_Cov", "HNSW_Recall", "HFresh_Reach", "HFresh_Rec", "HNSW_ms", "HFresh_ms")
	t.Log("---------|--------------|----------------|--------------|----------------|--------------|------------|----------")

	for _, budget := range budgets {
		fde := fdeBaseline[budget]
		if fde == 0 {
			// Find closest
			for b, v := range fdeBaseline {
				if b <= budget && v > fde {
					fde = v
				}
			}
		}

		hnsw := hnswResults[budget]
		hfresh := hfreshResults[budget]

		result := ComparisonResult{
			CandidateBudget:           budget,
			FDEGTCoverage:             fde,
			HNSWCandidateGTCoverage:   hnsw.candidateCoverage,
			HNSWRecall:                hnsw.recall,
			HNSWAvgMs:                 hnsw.avgMs,
			HNSWAvgCandidates:         hnsw.avgCandidates,
			HFreshReachableGTCoverage: hfresh.reachableCoverage,
			HFreshRecall:              hfresh.recall,
			HFreshAvgMs:               hfresh.avgMs,
			HFreshSearchProbe:         hfreshSearchProbe,
		}
		report.Results = append(report.Results, result)

		t.Logf("%-8d | %-12.4f | %-14.4f | %-12.4f | %-14.4f | %-12.4f | %-10.2f | %-10.2f",
			budget, fde, hnsw.candidateCoverage, hnsw.recall, hfresh.reachableCoverage, hfresh.recall, hnsw.avgMs, hfresh.avgMs)
	}

	t.Log("")
	t.Log("================================================================================")
	t.Log("INTERPRETATION")
	t.Log("================================================================================")

	// Check if HNSW candidate coverage tracks FDE baseline
	hnswTracksFDE := true
	for _, budget := range budgets {
		fde := fdeBaseline[budget]
		hnsw := hnswResults[budget]
		if hnsw.candidateCoverage < fde*0.9 {
			hnswTracksFDE = false
			break
		}
	}

	if hnswTracksFDE {
		t.Log("- HNSW candidate coverage closely tracks FDE baseline")
		t.Log("  This suggests HNSW effectively preserves FDE ranking")
	} else {
		t.Log("- HNSW candidate coverage diverges from FDE baseline")
		t.Log("  HNSW graph navigation loses some FDE neighbors")
	}

	// Compare HNSW vs HFresh at same budget
	hnswBetter := true
	for _, budget := range budgets {
		hnsw := hnswResults[budget]
		hfresh := hfreshResults[budget]
		if hfresh.recall > hnsw.recall*1.05 {
			hnswBetter = false
			break
		}
	}

	if hnswBetter {
		t.Log("- HNSW+MUVERA achieves similar or better recall than HFresh+MUVERA")
		t.Log("  at the same candidate budget")
	} else {
		t.Log("- HFresh+MUVERA achieves better recall than HNSW+MUVERA")
		t.Log("  at some candidate budgets")
	}

	// Check if both follow similar recall curves
	bothNeedLargeBudget := true
	for _, budget := range budgets {
		hnsw := hnswResults[budget]
		if budget <= 800 && hnsw.recall > 0.7 {
			bothNeedLargeBudget = false
			break
		}
	}

	if bothNeedLargeBudget {
		t.Log("- Both HNSW and HFresh need large candidate budgets (>1000) for good recall")
		t.Log("  This confirms the long-tail requirement is MUVERA/FDE-inherent")
	}

	t.Log("")

	// Write output file
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/hnsw_vs_hfresh.json"
	}

	compData, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(outputPath, compData, 0644)
	require.NoError(t, err)

	t.Logf("Report written to: %s", outputPath)
}

// createHNSWMuveraIndex creates an HNSW+MUVERA index for the given documents.
func createHNSWMuveraIndex(t *testing.T, docs []DocInput) (*hnsw.HNSW, func()) {
	t.Helper()

	ctx := context.Background()

	store := testinghelpers.NewDummyStore(t)
	mvStore := make(map[uint64][][]float32)

	uc := enthnsw.UserConfig{
		VectorCacheMaxObjects: 1e12,
		MaxConnections:        32,
		EFConstruction:        128,
		EF:                    256,
		Multivector: enthnsw.MultivectorConfig{
			Enabled: true,
			MuveraConfig: enthnsw.MuveraConfig{
				Enabled:      true,
				KSim:         enthnsw.DefaultMultivectorKSim,
				DProjections: enthnsw.DefaultMultivectorDProjections,
				Repetitions:  enthnsw.DefaultMultivectorRepetitions,
			},
		},
	}

	cfg := hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hnsw_muvera_comparison",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, fmt.Errorf("single vector not supported")
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			vecs, ok := mvStore[id]
			if !ok {
				return nil, fmt.Errorf("doc %d not found", id)
			}
			return vecs, nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
	}

	index, err := hnsw.New(cfg, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)

	t.Log("Inserting documents into HNSW+MUVERA...")
	for i, doc := range docs {
		mvStore[doc.ID] = doc.Vectors
		err := index.AddMulti(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err, "failed to insert doc %d", doc.ID)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}

	cleanup := func() {
		_ = index.Shutdown(ctx)
	}

	return index, cleanup
}

// =============================================================================
// Experiment A: RQ1 vs Uncompressed FDE Ranking
// =============================================================================
// This test diagnoses whether the HFresh recall loss is due to RQ1 compression
// losing fidelity vs the exact FDE vectors. For each query:
// 1. Build the reachable set from selected postings (same as production flow)
// 2. Rank the reachable set using RQ1 approximate distance
// 3. Rank the same set using uncompressed FDE L2 distance
// 4. Compare both against MaxSim ground truth
// =============================================================================

// RQ1vsExactFDEResult captures per-query analysis
type RQ1vsExactFDEResult struct {
	QueryID     string  `json:"query_id"`
	ReachableGT int     `json:"reachable_gt"`   // GT docs in reachable set
	TotalGT     int     `json:"total_gt"`       // Total GT docs
	RQ1GTAtK    int     `json:"rq1_gt_at_k"`    // GT docs in RQ1 top-rescoreLimit
	FDEGTATK    int     `json:"fde_gt_at_k"`    // GT docs in FDE top-rescoreLimit
	ReachableN  int     `json:"reachable_n"`    // Total docs in reachable set
	AvgRQ1Rank  float64 `json:"avg_rq1_rank"`   // Avg RQ1 rank of GT docs
	AvgFDERank  float64 `json:"avg_fde_rank"`   // Avg FDE rank of GT docs
}

// RQ1vsExactFDEReport is the overall report
type RQ1vsExactFDEReport struct {
	Timestamp        string                     `json:"timestamp"`
	NumDocs          int                        `json:"num_docs"`
	NumQueries       int                        `json:"num_queries"`
	SearchProbe      int                        `json:"search_probe"`
	RescoreLimit     int                        `json:"rescore_limit"`
	K                int                        `json:"k"`
	CoverageAtBudget []RQ1vsFDECoveragePoint    `json:"coverage_at_budget"`
	PerQuery         []RQ1vsExactFDEResult      `json:"per_query,omitempty"`
	Summary          RQ1vsFDESummary            `json:"summary"`
}

// RQ1vsFDECoveragePoint compares RQ1 vs FDE at a specific budget
type RQ1vsFDECoveragePoint struct {
	Budget              int     `json:"budget"`
	ReachableGTCoverage float64 `json:"reachable_gt_coverage"` // GT coverage in reachable set
	RQ1GTCoverage       float64 `json:"rq1_gt_coverage"`       // GT coverage in RQ1 top-budget
	FDEGTCoverage       float64 `json:"fde_gt_coverage"`       // GT coverage in exact FDE top-budget
	RQ1LossVsFDE        float64 `json:"rq1_loss_vs_fde"`       // (FDE - RQ1) / FDE
}

// RQ1vsFDESummary aggregates across all queries
type RQ1vsFDESummary struct {
	TotalGT         int     `json:"total_gt"`
	TotalReachable  int     `json:"total_reachable"`
	ReachablePct    float64 `json:"reachable_pct"`
	AvgRQ1RankOfGT  float64 `json:"avg_rq1_rank_of_gt"`
	AvgFDERankOfGT  float64 `json:"avg_fde_rank_of_gt"`
	RQ1RankSpearman float64 `json:"rq1_rank_spearman,omitempty"` // Optional: rank correlation
}

func TestHFreshMuveraRQ1vsExactFDE(t *testing.T) {
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
	}

	ctx := context.Background()

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	// Configuration
	searchProbe := *flagSearchProbe
	if searchProbe == 0 {
		searchProbe = 1024 // Default to high probe for reachability
	}
	rescoreLimit := *flagRescoreLimit
	if rescoreLimit == 0 {
		rescoreLimit = 8192 // Default to high rescore limit
	}

	// Create index
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, searchProbe, rescoreLimit, *flagVerbose)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)

	t.Logf("Index built in %.2fs, %d postings", time.Since(insertStart).Seconds(), runner.index.PostingMap.Size())

	// Load FDE vectors for all documents
	t.Log("Loading FDE vectors...")
	muveraBucket := runner.index.id + "_muvera_vectors"
	docFDEVectors := make(map[uint64][]float32)
	for _, doc := range docs {
		fdeVec, err := runner.index.muveraEncoder.GetMuveraVectorForID(doc.ID, muveraBucket)
		if err != nil {
			continue
		}
		docFDEVectors[doc.ID] = fdeVec
	}
	t.Logf("Loaded FDE vectors for %d documents", len(docFDEVectors))

	// Build posting membership map
	t.Log("Building posting membership map...")
	docToPostings := make(map[uint64][]uint64)
	for postingID, metadata := range runner.index.PostingMap.Iter() {
		for docID := range metadata.Iter() {
			docToPostings[docID] = append(docToPostings[docID], postingID)
		}
	}
	t.Logf("Built posting membership for %d documents", len(docToPostings))

	// Coverage budgets to evaluate
	budgets := []int{100, 256, 512, 1024, 2048, 4096, 8192}

	// Create L2 distance provider
	l2Provider := distancer.NewL2SquaredProvider()

	// Initialize report
	report := RQ1vsExactFDEReport{
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		NumDocs:      len(docs),
		NumQueries:   len(queries),
		SearchProbe:  searchProbe,
		RescoreLimit: rescoreLimit,
		K:            *flagK,
	}

	// Per-budget accumulators
	type budgetStats struct {
		reachableGT int
		rq1GT       int
		fdeGT       int
		totalGT     int
	}
	budgetStatsMap := make(map[int]*budgetStats)
	for _, b := range budgets {
		budgetStatsMap[b] = &budgetStats{}
	}

	// Summary accumulators
	var totalGTDocs, totalReachableGTDocs int
	var sumRQ1Ranks, sumFDERanks float64
	var gtRankCount int

	t.Log("")
	t.Log("Analyzing RQ1 vs Exact FDE ranking...")
	t.Log("")

	for qi, query := range queries {
		gt, ok := groundTruth[query.ID]
		if !ok {
			continue
		}
		if len(gt) > *flagK {
			gt = gt[:*flagK]
		}

		gtSet := make(map[uint64]bool, len(gt))
		for _, id := range gt {
			gtSet[id] = true
		}

		// Encode query to FDE
		queryFDE := runner.index.muveraEncoder.EncodeQuery(query.Vectors)
		queryFDE = runner.index.normalizeVec(queryFDE)

		// Create query distancer for RQ1 scoring
		queryDistancer := runner.index.quantizer.NewDistancer(queryFDE)

		// Step 1: Get selected centroids (same as production)
		centroids, err := runner.index.Centroids.Search(queryFDE, searchProbe, nil)
		require.NoError(t, err)

		selectedCentroids := make([]uint64, 0, searchProbe)
		maxDist := centroids.data[0].Distance * runner.index.config.MaxDistanceRatio
		for i := 0; i < len(centroids.data) && len(selectedCentroids) < searchProbe; i++ {
			if maxDist > pruningMinMaxDistance && centroids.data[i].Distance > maxDist {
				continue
			}
			count, err := runner.index.PostingSizes.Get(ctx, centroids.data[i].ID)
			if err != nil || count == 0 {
				continue
			}
			selectedCentroids = append(selectedCentroids, centroids.data[i].ID)
		}

		// Step 2: Build reachable set from postings
		postings, err := runner.index.PostingStore.MultiGet(ctx, selectedCentroids)
		require.NoError(t, err)

		// Collect unique docs with their RQ1 distances
		type docWithDist struct {
			id      uint64
			rq1Dist float32
			fdeDist float32
		}
		reachableMap := make(map[uint64]*docWithDist)
		var decompressBuf []uint64

		for _, p := range postings {
			if p == nil {
				continue
			}
			for _, v := range p {
				id := v.ID()

				// Skip deleted
				deleted, err := runner.index.VersionMap.IsDeleted(ctx, id)
				if err != nil || deleted {
					continue
				}

				if _, seen := reachableMap[id]; seen {
					continue
				}

				// RQ1 distance
				decompressBuf = runner.index.quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
				rq1Dist, err := queryDistancer.Distance(decompressBuf)
				if err != nil {
					continue
				}

				// FDE distance
				var fdeDist float32 = 1e9
				if fdeVec, ok := docFDEVectors[id]; ok {
					fdeDist, _ = l2Provider.SingleDist(queryFDE, fdeVec)
				}

				reachableMap[id] = &docWithDist{id: id, rq1Dist: rq1Dist, fdeDist: fdeDist}
			}
		}

		// Count GT in reachable set
		reachableGTCount := 0
		for gtID := range gtSet {
			if _, ok := reachableMap[gtID]; ok {
				reachableGTCount++
			}
		}

		totalGTDocs += len(gt)
		totalReachableGTDocs += reachableGTCount

		// Convert to slice for sorting
		reachable := make([]*docWithDist, 0, len(reachableMap))
		for _, d := range reachableMap {
			reachable = append(reachable, d)
		}

		// Sort by RQ1 distance
		sort.Slice(reachable, func(i, j int) bool {
			return reachable[i].rq1Dist < reachable[j].rq1Dist
		})
		rq1Ranks := make(map[uint64]int, len(reachable))
		for rank, d := range reachable {
			rq1Ranks[d.id] = rank + 1
		}

		// Sort by FDE distance
		sort.Slice(reachable, func(i, j int) bool {
			return reachable[i].fdeDist < reachable[j].fdeDist
		})
		fdeRanks := make(map[uint64]int, len(reachable))
		for rank, d := range reachable {
			fdeRanks[d.id] = rank + 1
		}

		// Compute GT ranks
		for gtID := range gtSet {
			if rq1Rank, ok := rq1Ranks[gtID]; ok {
				sumRQ1Ranks += float64(rq1Rank)
				gtRankCount++
			}
			if fdeRank, ok := fdeRanks[gtID]; ok {
				sumFDERanks += float64(fdeRank)
			}
		}

		// Per-budget GT coverage
		for _, budget := range budgets {
			stats := budgetStatsMap[budget]
			stats.totalGT += len(gt)
			stats.reachableGT += reachableGTCount

			// RQ1 top-budget
			rq1TopSet := make(map[uint64]bool)
			// Sort by RQ1 again for top-K extraction
			sort.Slice(reachable, func(i, j int) bool {
				return reachable[i].rq1Dist < reachable[j].rq1Dist
			})
			for i := 0; i < budget && i < len(reachable); i++ {
				rq1TopSet[reachable[i].id] = true
			}
			for gtID := range gtSet {
				if rq1TopSet[gtID] {
					stats.rq1GT++
				}
			}

			// FDE top-budget
			fdeTopSet := make(map[uint64]bool)
			sort.Slice(reachable, func(i, j int) bool {
				return reachable[i].fdeDist < reachable[j].fdeDist
			})
			for i := 0; i < budget && i < len(reachable); i++ {
				fdeTopSet[reachable[i].id] = true
			}
			for gtID := range gtSet {
				if fdeTopSet[gtID] {
					stats.fdeGT++
				}
			}
		}

		// Per-query result
		result := RQ1vsExactFDEResult{
			QueryID:     query.ID,
			ReachableGT: reachableGTCount,
			TotalGT:     len(gt),
			ReachableN:  len(reachable),
		}

		// Use first budget for per-query RQ1/FDE at K
		if len(budgets) > 0 {
			firstBudget := budgets[0]
			rq1TopSet := make(map[uint64]bool)
			sort.Slice(reachable, func(i, j int) bool {
				return reachable[i].rq1Dist < reachable[j].rq1Dist
			})
			for i := 0; i < firstBudget && i < len(reachable); i++ {
				rq1TopSet[reachable[i].id] = true
			}
			for gtID := range gtSet {
				if rq1TopSet[gtID] {
					result.RQ1GTAtK++
				}
			}

			fdeTopSet := make(map[uint64]bool)
			sort.Slice(reachable, func(i, j int) bool {
				return reachable[i].fdeDist < reachable[j].fdeDist
			})
			for i := 0; i < firstBudget && i < len(reachable); i++ {
				fdeTopSet[reachable[i].id] = true
			}
			for gtID := range gtSet {
				if fdeTopSet[gtID] {
					result.FDEGTATK++
				}
			}
		}

		// Avg ranks
		var rq1RankSum, fdeRankSum float64
		var rankCnt int
		for gtID := range gtSet {
			if rq1Rank, ok := rq1Ranks[gtID]; ok {
				rq1RankSum += float64(rq1Rank)
				rankCnt++
			}
			if fdeRank, ok := fdeRanks[gtID]; ok {
				fdeRankSum += float64(fdeRank)
			}
		}
		if rankCnt > 0 {
			result.AvgRQ1Rank = rq1RankSum / float64(rankCnt)
			result.AvgFDERank = fdeRankSum / float64(rankCnt)
		}

		report.PerQuery = append(report.PerQuery, result)

		if (qi+1)%20 == 0 || qi == len(queries)-1 {
			t.Logf("Analyzed %d/%d queries", qi+1, len(queries))
		}
	}

	// Build coverage points
	t.Log("")
	t.Log("================================================================================")
	t.Log("               RQ1 vs EXACT FDE RANKING COMPARISON")
	t.Log("================================================================================")
	t.Log("")
	t.Log("Budget   | Reachable_GT | RQ1_GT_Cov   | FDE_GT_Cov   | RQ1_Loss_vs_FDE")
	t.Log("---------|--------------|--------------|--------------|----------------")

	for _, budget := range budgets {
		stats := budgetStatsMap[budget]
		reachableCov := float64(stats.reachableGT) / float64(stats.totalGT)
		rq1Cov := float64(stats.rq1GT) / float64(stats.totalGT)
		fdeCov := float64(stats.fdeGT) / float64(stats.totalGT)

		var loss float64
		if fdeCov > 0 {
			loss = (fdeCov - rq1Cov) / fdeCov
		}

		report.CoverageAtBudget = append(report.CoverageAtBudget, RQ1vsFDECoveragePoint{
			Budget:              budget,
			ReachableGTCoverage: reachableCov,
			RQ1GTCoverage:       rq1Cov,
			FDEGTCoverage:       fdeCov,
			RQ1LossVsFDE:        loss,
		})

		t.Logf("%-8d | %-12.4f | %-12.4f | %-12.4f | %-14.4f",
			budget, reachableCov, rq1Cov, fdeCov, loss)
	}

	// Summary
	report.Summary = RQ1vsFDESummary{
		TotalGT:        totalGTDocs,
		TotalReachable: totalReachableGTDocs,
		ReachablePct:   float64(totalReachableGTDocs) / float64(totalGTDocs),
	}
	if gtRankCount > 0 {
		report.Summary.AvgRQ1RankOfGT = sumRQ1Ranks / float64(gtRankCount)
		report.Summary.AvgFDERankOfGT = sumFDERanks / float64(gtRankCount)
	}

	t.Log("")
	t.Log("================================================================================")
	t.Log("SUMMARY")
	t.Log("================================================================================")
	t.Logf("Total GT docs: %d", totalGTDocs)
	t.Logf("Reachable GT docs: %d (%.2f%%)", totalReachableGTDocs, 100*report.Summary.ReachablePct)
	t.Logf("Avg RQ1 rank of GT docs: %.1f", report.Summary.AvgRQ1RankOfGT)
	t.Logf("Avg FDE rank of GT docs: %.1f", report.Summary.AvgFDERankOfGT)

	t.Log("")
	t.Log("================================================================================")
	t.Log("INTERPRETATION")
	t.Log("================================================================================")

	// Check if RQ1 is significantly worse than FDE
	if len(report.CoverageAtBudget) > 0 {
		midPoint := report.CoverageAtBudget[len(report.CoverageAtBudget)/2]
		if midPoint.RQ1LossVsFDE > 0.1 {
			t.Logf("- RQ1 loses %.1f%% GT coverage vs exact FDE at budget=%d",
				100*midPoint.RQ1LossVsFDE, midPoint.Budget)
			t.Log("  This suggests RQ1 compression is a significant bottleneck")
			t.Log("  Consider RQ2/RQ4 for MUVERA mode")
		} else if midPoint.RQ1LossVsFDE > 0.05 {
			t.Logf("- RQ1 loses %.1f%% GT coverage vs exact FDE at budget=%d",
				100*midPoint.RQ1LossVsFDE, midPoint.Budget)
			t.Log("  RQ1 compression causes modest ranking degradation")
		} else {
			t.Logf("- RQ1 closely matches exact FDE ranking (loss < 5%%)")
			t.Log("  The bottleneck is NOT RQ1 compression")
			t.Log("  The issue is likely FDE correlation with MaxSim ground truth")
		}
	}

	// Write report
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/rq1_vs_fde.json"
	}

	reportJSON, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(outputPath, reportJSON, 0644)
	require.NoError(t, err)

	t.Logf("Report written to: %s", outputPath)
}

// =============================================================================
// PLAID-Style Intermediate Stage Viability Analysis
// =============================================================================
// This test evaluates whether a PLAID-style two-stage approach could help.
// PLAID (ColBERT v2) uses:
//   Stage 1: Centroid-based filtering to get candidate set
//   Stage 2: More expensive but accurate re-ranking
//
// Key question: How tightly clustered are MaxSim Top-K docs within FDE ranking?
//
// Methodology:
// 1. Compute exact MaxSim ranking (brute-force over all documents)
// 2. Compute exact FDE ranking (brute-force over all documents)
// 3. For each MaxSim Top-100 document, find its FDE rank
// 4. Report percentiles and coverage thresholds
//
// Interpretation:
// - If 90-95% of MaxSim Top-100 are in FDE Top-5000: PLAID intermediate stage viable
// - If spread across tens of thousands: PLAID unlikely to help
// =============================================================================

// PLAIDViabilityReport captures the analysis results
type PLAIDViabilityReport struct {
	Timestamp         string                  `json:"timestamp"`
	NumDocs           int                     `json:"num_docs"`
	NumQueries        int                     `json:"num_queries"`
	K                 int                     `json:"k"`
	FDEDimensions     int                     `json:"fde_dimensions"`
	FDERankPercentiles PLAIDPercentiles       `json:"fde_rank_percentiles"`
	FDEDepthForRecovery PLAIDRecoveryThresholds `json:"fde_depth_for_recovery"`
	PerQueryStats     []PLAIDPerQueryStats    `json:"per_query_stats,omitempty"`
	Interpretation    string                  `json:"interpretation"`
}

// PLAIDPercentiles shows FDE rank percentiles for MaxSim Top-K docs
type PLAIDPercentiles struct {
	P50  int `json:"p50"`
	P75  int `json:"p75"`
	P90  int `json:"p90"`
	P95  int `json:"p95"`
	P99  int `json:"p99"`
	Max  int `json:"max"`
}

// PLAIDRecoveryThresholds shows FDE depth needed to recover X% of MaxSim Top-K
type PLAIDRecoveryThresholds struct {
	Recover50Pct  int `json:"recover_50_pct"`
	Recover75Pct  int `json:"recover_75_pct"`
	Recover90Pct  int `json:"recover_90_pct"`
	Recover95Pct  int `json:"recover_95_pct"`
	Recover99Pct  int `json:"recover_99_pct"`
	Recover100Pct int `json:"recover_100_pct"`
}

// PLAIDPerQueryStats captures per-query analysis
type PLAIDPerQueryStats struct {
	QueryID          string `json:"query_id"`
	MaxSimTop100FDERanks []int `json:"maxsim_top100_fde_ranks"`
	P50FDERank       int    `json:"p50_fde_rank"`
	P90FDERank       int    `json:"p90_fde_rank"`
	P99FDERank       int    `json:"p99_fde_rank"`
	MaxFDERank       int    `json:"max_fde_rank"`
}

func TestPLAIDViabilityAnalysis(t *testing.T) {
	if *flagDocsPath == "" || *flagQueriesPath == "" {
		t.Skip("Skipping: requires -docs and -queries flags")
	}

	ctx := context.Background()

	// Load documents
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	// Load queries
	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	k := 100 // MaxSim Top-K to analyze

	// Create HFresh+MUVERA index (needed for FDE encoding)
	t.Log("Creating HFresh+MUVERA index for FDE encoding...")
	runner := createFileRunnerIndex(t, 64, 100, false)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)
	t.Logf("Index built in %.2fs", time.Since(insertStart).Seconds())

	// Load FDE vectors
	t.Log("Loading FDE vectors...")
	muveraBucket := runner.index.id + "_muvera_vectors"
	docFDEVectors := make(map[uint64][]float32)
	var fdeDims int
	for _, doc := range docs {
		fdeVec, err := runner.index.muveraEncoder.GetMuveraVectorForID(doc.ID, muveraBucket)
		if err != nil {
			continue
		}
		docFDEVectors[doc.ID] = fdeVec
		if fdeDims == 0 {
			fdeDims = len(fdeVec)
		}
	}
	t.Logf("Loaded FDE vectors for %d documents (dims=%d)", len(docFDEVectors), fdeDims)

	// Build doc ID list for iteration
	docIDs := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
	}

	// Create L2 distance provider
	l2Provider := distancer.NewL2SquaredProvider()

	// Initialize report
	report := PLAIDViabilityReport{
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		NumDocs:       len(docs),
		NumQueries:    len(queries),
		K:             k,
		FDEDimensions: fdeDims,
	}

	// Collect all FDE ranks for MaxSim Top-K docs across all queries
	allFDERanks := make([]int, 0, len(queries)*k)

	t.Log("")
	t.Log("Computing MaxSim and FDE rankings for each query...")
	t.Log("")

	for qi, query := range queries {
		// Step 1: Compute exact MaxSim ranking (brute-force)
		type docMaxSim struct {
			id    uint64
			score float32
		}
		maxSimScores := make([]docMaxSim, 0, len(docs))

		for _, docID := range docIDs {
			docVecs, ok := runner.mvStore[docID]
			if !ok {
				continue
			}
			score, err := runner.index.maxSimScore(query.Vectors, docVecs)
			if err != nil {
				continue
			}
			maxSimScores = append(maxSimScores, docMaxSim{id: docID, score: score})
		}

		// Sort by MaxSim score (lower = more similar)
		sort.Slice(maxSimScores, func(i, j int) bool {
			return maxSimScores[i].score < maxSimScores[j].score
		})

		// Get MaxSim Top-K
		maxSimTopK := make(map[uint64]bool, k)
		for i := 0; i < k && i < len(maxSimScores); i++ {
			maxSimTopK[maxSimScores[i].id] = true
		}

		// Step 2: Compute exact FDE ranking (brute-force)
		queryFDE := runner.index.muveraEncoder.EncodeQuery(query.Vectors)

		type docFDEScore struct {
			id   uint64
			dist float32
		}
		fdeScores := make([]docFDEScore, 0, len(docFDEVectors))

		for docID, docFDEVec := range docFDEVectors {
			dist, err := l2Provider.SingleDist(queryFDE, docFDEVec)
			if err != nil {
				continue
			}
			fdeScores = append(fdeScores, docFDEScore{id: docID, dist: dist})
		}

		// Sort by FDE distance (lower = more similar)
		sort.Slice(fdeScores, func(i, j int) bool {
			return fdeScores[i].dist < fdeScores[j].dist
		})

		// Build FDE rank lookup
		fdeRankLookup := make(map[uint64]int, len(fdeScores))
		for rank, doc := range fdeScores {
			fdeRankLookup[doc.id] = rank + 1 // 1-indexed
		}

		// Step 3: Find FDE rank for each MaxSim Top-K document
		queryFDERanks := make([]int, 0, k)
		for docID := range maxSimTopK {
			if rank, ok := fdeRankLookup[docID]; ok {
				queryFDERanks = append(queryFDERanks, rank)
				allFDERanks = append(allFDERanks, rank)
			}
		}

		// Sort for percentile computation
		sort.Ints(queryFDERanks)

		// Per-query stats
		perQuery := PLAIDPerQueryStats{
			QueryID:              query.ID,
			MaxSimTop100FDERanks: queryFDERanks,
		}
		if len(queryFDERanks) > 0 {
			perQuery.P50FDERank = queryFDERanks[len(queryFDERanks)/2]
			perQuery.P90FDERank = queryFDERanks[int(float64(len(queryFDERanks))*0.9)]
			perQuery.P99FDERank = queryFDERanks[int(float64(len(queryFDERanks))*0.99)]
			perQuery.MaxFDERank = queryFDERanks[len(queryFDERanks)-1]
		}
		report.PerQueryStats = append(report.PerQueryStats, perQuery)

		if (qi+1)%20 == 0 || qi == len(queries)-1 {
			t.Logf("Analyzed %d/%d queries", qi+1, len(queries))
		}
	}

	// Compute global percentiles
	sort.Ints(allFDERanks)
	n := len(allFDERanks)
	if n > 0 {
		report.FDERankPercentiles = PLAIDPercentiles{
			P50: allFDERanks[n/2],
			P75: allFDERanks[int(float64(n)*0.75)],
			P90: allFDERanks[int(float64(n)*0.90)],
			P95: allFDERanks[int(float64(n)*0.95)],
			P99: allFDERanks[int(float64(n)*0.99)],
			Max: allFDERanks[n-1],
		}
	}

	// Compute FDE depth required for recovery thresholds
	// For each threshold, find minimum FDE depth that recovers X% of MaxSim Top-K
	thresholds := []float64{0.50, 0.75, 0.90, 0.95, 0.99, 1.00}
	recoveryDepths := make([]int, len(thresholds))

	for ti, threshold := range thresholds {
		targetCount := int(float64(n) * threshold)
		if targetCount >= n {
			targetCount = n
		}
		if targetCount > 0 {
			recoveryDepths[ti] = allFDERanks[targetCount-1]
		}
	}

	report.FDEDepthForRecovery = PLAIDRecoveryThresholds{
		Recover50Pct:  recoveryDepths[0],
		Recover75Pct:  recoveryDepths[1],
		Recover90Pct:  recoveryDepths[2],
		Recover95Pct:  recoveryDepths[3],
		Recover99Pct:  recoveryDepths[4],
		Recover100Pct: recoveryDepths[5],
	}

	// Print results
	t.Log("")
	t.Log("================================================================================")
	t.Log("              PLAID-STYLE INTERMEDIATE STAGE VIABILITY ANALYSIS")
	t.Log("================================================================================")
	t.Log("")
	t.Logf("Analyzed %d queries, tracking FDE ranks of MaxSim Top-%d documents", len(queries), k)
	t.Logf("Total MaxSim Top-%d docs across all queries: %d", k, n)
	t.Log("")

	t.Log("FDE RANK PERCENTILES (for MaxSim Top-100 documents):")
	t.Log("-----------------------------------------------------")
	t.Logf("  P50:  %d", report.FDERankPercentiles.P50)
	t.Logf("  P75:  %d", report.FDERankPercentiles.P75)
	t.Logf("  P90:  %d", report.FDERankPercentiles.P90)
	t.Logf("  P95:  %d", report.FDERankPercentiles.P95)
	t.Logf("  P99:  %d", report.FDERankPercentiles.P99)
	t.Logf("  Max:  %d", report.FDERankPercentiles.Max)
	t.Log("")

	t.Log("FDE DEPTH REQUIRED TO RECOVER X% OF MAXSIM TOP-100:")
	t.Log("----------------------------------------------------")
	t.Logf("  50%% recovery: FDE Top-%d", report.FDEDepthForRecovery.Recover50Pct)
	t.Logf("  75%% recovery: FDE Top-%d", report.FDEDepthForRecovery.Recover75Pct)
	t.Logf("  90%% recovery: FDE Top-%d", report.FDEDepthForRecovery.Recover90Pct)
	t.Logf("  95%% recovery: FDE Top-%d", report.FDEDepthForRecovery.Recover95Pct)
	t.Logf("  99%% recovery: FDE Top-%d", report.FDEDepthForRecovery.Recover99Pct)
	t.Logf("  100%% recovery: FDE Top-%d", report.FDEDepthForRecovery.Recover100Pct)
	t.Log("")

	t.Log("================================================================================")
	t.Log("INTERPRETATION")
	t.Log("================================================================================")

	// Determine viability
	p90Depth := report.FDEDepthForRecovery.Recover90Pct
	p95Depth := report.FDEDepthForRecovery.Recover95Pct

	if p90Depth <= 5000 {
		report.Interpretation = "PLAID-style intermediate stage is VIABLE. 90% of MaxSim Top-100 can be recovered from FDE Top-5000."
		t.Log("PLAID-style intermediate stage is VIABLE.")
		t.Logf("  - 90%% of MaxSim Top-100 documents are within FDE Top-%d", p90Depth)
		t.Logf("  - 95%% are within FDE Top-%d", p95Depth)
		t.Log("")
		t.Log("Recommendation: A two-stage approach could work:")
		t.Log("  Stage 1: Use FDE/centroid routing to get ~5000-10000 candidates")
		t.Log("  Stage 2: Re-rank candidates with more accurate scoring (e.g., partial MaxSim)")
	} else if p90Depth <= 20000 {
		report.Interpretation = "PLAID-style intermediate stage is MARGINALLY viable. 90% recovery requires FDE Top-" + fmt.Sprintf("%d", p90Depth)
		t.Log("PLAID-style intermediate stage is MARGINALLY viable.")
		t.Logf("  - 90%% of MaxSim Top-100 documents are within FDE Top-%d", p90Depth)
		t.Logf("  - 95%% are within FDE Top-%d", p95Depth)
		t.Log("")
		t.Log("A PLAID-style stage would require a large intermediate set (10K-20K candidates).")
		t.Log("This may still be worthwhile if MaxSim scoring is expensive.")
	} else {
		report.Interpretation = "PLAID-style intermediate stage is UNLIKELY to help. MaxSim Top-100 is spread across FDE Top-" + fmt.Sprintf("%d", p90Depth)
		t.Log("PLAID-style intermediate stage is UNLIKELY to help significantly.")
		t.Logf("  - 90%% of MaxSim Top-100 documents require FDE Top-%d", p90Depth)
		t.Logf("  - 95%% require FDE Top-%d", p95Depth)
		t.Log("")
		t.Log("The MaxSim Top-100 documents are spread too widely across the FDE ranking.")
		t.Log("A PLAID-style intermediate stage would need to process too many candidates.")
	}

	// Additional analysis: What fraction of corpus covers 95% of MaxSim Top-K?
	corpusFraction := float64(p95Depth) / float64(len(docs))
	t.Log("")
	t.Logf("To recover 95%% of MaxSim Top-100, you need to scan %.2f%% of the corpus by FDE ranking.",
		100*corpusFraction)

	// Write report
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/plaid_viability.json"
	}

	reportJSON, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(outputPath, reportJSON, 0644)
	require.NoError(t, err)

	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}

// =============================================================================
// PLAID-Style Simulation: FDE Top-N → Partial MaxSim → Top-M
// =============================================================================
// Tests whether a cheap intermediate scorer (partial MaxSim with fewer query
// tokens) can recover MaxSim-relevant documents better than FDE alone.
//
// Pipeline:
//   Stage 1: FDE ranking → Top-8000 candidates
//   Stage 2: Partial MaxSim (4/8/16 query tokens) → Top-M
//   Stage 3: Compare GT retention vs plain FDE Top-M
//
// Key question: Can partial MaxSim rescue GT documents that FDE mis-ranks?
// =============================================================================

// PLAIDSimulationReport captures the simulation results
type PLAIDSimulationReport struct {
	Timestamp       string                    `json:"timestamp"`
	NumDocs         int                       `json:"num_docs"`
	NumQueries      int                       `json:"num_queries"`
	K               int                       `json:"k"`
	FDECandidates   int                       `json:"fde_candidates"`
	TokenConfigs    []int                     `json:"token_configs"`
	Results         []PLAIDSimulationResult   `json:"results"`
}

// PLAIDSimulationResult shows GT retention for a specific configuration
type PLAIDSimulationResult struct {
	NumQueryTokens    int     `json:"num_query_tokens"`
	OutputLimit       int     `json:"output_limit"`
	FDEOnlyGTRetained int     `json:"fde_only_gt_retained"`
	PartialMaxSimGT   int     `json:"partial_maxsim_gt_retained"`
	TotalGT           int     `json:"total_gt"`
	FDEOnlyRecall     float64 `json:"fde_only_recall"`
	PartialMaxSimRecall float64 `json:"partial_maxsim_recall"`
	Improvement       float64 `json:"improvement_pct"`
}

func TestPLAIDSimulation(t *testing.T) {
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
	}

	ctx := context.Background()

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	k := *flagK
	if k == 0 {
		k = 10
	}

	// PLAID configuration
	fdeCandidates := 8000 // FDE Top-N candidates to feed into partial MaxSim
	tokenConfigs := []int{4, 8, 16, 32} // Number of query tokens for partial MaxSim
	outputLimits := []int{100, 256, 512, 1024, 2048} // Final Top-M to retain

	// Create HFresh+MUVERA index for FDE encoding
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, 64, 100, false)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)
	t.Logf("Index built in %.2fs", time.Since(insertStart).Seconds())

	// Load FDE vectors
	t.Log("Loading FDE vectors...")
	muveraBucket := runner.index.id + "_muvera_vectors"
	docFDEVectors := make(map[uint64][]float32)
	for _, doc := range docs {
		fdeVec, err := runner.index.muveraEncoder.GetMuveraVectorForID(doc.ID, muveraBucket)
		if err != nil {
			continue
		}
		docFDEVectors[doc.ID] = fdeVec
	}
	t.Logf("Loaded FDE vectors for %d documents", len(docFDEVectors))

	// Build doc ID list
	docIDs := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
	}

	// Create L2 distance provider
	l2Provider := distancer.NewL2SquaredProvider()

	// Initialize report
	report := PLAIDSimulationReport{
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		NumDocs:       len(docs),
		NumQueries:    len(queries),
		K:             k,
		FDECandidates: fdeCandidates,
		TokenConfigs:  tokenConfigs,
	}

	// Accumulators for each (tokenConfig, outputLimit) combination
	type configKey struct {
		tokens int
		output int
	}
	fdeOnlyGT := make(map[configKey]int)
	partialGT := make(map[configKey]int)
	totalGT := make(map[configKey]int)

	// Initialize accumulators
	for _, tokens := range tokenConfigs {
		for _, output := range outputLimits {
			key := configKey{tokens, output}
			fdeOnlyGT[key] = 0
			partialGT[key] = 0
			totalGT[key] = 0
		}
	}

	t.Log("")
	t.Log("Running PLAID simulation...")
	t.Log("")

	for qi, query := range queries {
		gt, ok := groundTruth[query.ID]
		if !ok {
			continue
		}
		if len(gt) > k {
			gt = gt[:k]
		}

		gtSet := make(map[uint64]bool, len(gt))
		for _, id := range gt {
			gtSet[id] = true
		}

		// Step 1: Compute FDE ranking for all documents
		queryFDE := runner.index.muveraEncoder.EncodeQuery(query.Vectors)

		type docScore struct {
			id   uint64
			dist float32
		}
		fdeScores := make([]docScore, 0, len(docFDEVectors))

		for docID, docFDEVec := range docFDEVectors {
			dist, err := l2Provider.SingleDist(queryFDE, docFDEVec)
			if err != nil {
				continue
			}
			fdeScores = append(fdeScores, docScore{id: docID, dist: dist})
		}

		// Sort by FDE distance
		sort.Slice(fdeScores, func(i, j int) bool {
			return fdeScores[i].dist < fdeScores[j].dist
		})

		// Get FDE Top-N candidates
		fdeTopN := make([]uint64, 0, fdeCandidates)
		for i := 0; i < fdeCandidates && i < len(fdeScores); i++ {
			fdeTopN = append(fdeTopN, fdeScores[i].id)
		}

		// For each token configuration, compute partial MaxSim ranking
		for _, numTokens := range tokenConfigs {
			// Select subset of query tokens
			querySubset := query.Vectors
			if numTokens < len(query.Vectors) {
				querySubset = query.Vectors[:numTokens]
			}

			// Compute partial MaxSim for FDE Top-N candidates
			type partialScore struct {
				id    uint64
				score float32
			}
			partialScores := make([]partialScore, 0, len(fdeTopN))

			for _, docID := range fdeTopN {
				docVecs, ok := runner.mvStore[docID]
				if !ok {
					continue
				}
				score, err := partialMaxSimScore(querySubset, docVecs, runner.index.config.DistanceProvider)
				if err != nil {
					continue
				}
				partialScores = append(partialScores, partialScore{id: docID, score: score})
			}

			// Sort by partial MaxSim score
			sort.Slice(partialScores, func(i, j int) bool {
				return partialScores[i].score < partialScores[j].score
			})

			// For each output limit, count GT retention
			for _, outputLimit := range outputLimits {
				key := configKey{numTokens, outputLimit}
				totalGT[key] += len(gt)

				// FDE-only: count GT in FDE Top-outputLimit
				fdeCount := 0
				for i := 0; i < outputLimit && i < len(fdeScores); i++ {
					if gtSet[fdeScores[i].id] {
						fdeCount++
					}
				}
				fdeOnlyGT[key] += fdeCount

				// Partial MaxSim: count GT in partial MaxSim Top-outputLimit
				partialCount := 0
				for i := 0; i < outputLimit && i < len(partialScores); i++ {
					if gtSet[partialScores[i].id] {
						partialCount++
					}
				}
				partialGT[key] += partialCount
			}
		}

		if (qi+1)%20 == 0 || qi == len(queries)-1 {
			t.Logf("Processed %d/%d queries", qi+1, len(queries))
		}
	}

	// Build results
	t.Log("")
	t.Log("================================================================================")
	t.Logf("      PLAID SIMULATION: FDE Top-%d → Partial MaxSim → Top-M", fdeCandidates)
	t.Log("================================================================================")
	t.Log("")
	t.Log("Tokens | Output | FDE_Recall | PartialMaxSim | Improvement")
	t.Log("-------|--------|------------|---------------|------------")

	for _, tokens := range tokenConfigs {
		for _, output := range outputLimits {
			key := configKey{tokens, output}

			fdeRecall := float64(fdeOnlyGT[key]) / float64(totalGT[key])
			partialRecall := float64(partialGT[key]) / float64(totalGT[key])
			improvement := 0.0
			if fdeRecall > 0 {
				improvement = (partialRecall - fdeRecall) / fdeRecall * 100
			}

			result := PLAIDSimulationResult{
				NumQueryTokens:      tokens,
				OutputLimit:         output,
				FDEOnlyGTRetained:   fdeOnlyGT[key],
				PartialMaxSimGT:     partialGT[key],
				TotalGT:             totalGT[key],
				FDEOnlyRecall:       fdeRecall,
				PartialMaxSimRecall: partialRecall,
				Improvement:         improvement,
			}
			report.Results = append(report.Results, result)

			t.Logf("%-6d | %-6d | %-10.4f | %-13.4f | %+.1f%%",
				tokens, output, fdeRecall, partialRecall, improvement)
		}
		t.Log("-------|--------|------------|---------------|------------")
	}

	t.Log("")
	t.Log("================================================================================")
	t.Log("INTERPRETATION")
	t.Log("================================================================================")

	// Find best improvement
	var bestImprovement float64
	var bestConfig PLAIDSimulationResult
	for _, r := range report.Results {
		if r.Improvement > bestImprovement {
			bestImprovement = r.Improvement
			bestConfig = r
		}
	}

	if bestImprovement > 10 {
		t.Logf("Partial MaxSim provides SIGNIFICANT improvement over FDE alone.")
		t.Logf("Best config: %d tokens, Top-%d → %.1f%% improvement (%.4f → %.4f recall)",
			bestConfig.NumQueryTokens, bestConfig.OutputLimit, bestConfig.Improvement,
			bestConfig.FDEOnlyRecall, bestConfig.PartialMaxSimRecall)
		t.Log("")
		t.Log("A PLAID-style intermediate stage is worthwhile.")
	} else if bestImprovement > 5 {
		t.Logf("Partial MaxSim provides MODERATE improvement over FDE alone.")
		t.Logf("Best config: %d tokens, Top-%d → %.1f%% improvement",
			bestConfig.NumQueryTokens, bestConfig.OutputLimit, bestConfig.Improvement)
	} else {
		t.Log("Partial MaxSim provides MINIMAL improvement over FDE alone.")
		t.Log("The FDE ranking already captures most of what partial MaxSim can find.")
		t.Log("A PLAID-style intermediate stage may not be worthwhile.")
	}

	// Write report
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/plaid_simulation.json"
	}

	reportJSON, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(outputPath, reportJSON, 0644)
	require.NoError(t, err)

	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}

// partialMaxSimScore computes MaxSim using only a subset of query tokens.
// This is cheaper than full MaxSim and can serve as an intermediate scorer.
func partialMaxSimScore(queryTokens [][]float32, docTokens [][]float32, distProvider distancer.Provider) (float32, error) {
	var score float32
	for _, queryToken := range queryTokens {
		d := distProvider.New(queryToken)
		minDist := float32(1e9)
		for _, docToken := range docTokens {
			dist, err := d.Distance(docToken)
			if err != nil {
				return 0, err
			}
			if dist < minDist {
				minDist = dist
			}
		}
		score += minDist
	}
	return score, nil
}

// =============================================================================
// PLAID Intermediate Scorer Experiment (Corrected Methodology)
// =============================================================================
//
// This experiment evaluates whether a cheap intermediate scoring stage can
// improve recall in a PLAID-style retrieval pipeline.
//
// PIPELINE:
//
//   FDE Top-N (cheap, approximate)
//       ↓
//   Intermediate Scorer → Top-M (moderate cost)
//       ↓
//   Full MaxSim on Top-M → Final Top-K (exact, expensive)
//       ↓
//   Measure final recall
//
// BASELINE COMPARISON:
//
//   FDE Top-M (no intermediate scorer)
//       ↓
//   Full MaxSim on Top-M → Final Top-K
//       ↓
//   Measure final recall
//
// KEY QUESTION: Does partial MaxSim preserve GT documents in Top-M better
// than FDE alone, leading to higher final recall after full MaxSim?
//
// SCORERS EVALUATED:
//   - Partial MaxSim with 4 query tokens
//   - Partial MaxSim with 8 query tokens
//   - Partial MaxSim with 16 query tokens
//
// FDE DEPTHS: 2000, 5000, 8000
// TOP-M VALUES: 256, 512, 1024, 2048
//
// METRICS REPORTED:
//   1. GT retention after intermediate stage (|GT ∩ Top-M| / |GT|)
//   2. Final recall after full MaxSim (|GT ∩ MaxSim(Top-M, K)| / |GT|)
//   3. Baseline recall (FDE Top-M → MaxSim → recall)
//
// =============================================================================

// PLAIDExperimentReport captures the full experiment results
type PLAIDExperimentReport struct {
	Timestamp     string                   `json:"timestamp"`
	NumDocs       int                      `json:"num_docs"`
	NumQueries    int                      `json:"num_queries"`
	K             int                      `json:"k"`
	FDEDepths     []int                    `json:"fde_depths"`
	TopMValues    []int                    `json:"top_m_values"`
	QueryTokens   []int                    `json:"query_token_configs"`
	Results       []PLAIDExperimentResult  `json:"results"`
	Summary       PLAIDExperimentSummary   `json:"summary"`
}

// PLAIDExperimentResult captures one configuration's results
type PLAIDExperimentResult struct {
	FDEDepth           int     `json:"fde_depth"`
	NumQueryTokens     int     `json:"num_query_tokens"`      // 0 = baseline (no intermediate scorer)
	TopM               int     `json:"top_m"`
	GTRetentionInTopM  float64 `json:"gt_retention_in_top_m"` // |GT ∩ Top-M| / |GT|
	FinalRecall        float64 `json:"final_recall"`          // |GT ∩ MaxSim(Top-M, K)| / |GT|
	BaselineRecall     float64 `json:"baseline_recall"`       // FDE Top-M → MaxSim → recall
	ImprovementOverFDE float64 `json:"improvement_over_fde"`  // (FinalRecall - BaselineRecall) / BaselineRecall * 100
}

// PLAIDExperimentSummary provides aggregate analysis
type PLAIDExperimentSummary struct {
	BestConfig        PLAIDExperimentResult `json:"best_config"`
	BestByTokenCount  map[int]PLAIDExperimentResult `json:"best_by_token_count"`
	BestByFDEDepth    map[int]PLAIDExperimentResult `json:"best_by_fde_depth"`
	Interpretation    string `json:"interpretation"`
}

func TestPLAIDIntermediateScorer(t *testing.T) {
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
	}

	ctx := context.Background()

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	k := *flagK
	if k == 0 {
		k = 10
	}

	// Experiment configuration (simplified)
	fdeDepths := []int{2000, 5000, 8000}
	topMValues := []int{256, 512, 1024, 2048}
	queryTokenConfigs := []int{4, 8, 16} // Partial MaxSim configurations

	// Create HFresh+MUVERA index for FDE encoding
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, 64, 100, false)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err)

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)
	t.Logf("Index built in %.2fs", time.Since(insertStart).Seconds())

	// Load FDE vectors
	t.Log("Loading FDE vectors...")
	muveraBucket := runner.index.id + "_muvera_vectors"
	docFDEVectors := make(map[uint64][]float32)
	for _, doc := range docs {
		fdeVec, err := runner.index.muveraEncoder.GetMuveraVectorForID(doc.ID, muveraBucket)
		if err != nil {
			continue
		}
		docFDEVectors[doc.ID] = fdeVec
	}
	t.Logf("Loaded FDE vectors for %d documents", len(docFDEVectors))

	// Build doc ID list
	docIDs := make([]uint64, 0, len(docs))
	for _, doc := range docs {
		docIDs = append(docIDs, doc.ID)
	}

	// Create L2 distance provider
	l2Provider := distancer.NewL2SquaredProvider()

	// Initialize report
	report := PLAIDExperimentReport{
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		NumDocs:       len(docs),
		NumQueries:    len(queries),
		K:             k,
		FDEDepths:     fdeDepths,
		TopMValues:    topMValues,
		QueryTokens:   queryTokenConfigs,
	}

	// Result accumulators keyed by (fdeDepth, numTokens, topM)
	// numTokens=0 means baseline (no intermediate scorer)
	type configKey struct {
		fdeDepth  int
		numTokens int // 0 = baseline
		topM      int
	}

	// Accumulators
	gtRetention := make(map[configKey]int)     // GT docs in Top-M after intermediate scorer
	finalRecall := make(map[configKey]int)     // GT docs in final MaxSim Top-K
	baselineRecall := make(map[configKey]int)  // GT docs with baseline (FDE Top-M → MaxSim)
	totalGT := make(map[configKey]int)

	t.Log("")
	t.Log("Running PLAID Intermediate Scorer experiment...")
	t.Log("Pipeline: FDE Top-N → Partial MaxSim → Top-M → Full MaxSim → Top-K")
	t.Log("")

	for qi, query := range queries {
		gt, ok := groundTruth[query.ID]
		if !ok {
			continue
		}
		if len(gt) > k {
			gt = gt[:k]
		}

		gtSet := make(map[uint64]bool, len(gt))
		for _, id := range gt {
			gtSet[id] = true
		}

		// Step 1: Compute FDE ranking for all documents
		queryFDE := runner.index.muveraEncoder.EncodeQuery(query.Vectors)

		type docScore struct {
			id   uint64
			dist float32
		}
		fdeScores := make([]docScore, 0, len(docFDEVectors))

		for docID, docFDEVec := range docFDEVectors {
			dist, err := l2Provider.SingleDist(queryFDE, docFDEVec)
			if err != nil {
				continue
			}
			fdeScores = append(fdeScores, docScore{id: docID, dist: dist})
		}

		// Sort by FDE distance (ascending = most similar first)
		sort.Slice(fdeScores, func(i, j int) bool {
			return fdeScores[i].dist < fdeScores[j].dist
		})

		// For each FDE depth
		for _, fdeDepth := range fdeDepths {
			// Get FDE Top-N candidates
			fdeTopN := make([]uint64, 0, fdeDepth)
			for i := 0; i < fdeDepth && i < len(fdeScores); i++ {
				fdeTopN = append(fdeTopN, fdeScores[i].id)
			}

			// For each Top-M value
			for _, topM := range topMValues {
				// Skip if topM > fdeDepth (can't select more than available)
				if topM > fdeDepth {
					continue
				}

				// =============================================================
				// BASELINE: FDE Top-M → Full MaxSim → Top-K
				// =============================================================
				baselineKey := configKey{fdeDepth, 0, topM}
				totalGT[baselineKey] += len(gt)

				// Get FDE Top-M (baseline candidates)
				fdeTopM := fdeTopN
				if topM < len(fdeTopN) {
					fdeTopM = fdeTopN[:topM]
				}

				// Run full MaxSim on baseline Top-M
				baselineMaxSim := make([]docScore, 0, len(fdeTopM))
				for _, docID := range fdeTopM {
					docVecs, ok := runner.mvStore[docID]
					if !ok {
						continue
					}
					score, err := runner.index.maxSimScore(query.Vectors, docVecs)
					if err != nil {
						continue
					}
					baselineMaxSim = append(baselineMaxSim, docScore{id: docID, dist: score})
				}

				sort.Slice(baselineMaxSim, func(i, j int) bool {
					return baselineMaxSim[i].dist < baselineMaxSim[j].dist
				})

				// Count GT in baseline final Top-K
				baselineGTCount := 0
				for i := 0; i < k && i < len(baselineMaxSim); i++ {
					if gtSet[baselineMaxSim[i].id] {
						baselineGTCount++
					}
				}
				baselineRecall[baselineKey] += baselineGTCount

				// =============================================================
				// PARTIAL MAXSIM VARIANTS
				// =============================================================
				for _, numTokens := range queryTokenConfigs {
					key := configKey{fdeDepth, numTokens, topM}
					totalGT[key] += len(gt)

					// Step 2: Score FDE Top-N with partial MaxSim
					querySubset := query.Vectors
					if numTokens < len(query.Vectors) {
						querySubset = query.Vectors[:numTokens]
					}

					partialScores := make([]docScore, 0, len(fdeTopN))
					for _, docID := range fdeTopN {
						docVecs, ok := runner.mvStore[docID]
						if !ok {
							continue
						}
						score, err := partialMaxSimScore(querySubset, docVecs, runner.index.config.DistanceProvider)
						if err != nil {
							continue
						}
						partialScores = append(partialScores, docScore{id: docID, dist: score})
					}

					// Sort by partial MaxSim score
					sort.Slice(partialScores, func(i, j int) bool {
						return partialScores[i].dist < partialScores[j].dist
					})

					// Step 3: Take Top-M after partial MaxSim
					scorerTopM := partialScores
					if topM < len(partialScores) {
						scorerTopM = partialScores[:topM]
					}

					// Count GT retention in Top-M (intermediate checkpoint)
					gtInTopM := 0
					for _, ds := range scorerTopM {
						if gtSet[ds.id] {
							gtInTopM++
						}
					}
					gtRetention[key] += gtInTopM

					// Step 4: Run full MaxSim on Top-M
					fullMaxSimScores := make([]docScore, 0, len(scorerTopM))
					for _, ds := range scorerTopM {
						docVecs, ok := runner.mvStore[ds.id]
						if !ok {
							continue
						}
						score, err := runner.index.maxSimScore(query.Vectors, docVecs)
						if err != nil {
							continue
						}
						fullMaxSimScores = append(fullMaxSimScores, docScore{id: ds.id, dist: score})
					}

					// Sort by full MaxSim score
					sort.Slice(fullMaxSimScores, func(i, j int) bool {
						return fullMaxSimScores[i].dist < fullMaxSimScores[j].dist
					})

					// Step 5: Count GT in final Top-K
					gtInFinalK := 0
					for i := 0; i < k && i < len(fullMaxSimScores); i++ {
						if gtSet[fullMaxSimScores[i].id] {
							gtInFinalK++
						}
					}
					finalRecall[key] += gtInFinalK
				}
			}
		}

		if (qi+1)%10 == 0 || qi == len(queries)-1 {
			t.Logf("Processed %d/%d queries", qi+1, len(queries))
		}
	}

	// Build results
	t.Log("")
	t.Log("================================================================================")
	t.Log("      PLAID INTERMEDIATE SCORER EXPERIMENT RESULTS")
	t.Log("================================================================================")
	t.Log("")
	t.Log("Pipeline: FDE Top-N → Partial MaxSim (X tokens) → Top-M → Full MaxSim → Top-K")
	t.Log("")

	bestImprovement := -100.0
	var bestConfig PLAIDExperimentResult
	bestByTokens := make(map[int]PLAIDExperimentResult)
	bestByDepth := make(map[int]PLAIDExperimentResult)

	for _, depth := range fdeDepths {
		t.Log("")
		t.Logf("--- FDE Depth: %d ---", depth)
		t.Log("Tokens | Top-M | GT_Retention | Final_Recall | Baseline | Improvement")
		t.Log("-------|-------|--------------|--------------|----------|------------")

		for _, topM := range topMValues {
			if topM > depth {
				continue
			}

			// Get baseline recall for this (depth, topM) pair
			baselineKey := configKey{depth, 0, topM}
			baselineRec := 0.0
			if totalGT[baselineKey] > 0 {
				baselineRec = float64(baselineRecall[baselineKey]) / float64(totalGT[baselineKey])
			}

			for _, numTokens := range queryTokenConfigs {
				key := configKey{depth, numTokens, topM}

				retention := 0.0
				if totalGT[key] > 0 {
					retention = float64(gtRetention[key]) / float64(totalGT[key])
				}
				final := 0.0
				if totalGT[key] > 0 {
					final = float64(finalRecall[key]) / float64(totalGT[key])
				}

				improvement := 0.0
				if baselineRec > 0 {
					improvement = (final - baselineRec) / baselineRec * 100
				}

				result := PLAIDExperimentResult{
					FDEDepth:           depth,
					NumQueryTokens:     numTokens,
					TopM:               topM,
					GTRetentionInTopM:  retention,
					FinalRecall:        final,
					BaselineRecall:     baselineRec,
					ImprovementOverFDE: improvement,
				}
				report.Results = append(report.Results, result)

				// Track best configurations
				if improvement > bestImprovement {
					bestImprovement = improvement
					bestConfig = result
				}
				if best, ok := bestByTokens[numTokens]; !ok || improvement > best.ImprovementOverFDE {
					bestByTokens[numTokens] = result
				}
				if best, ok := bestByDepth[depth]; !ok || improvement > best.ImprovementOverFDE {
					bestByDepth[depth] = result
				}

				t.Logf("%-6d | %-5d | %-12.4f | %-12.4f | %-8.4f | %+7.1f%%",
					numTokens, topM, retention, final, baselineRec, improvement)
			}
		}
	}

	// Summary
	t.Log("")
	t.Log("================================================================================")
	t.Log("SUMMARY")
	t.Log("================================================================================")
	t.Log("")

	t.Logf("Best configuration: %d query tokens, FDE-%d → Top-%d",
		bestConfig.NumQueryTokens, bestConfig.FDEDepth, bestConfig.TopM)
	t.Logf("  Baseline (FDE → MaxSim): %.4f", bestConfig.BaselineRecall)
	t.Logf("  With partial MaxSim:     %.4f (+%.1f%%)",
		bestConfig.FinalRecall, bestConfig.ImprovementOverFDE)
	t.Logf("  GT retention in Top-M:   %.4f", bestConfig.GTRetentionInTopM)

	t.Log("")
	t.Log("Best by query token count:")
	for _, tokens := range queryTokenConfigs {
		if best, ok := bestByTokens[tokens]; ok {
			t.Logf("  %2d tokens: FDE-%d → Top-%d, +%.1f%% (%.4f → %.4f)",
				tokens, best.FDEDepth, best.TopM, best.ImprovementOverFDE,
				best.BaselineRecall, best.FinalRecall)
		}
	}

	t.Log("")
	t.Log("Best by FDE depth:")
	for _, depth := range fdeDepths {
		if best, ok := bestByDepth[depth]; ok {
			t.Logf("  FDE-%5d: %d tokens → Top-%d, +%.1f%% (%.4f → %.4f)",
				depth, best.NumQueryTokens, best.TopM, best.ImprovementOverFDE,
				best.BaselineRecall, best.FinalRecall)
		}
	}

	// Interpretation
	interpretation := ""
	if bestImprovement > 20 {
		interpretation = "STRONG: Partial MaxSim provides major improvement over FDE alone. " +
			"A PLAID-style pipeline is strongly recommended. Proceed to evaluate representative-based scorers."
	} else if bestImprovement > 10 {
		interpretation = "MODERATE: Partial MaxSim provides meaningful improvement. " +
			"A PLAID-style pipeline is worthwhile. Consider evaluating cheaper intermediate scorers."
	} else if bestImprovement > 5 {
		interpretation = "MARGINAL: Partial MaxSim provides small improvement. " +
			"PLAID-style pipeline may help in specific configurations but gains are limited."
	} else if bestImprovement > 0 {
		interpretation = "MINIMAL: Partial MaxSim provides negligible improvement. " +
			"FDE ranking already captures most of the signal. Alternative approaches may be needed."
	} else {
		interpretation = "NEGATIVE: Partial MaxSim performs worse than baseline. " +
			"The intermediate scoring stage is not beneficial for this dataset."
	}

	t.Log("")
	t.Logf("INTERPRETATION: %s", interpretation)

	report.Summary = PLAIDExperimentSummary{
		BestConfig:       bestConfig,
		BestByTokenCount: bestByTokens,
		BestByFDEDepth:   bestByDepth,
		Interpretation:   interpretation,
	}

	// Write report
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/plaid_intermediate_scorer.json"
	}

	reportJSON, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(outputPath, reportJSON, 0644)
	require.NoError(t, err)

	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}

// =============================================================================
// PLAID Token Selection Validation Experiment
// =============================================================================
//
// This experiment validates whether the partial MaxSim improvement is robust
// across different token selection strategies, or if it's an artifact of
// selecting the first N query tokens.
//
// TOKEN SELECTION STRATEGIES:
//
// 1. first       - query.Vectors[:n] (current behavior)
// 2. even_spaced - n tokens evenly distributed across the sequence
// 3. highest_norm - n tokens with highest L2 norm
// 4. random_fixed - n random tokens with fixed seed for reproducibility
// 5. random_avg_5 - average of 5 random selections (optional)
//
// PIPELINE (same as before):
//
//   FDE Top-N → Partial MaxSim (selected tokens) → Top-M → Full MaxSim → Top-K
//
// =============================================================================

// TokenSelectionStrategy identifies the strategy for selecting query tokens
type TokenSelectionStrategy string

const (
	TokensFirst      TokenSelectionStrategy = "first"
	TokensEvenSpaced TokenSelectionStrategy = "even_spaced"
	TokensHighestNorm TokenSelectionStrategy = "highest_norm"
	TokensRandomFixed TokenSelectionStrategy = "random_fixed"
	TokensRandomAvg5  TokenSelectionStrategy = "random_avg_5"
)

// TokenSelectionResult captures results for one configuration
type TokenSelectionResult struct {
	FDEDepth       int                    `json:"fde_depth"`
	Strategy       TokenSelectionStrategy `json:"strategy"`
	NumTokens      int                    `json:"num_tokens"`
	TopM           int                    `json:"top_m"`
	GTRetention    float64                `json:"gt_retention"`
	FinalRecall    float64                `json:"final_recall"`
	BaselineRecall float64                `json:"baseline_recall"`
	Improvement    float64                `json:"improvement_pct"`
	EstimatedCost  int64                  `json:"estimated_cost"` // tokens × avg_doc_tokens × fde_depth
}

// TokenSelectionReport is the full experiment report
type TokenSelectionReport struct {
	Timestamp      string                  `json:"timestamp"`
	NumDocs        int                     `json:"num_docs"`
	NumQueries     int                     `json:"num_queries"`
	AvgDocTokens   float64                 `json:"avg_doc_tokens"`
	AvgQueryTokens float64                 `json:"avg_query_tokens"`
	K              int                     `json:"k"`
	FDEDepths      []int                   `json:"fde_depths"`
	TopMValues     []int                   `json:"top_m_values"`
	TokenCounts    []int                   `json:"token_counts"`
	Strategies     []TokenSelectionStrategy `json:"strategies"`
	Results        []TokenSelectionResult  `json:"results"`
	Summary        TokenSelectionSummary   `json:"summary"`
}

// TokenSelectionSummary provides aggregate analysis
type TokenSelectionSummary struct {
	BestByStrategy    map[TokenSelectionStrategy]TokenSelectionResult `json:"best_by_strategy"`
	BestOverall       TokenSelectionResult                            `json:"best_overall"`
	StrategyConsistency string                                        `json:"strategy_consistency"`
	Interpretation    string                                          `json:"interpretation"`
}

// selectQueryTokens returns a subset of query tokens based on the strategy
func selectQueryTokens(queryVecs [][]float32, n int, strategy TokenSelectionStrategy, queryIdx int) [][]float32 {
	if n >= len(queryVecs) {
		return queryVecs
	}

	switch strategy {
	case TokensFirst:
		return queryVecs[:n]

	case TokensEvenSpaced:
		step := float64(len(queryVecs)-1) / float64(n-1)
		result := make([][]float32, n)
		for i := 0; i < n; i++ {
			idx := int(float64(i) * step)
			if idx >= len(queryVecs) {
				idx = len(queryVecs) - 1
			}
			result[i] = queryVecs[idx]
		}
		return result

	case TokensHighestNorm:
		type tokenNorm struct {
			idx  int
			norm float32
		}
		norms := make([]tokenNorm, len(queryVecs))
		for i, vec := range queryVecs {
			var norm float32
			for _, v := range vec {
				norm += v * v
			}
			norms[i] = tokenNorm{i, norm}
		}
		sort.Slice(norms, func(i, j int) bool {
			return norms[i].norm > norms[j].norm
		})
		result := make([][]float32, n)
		for i := 0; i < n; i++ {
			result[i] = queryVecs[norms[i].idx]
		}
		return result

	case TokensRandomFixed:
		// Use deterministic seed based on query index and token count
		seed := int64(queryIdx*1000 + n)
		rng := rand.New(rand.NewSource(seed))
		perm := rng.Perm(len(queryVecs))
		indices := perm[:n]
		sort.Ints(indices) // Preserve relative order
		result := make([][]float32, n)
		for i, idx := range indices {
			result[i] = queryVecs[idx]
		}
		return result

	case TokensRandomAvg5:
		// This is handled separately in the main loop
		// Return first-n as fallback
		return queryVecs[:n]
	}

	return queryVecs[:n]
}

func TestPLAIDTokenSelectionValidation(t *testing.T) {
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
	}

	ctx := context.Background()

	// Load input files
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	groundTruth, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(groundTruth))

	k := *flagK
	if k == 0 {
		k = 10
	}

	// Experiment configuration - focused on best region
	fdeDepths := []int{5000, 8000}
	topMValues := []int{256, 512}
	tokenCounts := []int{8, 16}
	strategies := []TokenSelectionStrategy{
		TokensFirst,
		TokensEvenSpaced,
		TokensHighestNorm,
		TokensRandomFixed,
		TokensRandomAvg5,
	}

	// Create HFresh+MUVERA index for FDE encoding
	t.Log("Creating HFresh+MUVERA index...")
	runner := createFileRunnerIndex(t, 64, 100, false)
	defer runner.cleanup()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	var totalDocTokens int64
	for i, doc := range docs {
		err := runner.insertDoc(ctx, doc.ID, doc.Vectors)
		require.NoError(t, err)
		totalDocTokens += int64(len(doc.Vectors))

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)
	t.Logf("Index built in %.2fs", time.Since(insertStart).Seconds())

	avgDocTokens := float64(totalDocTokens) / float64(len(docs))
	t.Logf("Average doc tokens: %.1f", avgDocTokens)

	// Compute average query tokens
	var totalQueryTokens int64
	for _, q := range queries {
		totalQueryTokens += int64(len(q.Vectors))
	}
	avgQueryTokens := float64(totalQueryTokens) / float64(len(queries))
	t.Logf("Average query tokens: %.1f", avgQueryTokens)

	// Load FDE vectors
	t.Log("Loading FDE vectors...")
	muveraBucket := runner.index.id + "_muvera_vectors"
	docFDEVectors := make(map[uint64][]float32)
	for _, doc := range docs {
		fdeVec, err := runner.index.muveraEncoder.GetMuveraVectorForID(doc.ID, muveraBucket)
		if err != nil {
			continue
		}
		docFDEVectors[doc.ID] = fdeVec
	}
	t.Logf("Loaded FDE vectors for %d documents", len(docFDEVectors))

	// Create L2 distance provider
	l2Provider := distancer.NewL2SquaredProvider()

	// Initialize report
	report := TokenSelectionReport{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		NumDocs:        len(docs),
		NumQueries:     len(queries),
		AvgDocTokens:   avgDocTokens,
		AvgQueryTokens: avgQueryTokens,
		K:              k,
		FDEDepths:      fdeDepths,
		TopMValues:     topMValues,
		TokenCounts:    tokenCounts,
		Strategies:     strategies,
	}

	// Result accumulators
	type configKey struct {
		fdeDepth int
		strategy TokenSelectionStrategy
		tokens   int
		topM     int
	}

	gtRetention := make(map[configKey]int)
	finalRecall := make(map[configKey]int)
	baselineRecall := make(map[configKey]int)
	totalGT := make(map[configKey]int)

	t.Log("")
	t.Log("Running PLAID Token Selection Validation experiment...")
	t.Log("Pipeline: FDE Top-N → Partial MaxSim (selected tokens) → Top-M → Full MaxSim → Top-K")
	t.Log("")

	for qi, query := range queries {
		gt, ok := groundTruth[query.ID]
		if !ok {
			continue
		}
		if len(gt) > k {
			gt = gt[:k]
		}

		gtSet := make(map[uint64]bool, len(gt))
		for _, id := range gt {
			gtSet[id] = true
		}

		// Step 1: Compute FDE ranking for all documents
		queryFDE := runner.index.muveraEncoder.EncodeQuery(query.Vectors)

		type docScore struct {
			id   uint64
			dist float32
		}
		fdeScores := make([]docScore, 0, len(docFDEVectors))

		for docID, docFDEVec := range docFDEVectors {
			dist, err := l2Provider.SingleDist(queryFDE, docFDEVec)
			if err != nil {
				continue
			}
			fdeScores = append(fdeScores, docScore{id: docID, dist: dist})
		}

		sort.Slice(fdeScores, func(i, j int) bool {
			return fdeScores[i].dist < fdeScores[j].dist
		})

		// For each FDE depth
		for _, fdeDepth := range fdeDepths {
			fdeTopN := make([]uint64, 0, fdeDepth)
			for i := 0; i < fdeDepth && i < len(fdeScores); i++ {
				fdeTopN = append(fdeTopN, fdeScores[i].id)
			}

			// For each Top-M
			for _, topM := range topMValues {
				if topM > fdeDepth {
					continue
				}

				// Compute baseline once per (fdeDepth, topM)
				baselineKey := configKey{fdeDepth, "", 0, topM}
				if _, exists := totalGT[baselineKey]; !exists {
					totalGT[baselineKey] = 0
				}
				totalGT[baselineKey] += len(gt)

				// Baseline: FDE Top-M → Full MaxSim
				fdeTopM := fdeTopN
				if topM < len(fdeTopN) {
					fdeTopM = fdeTopN[:topM]
				}

				baselineMaxSim := make([]docScore, 0, len(fdeTopM))
				for _, docID := range fdeTopM {
					docVecs, ok := runner.mvStore[docID]
					if !ok {
						continue
					}
					score, err := runner.index.maxSimScore(query.Vectors, docVecs)
					if err != nil {
						continue
					}
					baselineMaxSim = append(baselineMaxSim, docScore{id: docID, dist: score})
				}

				sort.Slice(baselineMaxSim, func(i, j int) bool {
					return baselineMaxSim[i].dist < baselineMaxSim[j].dist
				})

				baselineGTCount := 0
				for i := 0; i < k && i < len(baselineMaxSim); i++ {
					if gtSet[baselineMaxSim[i].id] {
						baselineGTCount++
					}
				}
				baselineRecall[baselineKey] += baselineGTCount

				// For each token count
				for _, numTokens := range tokenCounts {
					// For each strategy
					for _, strategy := range strategies {
						key := configKey{fdeDepth, strategy, numTokens, topM}
						totalGT[key] += len(gt)

						if strategy == TokensRandomAvg5 {
							// Average over 5 random selections
							var totalGTInTopM, totalGTInFinal int
							for trial := 0; trial < 5; trial++ {
								seed := int64(qi*10000 + numTokens*100 + trial)
								rng := rand.New(rand.NewSource(seed))
								perm := rng.Perm(len(query.Vectors))
								n := numTokens
								if n > len(query.Vectors) {
									n = len(query.Vectors)
								}
								indices := perm[:n]
								sort.Ints(indices)
								querySubset := make([][]float32, n)
								for i, idx := range indices {
									querySubset[i] = query.Vectors[idx]
								}

								// Score with partial MaxSim
								partialScores := make([]docScore, 0, len(fdeTopN))
								for _, docID := range fdeTopN {
									docVecs, ok := runner.mvStore[docID]
									if !ok {
										continue
									}
									score, err := partialMaxSimScore(querySubset, docVecs, runner.index.config.DistanceProvider)
									if err != nil {
										continue
									}
									partialScores = append(partialScores, docScore{id: docID, dist: score})
								}

								sort.Slice(partialScores, func(i, j int) bool {
									return partialScores[i].dist < partialScores[j].dist
								})

								scorerTopM := partialScores
								if topM < len(partialScores) {
									scorerTopM = partialScores[:topM]
								}

								// Count GT in Top-M
								for _, ds := range scorerTopM {
									if gtSet[ds.id] {
										totalGTInTopM++
									}
								}

								// Full MaxSim on Top-M
								fullMaxSim := make([]docScore, 0, len(scorerTopM))
								for _, ds := range scorerTopM {
									docVecs, ok := runner.mvStore[ds.id]
									if !ok {
										continue
									}
									score, err := runner.index.maxSimScore(query.Vectors, docVecs)
									if err != nil {
										continue
									}
									fullMaxSim = append(fullMaxSim, docScore{id: ds.id, dist: score})
								}

								sort.Slice(fullMaxSim, func(i, j int) bool {
									return fullMaxSim[i].dist < fullMaxSim[j].dist
								})

								for i := 0; i < k && i < len(fullMaxSim); i++ {
									if gtSet[fullMaxSim[i].id] {
										totalGTInFinal++
									}
								}
							}
							// Average (using integer accumulation, will be divided later)
							gtRetention[key] += totalGTInTopM // Will divide by 5 when computing recall
							finalRecall[key] += totalGTInFinal

						} else {
							// Single selection
							querySubset := selectQueryTokens(query.Vectors, numTokens, strategy, qi)

							// Score with partial MaxSim
							partialScores := make([]docScore, 0, len(fdeTopN))
							for _, docID := range fdeTopN {
								docVecs, ok := runner.mvStore[docID]
								if !ok {
									continue
								}
								score, err := partialMaxSimScore(querySubset, docVecs, runner.index.config.DistanceProvider)
								if err != nil {
									continue
								}
								partialScores = append(partialScores, docScore{id: docID, dist: score})
							}

							sort.Slice(partialScores, func(i, j int) bool {
								return partialScores[i].dist < partialScores[j].dist
							})

							scorerTopM := partialScores
							if topM < len(partialScores) {
								scorerTopM = partialScores[:topM]
							}

							// Count GT in Top-M
							gtInTopM := 0
							for _, ds := range scorerTopM {
								if gtSet[ds.id] {
									gtInTopM++
								}
							}
							gtRetention[key] += gtInTopM

							// Full MaxSim on Top-M
							fullMaxSim := make([]docScore, 0, len(scorerTopM))
							for _, ds := range scorerTopM {
								docVecs, ok := runner.mvStore[ds.id]
								if !ok {
									continue
								}
								score, err := runner.index.maxSimScore(query.Vectors, docVecs)
								if err != nil {
									continue
								}
								fullMaxSim = append(fullMaxSim, docScore{id: ds.id, dist: score})
							}

							sort.Slice(fullMaxSim, func(i, j int) bool {
								return fullMaxSim[i].dist < fullMaxSim[j].dist
							})

							gtInFinal := 0
							for i := 0; i < k && i < len(fullMaxSim); i++ {
								if gtSet[fullMaxSim[i].id] {
									gtInFinal++
								}
							}
							finalRecall[key] += gtInFinal
						}
					}
				}
			}
		}

		if (qi+1)%10 == 0 || qi == len(queries)-1 {
			t.Logf("Processed %d/%d queries", qi+1, len(queries))
		}
	}

	// Build results
	t.Log("")
	t.Log("================================================================================")
	t.Log("      PLAID TOKEN SELECTION VALIDATION RESULTS")
	t.Log("================================================================================")
	t.Log("")
	t.Log("FDE_Depth | Strategy      | Tokens | TopM | Retention | Final    | Baseline | Improv")
	t.Log("----------|---------------|--------|------|-----------|----------|----------|-------")

	bestImprovement := -100.0
	var bestConfig TokenSelectionResult
	bestByStrategy := make(map[TokenSelectionStrategy]TokenSelectionResult)

	for _, fdeDepth := range fdeDepths {
		for _, topM := range topMValues {
			if topM > fdeDepth {
				continue
			}

			baselineKey := configKey{fdeDepth, "", 0, topM}
			baselineRec := 0.0
			if totalGT[baselineKey] > 0 {
				baselineRec = float64(baselineRecall[baselineKey]) / float64(totalGT[baselineKey])
			}

			for _, numTokens := range tokenCounts {
				for _, strategy := range strategies {
					key := configKey{fdeDepth, strategy, numTokens, topM}

					divisor := float64(totalGT[key])
					if strategy == TokensRandomAvg5 {
						divisor *= 5 // We accumulated 5 trials
					}

					retention := 0.0
					if divisor > 0 {
						retention = float64(gtRetention[key]) / divisor
					}
					final := 0.0
					if divisor > 0 {
						final = float64(finalRecall[key]) / divisor
					}

					improvement := 0.0
					if baselineRec > 0 {
						improvement = (final - baselineRec) / baselineRec * 100
					}

					cost := int64(numTokens) * int64(avgDocTokens) * int64(fdeDepth)

					result := TokenSelectionResult{
						FDEDepth:       fdeDepth,
						Strategy:       strategy,
						NumTokens:      numTokens,
						TopM:           topM,
						GTRetention:    retention,
						FinalRecall:    final,
						BaselineRecall: baselineRec,
						Improvement:    improvement,
						EstimatedCost:  cost,
					}
					report.Results = append(report.Results, result)

					if improvement > bestImprovement {
						bestImprovement = improvement
						bestConfig = result
					}
					if best, ok := bestByStrategy[strategy]; !ok || improvement > best.Improvement {
						bestByStrategy[strategy] = result
					}

					t.Logf("%-9d | %-13s | %-6d | %-4d | %-9.4f | %-8.4f | %-8.4f | %+6.1f%%",
						fdeDepth, strategy, numTokens, topM, retention, final, baselineRec, improvement)
				}
			}
		}
		t.Log("----------|---------------|--------|------|-----------|----------|----------|-------")
	}

	// Summary
	t.Log("")
	t.Log("================================================================================")
	t.Log("SUMMARY BY STRATEGY")
	t.Log("================================================================================")
	t.Log("")

	strategyImprovements := make([]float64, 0, len(strategies))
	for _, strategy := range strategies {
		if best, ok := bestByStrategy[strategy]; ok {
			t.Logf("%-13s: FDE-%d, %d tokens, Top-%d → +%.1f%% (%.4f → %.4f)",
				strategy, best.FDEDepth, best.NumTokens, best.TopM,
				best.Improvement, best.BaselineRecall, best.FinalRecall)
			strategyImprovements = append(strategyImprovements, best.Improvement)
		}
	}

	// Analyze consistency
	t.Log("")
	t.Log("================================================================================")
	t.Log("INTERPRETATION")
	t.Log("================================================================================")
	t.Log("")

	// Count strategies with >5% improvement
	strongStrategies := 0
	for _, imp := range strategyImprovements {
		if imp >= 5.0 {
			strongStrategies++
		}
	}

	consistency := ""
	interpretation := ""

	if strongStrategies >= 4 {
		consistency = "STRONG CONSISTENCY"
		interpretation = "Multiple token selection strategies show >5% improvement. " +
			"The partial MaxSim signal is robust and NOT an artifact of first-N selection. " +
			"A PLAID-like intermediate scoring stage is worth prototyping."
	} else if strongStrategies >= 2 {
		consistency = "MODERATE CONSISTENCY"
		interpretation = "Some token selection strategies show >5% improvement. " +
			"The signal exists but may depend on selection strategy. " +
			"Further investigation recommended before committing to PLAID-like implementation."
	} else if strongStrategies == 1 {
		consistency = "WEAK CONSISTENCY"
		interpretation = "Only one strategy shows >5% improvement. " +
			"The previous result may be partially an artifact. " +
			"PLAID-like path is not strongly justified."
	} else {
		consistency = "NO CONSISTENCY"
		interpretation = "No strategy shows >5% improvement. " +
			"The partial MaxSim intermediate stage does not provide robust gains. " +
			"PLAID-like path is NOT recommended."
	}

	t.Logf("Strategies with >5%% improvement: %d / %d", strongStrategies, len(strategies))
	t.Logf("Consistency: %s", consistency)
	t.Log("")
	t.Logf("INTERPRETATION: %s", interpretation)

	report.Summary = TokenSelectionSummary{
		BestByStrategy:     bestByStrategy,
		BestOverall:        bestConfig,
		StrategyConsistency: consistency,
		Interpretation:     interpretation,
	}

	// Write report
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/plaid_token_selection.json"
	}

	reportJSON, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(outputPath, reportJSON, 0644)
	require.NoError(t, err)

	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}

// =============================================================================
// Experiment B: Standard HFresh Single-Vector RQ1 vs Exact Comparison
// =============================================================================
// This test diagnoses whether RQ1 loses ranking quality in standard (non-MUVERA)
// HFresh mode, mirroring Experiment A's methodology exactly.
//
// METHODOLOGY:
// 1. Build the reachable candidate set from selected postings
// 2. Rank the reachable set using RQ1 approximate distance
// 3. Rank the same reachable set using uncompressed vector L2 distance
// 4. Compare both rankings against brute-force L2 ground truth
//
// DATA SOURCE LIMITATIONS:
// This test uses the first token of each multi-vector document as a proxy for
// single-vector data. This is a reasonable approximation because:
// - Token embeddings have the same dimensionality and distribution as real vectors
// - The clustering and RQ1 compression behave identically regardless of origin
// - This allows direct comparison with MUVERA results on the same dataset
//
// Caveats:
// - First tokens may cluster differently than naturally single-vector data
// - The token distribution may not represent typical single-vector workloads
// - For production validation, test with native single-vector datasets
// =============================================================================

// SingleVecRQ1vsExactResult captures per-query RQ1 vs exact comparison
type SingleVecRQ1vsExactResult struct {
	QueryID     string  `json:"query_id"`
	ReachableGT int     `json:"reachable_gt"`
	TotalGT     int     `json:"total_gt"`
	RQ1GTAtK    int     `json:"rq1_gt_at_k"`
	ExactGTAtK  int     `json:"exact_gt_at_k"`
	ReachableN  int     `json:"reachable_n"`
	AvgRQ1Rank  float64 `json:"avg_rq1_rank"`
	AvgExactRank float64 `json:"avg_exact_rank"`
}

// SingleVecRQ1vsExactReport is the overall report
type SingleVecRQ1vsExactReport struct {
	Timestamp        string                       `json:"timestamp"`
	NumDocs          int                          `json:"num_docs"`
	NumQueries       int                          `json:"num_queries"`
	VectorDims       int                          `json:"vector_dims"`
	SearchProbe      int                          `json:"search_probe"`
	K                int                          `json:"k"`
	DataSourceNote   string                       `json:"data_source_note"`
	CoverageAtBudget []SingleVecCoveragePoint     `json:"coverage_at_budget"`
	PerQuery         []SingleVecRQ1vsExactResult  `json:"per_query,omitempty"`
	Summary          SingleVecSummary             `json:"summary"`
	SecondaryRecall  []SingleVecRecallAttribution `json:"secondary_recall,omitempty"`
}

// SingleVecCoveragePoint compares RQ1 vs exact at a specific budget
type SingleVecCoveragePoint struct {
	Budget              int     `json:"budget"`
	ReachableGTCoverage float64 `json:"reachable_gt_coverage"`
	RQ1GTCoverage       float64 `json:"rq1_gt_coverage"`
	ExactGTCoverage     float64 `json:"exact_gt_coverage"`
	RQ1LossVsExact      float64 `json:"rq1_loss_vs_exact"`
}

// SingleVecSummary aggregates across all queries
type SingleVecSummary struct {
	TotalGT         int     `json:"total_gt"`
	TotalReachable  int     `json:"total_reachable"`
	ReachablePct    float64 `json:"reachable_pct"`
	AvgRQ1RankOfGT  float64 `json:"avg_rq1_rank_of_gt"`
	AvgExactRankOfGT float64 `json:"avg_exact_rank_of_gt"`
}

// SingleVecRecallAttribution is secondary output for recall attribution
type SingleVecRecallAttribution struct {
	RescoreLimit       int     `json:"rescore_limit"`
	Recall             float64 `json:"recall"`
	RoutingFailureRate float64 `json:"routing_failure_rate"`
	RQ1FailureRate     float64 `json:"rq1_failure_rate"`
	ExactFailureRate   float64 `json:"exact_failure_rate"`
}

func TestHFreshSingleVectorRQ1vsExact(t *testing.T) {
	if *flagDocsPath == "" {
		t.Skip("Skipping: requires -docs flag")
	}

	ctx := context.Background()

	// Load documents and use first token as single vector
	t.Log("Loading documents (using first token as single vector)...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	// Extract first token from each document as single vector
	singleVectors := make(map[uint64][]float32)
	var dims int
	for _, doc := range docs {
		if len(doc.Vectors) > 0 && len(doc.Vectors[0]) > 0 {
			singleVectors[doc.ID] = doc.Vectors[0]
			if dims == 0 {
				dims = len(doc.Vectors[0])
			}
		}
	}
	t.Logf("Extracted %d single vectors (dims=%d)", len(singleVectors), dims)

	// Configuration
	searchProbe := *flagSearchProbe
	if searchProbe == 0 {
		searchProbe = 1024 // High probe for reachability
	}
	k := *flagK
	if k == 0 {
		k = 10
	}

	// Create HFresh index WITHOUT MUVERA using the correct pattern
	t.Log("Creating HFresh single-vector index...")

	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.ErrorLevel)

	cfg := DefaultConfig()
	cfg.RootPath = t.TempDir()

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	cfg.Scheduler = scheduler

	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh_single_vec_rq1_test",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger
	cfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		if vec, ok := singleVectors[id]; ok {
			return vec, nil
		}
		return nil, fmt.Errorf("vector %d not found", id)
	}

	scheduler.Start()

	uc := ent.NewDefaultUserConfig()
	uc.SearchProbe = uint32(searchProbe)
	uc.RQ.RescoreLimit = 8192 // High rescore limit for full reachable set analysis

	store := testinghelpers.NewDummyStore(t)
	index, err := New(cfg, uc, store)
	require.NoError(t, err)

	defer func() {
		_ = index.Shutdown(ctx)
		scheduler.Close(ctx)
	}()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()
	for i, doc := range docs {
		if vec, ok := singleVectors[doc.ID]; ok {
			err := index.Add(ctx, doc.ID, vec)
			require.NoError(t, err, "failed to insert doc %d", doc.ID)
		}

		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)

	t.Logf("Index built in %.2fs, %d postings", time.Since(insertStart).Seconds(), index.PostingMap.Size())

	// Build posting membership for recall attribution
	t.Log("Building posting membership map...")
	docToPostings := make(map[uint64][]uint64)
	for postingID, metadata := range index.PostingMap.Iter() {
		for docID := range metadata.Iter() {
			docToPostings[docID] = append(docToPostings[docID], postingID)
		}
	}
	t.Logf("Built posting membership for %d documents", len(docToPostings))

	// Create L2 provider for distance computation
	l2Provider := distancer.NewL2SquaredProvider()

	// Generate synthetic queries (use every N-th document)
	numQueries := 100
	if numQueries > len(docs) {
		numQueries = len(docs)
	}
	queryInterval := len(docs) / numQueries

	type singleVecQuery struct {
		id     int
		vector []float32
	}
	queries := make([]singleVecQuery, 0, numQueries)
	for i := 0; i < numQueries; i++ {
		docIdx := i * queryInterval
		if docIdx < len(docs) {
			if vec, ok := singleVectors[docs[docIdx].ID]; ok {
				queries = append(queries, singleVecQuery{id: i, vector: vec})
			}
		}
	}
	t.Logf("Generated %d synthetic queries", len(queries))

	// Compute ground truth for each query (brute-force L2 nearest neighbors)
	t.Log("Computing ground truth (brute-force L2)...")
	type docDist struct {
		id   uint64
		dist float32
	}
	allDocDists := make([]docDist, 0, len(singleVectors))
	groundTruth := make(map[int][]uint64)

	for qi, query := range queries {
		allDocDists = allDocDists[:0]
		for docID, docVec := range singleVectors {
			dist, _ := l2Provider.SingleDist(query.vector, docVec)
			allDocDists = append(allDocDists, docDist{id: docID, dist: dist})
		}
		sort.Slice(allDocDists, func(i, j int) bool {
			return allDocDists[i].dist < allDocDists[j].dist
		})

		gt := make([]uint64, 0, k)
		for i := 0; i < k && i < len(allDocDists); i++ {
			gt = append(gt, allDocDists[i].id)
		}
		groundTruth[query.id] = gt

		if (qi+1)%20 == 0 {
			t.Logf("Computed GT for %d/%d queries", qi+1, len(queries))
		}
	}

	// Initialize report
	report := SingleVecRQ1vsExactReport{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		NumDocs:        len(singleVectors),
		NumQueries:     len(queries),
		VectorDims:     dims,
		SearchProbe:    searchProbe,
		K:              k,
		DataSourceNote: "Using first token of each multi-vector document as proxy for single-vector data. See test documentation for limitations.",
	}

	// Coverage budgets to evaluate
	budgets := []int{100, 256, 512, 1024, 2048, 4096, 8192}

	// Per-budget accumulators
	type budgetStats struct {
		reachableGT int
		rq1GT       int
		exactGT     int
		totalGT     int
	}
	budgetStatsMap := make(map[int]*budgetStats)
	for _, b := range budgets {
		budgetStatsMap[b] = &budgetStats{}
	}

	// Summary accumulators
	var totalGTDocs, totalReachableGTDocs int
	var sumRQ1Ranks, sumExactRanks float64
	var gtRankCount int

	t.Log("")
	t.Log("Analyzing RQ1 vs Exact single-vector ranking...")
	t.Log("")

	for qi, query := range queries {
		gt := groundTruth[query.id]
		gtSet := make(map[uint64]bool, len(gt))
		for _, id := range gt {
			gtSet[id] = true
		}

		// Normalize query vector
		queryNorm := index.normalizeVec(query.vector)

		// Create query distancer for RQ1 scoring
		queryDistancer := index.quantizer.NewDistancer(queryNorm)

		// Step 1: Get selected centroids (same as production)
		centroids, err := index.Centroids.Search(queryNorm, searchProbe, nil)
		require.NoError(t, err)

		selectedCentroids := make([]uint64, 0, searchProbe)
		maxDist := centroids.data[0].Distance * index.config.MaxDistanceRatio
		for i := 0; i < len(centroids.data) && len(selectedCentroids) < searchProbe; i++ {
			if maxDist > pruningMinMaxDistance && centroids.data[i].Distance > maxDist {
				continue
			}
			count, err := index.PostingSizes.Get(ctx, centroids.data[i].ID)
			if err != nil || count == 0 {
				continue
			}
			selectedCentroids = append(selectedCentroids, centroids.data[i].ID)
		}

		// Step 2: Build reachable set from postings
		postings, err := index.PostingStore.MultiGet(ctx, selectedCentroids)
		require.NoError(t, err)

		// Collect unique docs with their RQ1 and exact distances
		type docWithDist struct {
			id        uint64
			rq1Dist   float32
			exactDist float32
		}
		reachableMap := make(map[uint64]*docWithDist)
		var decompressBuf []uint64

		for _, p := range postings {
			if p == nil {
				continue
			}
			for _, v := range p {
				id := v.ID()

				// Skip deleted
				deleted, err := index.VersionMap.IsDeleted(ctx, id)
				if err != nil || deleted {
					continue
				}

				if _, seen := reachableMap[id]; seen {
					continue
				}

				// RQ1 distance
				decompressBuf = index.quantizer.FromCompressedBytesInto(v.Data(), decompressBuf)
				rq1Dist, err := queryDistancer.Distance(decompressBuf)
				if err != nil {
					continue
				}

				// Exact distance
				var exactDist float32 = 1e9
				if docVec, ok := singleVectors[id]; ok {
					exactDist, _ = l2Provider.SingleDist(queryNorm, index.normalizeVec(docVec))
				}

				reachableMap[id] = &docWithDist{id: id, rq1Dist: rq1Dist, exactDist: exactDist}
			}
		}

		// Count GT in reachable set
		reachableGTCount := 0
		for gtID := range gtSet {
			if _, ok := reachableMap[gtID]; ok {
				reachableGTCount++
			}
		}

		totalGTDocs += len(gt)
		totalReachableGTDocs += reachableGTCount

		// Convert to slice for sorting
		reachable := make([]*docWithDist, 0, len(reachableMap))
		for _, d := range reachableMap {
			reachable = append(reachable, d)
		}

		// Sort by RQ1 distance and build rank map
		sort.Slice(reachable, func(i, j int) bool {
			return reachable[i].rq1Dist < reachable[j].rq1Dist
		})
		rq1Ranks := make(map[uint64]int, len(reachable))
		for rank, d := range reachable {
			rq1Ranks[d.id] = rank + 1
		}

		// Sort by exact distance and build rank map
		sort.Slice(reachable, func(i, j int) bool {
			return reachable[i].exactDist < reachable[j].exactDist
		})
		exactRanks := make(map[uint64]int, len(reachable))
		for rank, d := range reachable {
			exactRanks[d.id] = rank + 1
		}

		// Compute GT ranks
		for gtID := range gtSet {
			if rq1Rank, ok := rq1Ranks[gtID]; ok {
				sumRQ1Ranks += float64(rq1Rank)
				gtRankCount++
			}
			if exactRank, ok := exactRanks[gtID]; ok {
				sumExactRanks += float64(exactRank)
			}
		}

		// Per-budget GT coverage
		for _, budget := range budgets {
			stats := budgetStatsMap[budget]
			stats.totalGT += len(gt)
			stats.reachableGT += reachableGTCount

			// RQ1 top-budget
			rq1TopSet := make(map[uint64]bool)
			sort.Slice(reachable, func(i, j int) bool {
				return reachable[i].rq1Dist < reachable[j].rq1Dist
			})
			for i := 0; i < budget && i < len(reachable); i++ {
				rq1TopSet[reachable[i].id] = true
			}
			for gtID := range gtSet {
				if rq1TopSet[gtID] {
					stats.rq1GT++
				}
			}

			// Exact top-budget
			exactTopSet := make(map[uint64]bool)
			sort.Slice(reachable, func(i, j int) bool {
				return reachable[i].exactDist < reachable[j].exactDist
			})
			for i := 0; i < budget && i < len(reachable); i++ {
				exactTopSet[reachable[i].id] = true
			}
			for gtID := range gtSet {
				if exactTopSet[gtID] {
					stats.exactGT++
				}
			}
		}

		if (qi+1)%20 == 0 || qi == len(queries)-1 {
			t.Logf("Analyzed %d/%d queries", qi+1, len(queries))
		}
	}

	// Build coverage points and print results
	t.Log("")
	t.Log("================================================================================")
	t.Log("         SINGLE-VECTOR RQ1 vs EXACT RANKING COMPARISON")
	t.Log("================================================================================")
	t.Log("")
	t.Log("Budget   | Reachable_GT | RQ1_GT_Cov   | Exact_GT_Cov | RQ1_Loss_vs_Exact")
	t.Log("---------|--------------|--------------|--------------|------------------")

	for _, budget := range budgets {
		stats := budgetStatsMap[budget]
		reachableCov := float64(stats.reachableGT) / float64(stats.totalGT)
		rq1Cov := float64(stats.rq1GT) / float64(stats.totalGT)
		exactCov := float64(stats.exactGT) / float64(stats.totalGT)

		var loss float64
		if exactCov > 0 {
			loss = (exactCov - rq1Cov) / exactCov
		}

		report.CoverageAtBudget = append(report.CoverageAtBudget, SingleVecCoveragePoint{
			Budget:              budget,
			ReachableGTCoverage: reachableCov,
			RQ1GTCoverage:       rq1Cov,
			ExactGTCoverage:     exactCov,
			RQ1LossVsExact:      loss,
		})

		t.Logf("%-8d | %-12.4f | %-12.4f | %-12.4f | %-16.4f",
			budget, reachableCov, rq1Cov, exactCov, loss)
	}

	// Summary
	report.Summary = SingleVecSummary{
		TotalGT:        totalGTDocs,
		TotalReachable: totalReachableGTDocs,
		ReachablePct:   float64(totalReachableGTDocs) / float64(totalGTDocs),
	}
	if gtRankCount > 0 {
		report.Summary.AvgRQ1RankOfGT = sumRQ1Ranks / float64(gtRankCount)
		report.Summary.AvgExactRankOfGT = sumExactRanks / float64(gtRankCount)
	}

	t.Log("")
	t.Log("================================================================================")
	t.Log("SUMMARY")
	t.Log("================================================================================")
	t.Logf("Total GT docs: %d", totalGTDocs)
	t.Logf("Reachable GT docs: %d (%.2f%%)", totalReachableGTDocs, 100*report.Summary.ReachablePct)
	t.Logf("Avg RQ1 rank of GT docs: %.1f", report.Summary.AvgRQ1RankOfGT)
	t.Logf("Avg Exact rank of GT docs: %.1f", report.Summary.AvgExactRankOfGT)

	t.Log("")
	t.Log("================================================================================")
	t.Log("INTERPRETATION")
	t.Log("================================================================================")

	// Check if RQ1 is significantly worse than exact
	if len(report.CoverageAtBudget) > 0 {
		midPoint := report.CoverageAtBudget[len(report.CoverageAtBudget)/2]
		if midPoint.RQ1LossVsExact > 0.1 {
			t.Logf("- RQ1 loses %.1f%% GT coverage vs exact vectors at budget=%d",
				100*midPoint.RQ1LossVsExact, midPoint.Budget)
			t.Log("  RQ1 compression causes SIGNIFICANT ranking loss in single-vector mode")
			t.Log("  Consider RQ2/RQ4 for standard HFresh, not just MUVERA")
		} else if midPoint.RQ1LossVsExact > 0.05 {
			t.Logf("- RQ1 loses %.1f%% GT coverage vs exact vectors at budget=%d",
				100*midPoint.RQ1LossVsExact, midPoint.Budget)
			t.Log("  RQ1 compression causes MODERATE ranking loss")
		} else {
			t.Logf("- RQ1 closely matches exact ranking (loss < 5%%)")
			t.Log("  RQ1 is NOT a significant bottleneck in single-vector mode")
			t.Log("  If MUVERA shows higher RQ1 loss, it's FDE-specific")
		}
	}

	t.Log("")
	t.Log("DATA SOURCE NOTE:")
	t.Log("  Using first token of each multi-vector document as single-vector proxy.")
	t.Log("  This tests RQ1 compression quality on the same embedding distribution.")
	t.Log("  For production validation, test with native single-vector datasets.")

	// Write report
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/single_vec_rq1_vs_exact.json"
	}

	reportJSON, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)

	err = os.WriteFile(outputPath, reportJSON, 0644)
	require.NoError(t, err)

	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}

// =============================================================================
// HNSW vs HFresh Controlled EF/SearchProbe Sweep
// =============================================================================
// This test validates that HNSW+MUVERA and HFresh+MUVERA achieve comparable
// recall when EF and searchProbe are swept over the same range.
//
// Both indexes use their STANDARD MUVERA overfetch behavior:
// - HNSW: hardcoded 2x overfetch → SearchByVector(k*2) → Late Interaction → Top-K
// - HFresh: rescoreLimit as candidate budget → SearchByVector(rescoreLimit) → Late Interaction → Top-K
//
// The sweep varies:
// - HNSW EF: controls how many candidates HNSW considers during graph traversal
// - HFresh searchProbe: controls how many centroids are searched
//
// With high rescoreLimit for HFresh (to remove rescoring bottleneck), we expect
// recall to scale primarily with searchProbe (which controls reachability).
// =============================================================================

// ControlledSweepResult captures per-configuration results
type ControlledSweepResult struct {
	Value                    int     `json:"value"`
	HNSWStrictRecall         float64 `json:"hnsw_strict_recall"`
	HNSWRelaxedRecall        float64 `json:"hnsw_relaxed_recall"`
	HNSWEffectiveEF          int     `json:"hnsw_effective_ef"`
	HNSWAvgMs                float64 `json:"hnsw_avg_ms"`
	HFreshStrictRecall       float64 `json:"hfresh_strict_recall"`
	HFreshRelaxedRecall      float64 `json:"hfresh_relaxed_recall"`
	HFreshConfiguredProbe    int     `json:"hfresh_configured_probe"`
	HFreshEffectiveCentroids float64 `json:"hfresh_effective_centroids_avg"`
	HFreshCandidatesRescored float64 `json:"hfresh_candidates_rescored_avg"`
	HFreshAvgMs              float64 `json:"hfresh_avg_ms"`
}

// ControlledSweepReport is the overall experiment report
type ControlledSweepReport struct {
	Timestamp             string                  `json:"timestamp"`
	NumDocs               int                     `json:"num_docs"`
	NumQueries            int                     `json:"num_queries"`
	NumCentroids          int                     `json:"num_centroids"`
	K                     int                     `json:"k"`
	HNSWOverfetch         int                     `json:"hnsw_overfetch"`
	HFreshRescoreLimit    int                     `json:"hfresh_rescore_limit"`
	HFreshDefaultProbe    int                     `json:"hfresh_default_probe"`
	SweepValues           []int                   `json:"sweep_values"`
	WiringValidation      string                  `json:"wiring_validation"`
	Results               []ControlledSweepResult `json:"results"`
	Interpretation        string                  `json:"interpretation"`
}

func TestHNSWvsHFreshControlledSweep(t *testing.T) {
	if *flagDocsPath == "" {
		t.Skip("Skipping: requires -docs flag")
	}
	if *flagQueriesPath == "" {
		t.Skip("Skipping: requires -queries flag")
	}
	if *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -groundtruth flag")
	}

	ctx := context.Background()

	// Load data
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	gt, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(gt))

	// Configuration - USE DEFAULTS
	k := 10
	hnswOverfetch := 2 // Standard HNSW MUVERA overfetch

	// Use DEFAULT rescoreLimit - this is critical for valid searchProbe sweep
	// Default is 350, NOT 8192
	hfreshRescoreLimit := ent.DefaultHFreshRescoreLimit // = 350

	// Sweep values - include 96 as requested
	sweepValues := []int{16, 32, 64, 96, 128, 256, 512}

	// Build document map for lookups
	docMap := make(map[uint64]*DocInput)
	for i := range docs {
		docMap[docs[i].ID] = &docs[i]
	}

	// Get dimensions from first doc
	var dims int
	if len(docs) > 0 && len(docs[0].Vectors) > 0 && len(docs[0].Vectors[0]) > 0 {
		dims = len(docs[0].Vectors[0])
	}
	require.Greater(t, dims, 0, "could not determine vector dimensions")

	// Create ground truth sets for quick lookup
	// gtStrictSets: only top-k ground truth docs (strict recall@k)
	// gtRelaxedSets: all ground truth docs (relaxed recall - how many returned are somewhere in GT)
	gtStrictSets := make(map[string]map[uint64]struct{})
	gtRelaxedSets := make(map[string]map[uint64]struct{})
	gtOrdered := make(map[string][]uint64) // preserve order for diagnostics
	for qid, gtDocs := range gt {
		gtOrdered[qid] = gtDocs
		gtStrictSets[qid] = make(map[uint64]struct{})
		gtRelaxedSets[qid] = make(map[uint64]struct{})
		for i, docID := range gtDocs {
			if i < k {
				gtStrictSets[qid][docID] = struct{}{}
			}
			gtRelaxedSets[qid][docID] = struct{}{}
		}
	}

	// =========================================================================
	// Create HNSW+MUVERA index
	// =========================================================================
	t.Log("")
	t.Log("Creating HNSW+MUVERA index...")

	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.ErrorLevel)

	// Multi-vector store for HNSW lookups
	mvStore := make(map[uint64][][]float32)

	hnswUC := enthnsw.UserConfig{
		VectorCacheMaxObjects: 1e12,
		MaxConnections:        32,
		EFConstruction:        128,
		EF:                    -1, // Will be set per-sweep
		Multivector: enthnsw.MultivectorConfig{
			Enabled: true,
			MuveraConfig: enthnsw.MuveraConfig{
				Enabled:      true,
				KSim:         enthnsw.DefaultMultivectorKSim,
				DProjections: enthnsw.DefaultMultivectorDProjections,
				Repetitions:  enthnsw.DefaultMultivectorRepetitions,
			},
		},
	}

	hnswCfg := hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hnsw_muvera_sweep",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return nil, fmt.Errorf("single vector not supported")
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			vecs, ok := mvStore[id]
			if !ok {
				return nil, fmt.Errorf("doc %d not found", id)
			}
			return vecs, nil
		},
		MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		AllocChecker:      memwatch.NewDummyMonitor(),
		GetViewThunk:      func() common.BucketView { return &noopBucketView{} },
	}

	hnswStore := testinghelpers.NewDummyStore(t)
	hnswIdx, err := hnsw.New(hnswCfg, hnswUC, cyclemanager.NewCallbackGroupNoop(), hnswStore)
	require.NoError(t, err)

	defer func() {
		_ = hnswIdx.Shutdown(ctx)
	}()

	// =========================================================================
	// Create HFresh+MUVERA index with DEFAULT configuration
	// =========================================================================
	t.Log("Creating HFresh+MUVERA index...")

	hfreshCfg := DefaultConfig()
	hfreshCfg.RootPath = t.TempDir()

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	hfreshCfg.Scheduler = scheduler

	hfreshCfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh_muvera_sweep_centroids",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	hfreshCfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	hfreshCfg.Logger = logger
	hfreshCfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		if doc, ok := docMap[id]; ok && len(doc.Vectors) > 0 {
			return doc.Vectors[0], nil
		}
		return nil, fmt.Errorf("vector %d not found", id)
	}
	hfreshCfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
		if doc, ok := docMap[id]; ok {
			return doc.Vectors, nil
		}
		return nil, fmt.Errorf("multi-vector %d not found", id)
	}

	scheduler.Start()

	// Use NewDefaultUserConfig and only modify what's needed for MUVERA
	hfreshUC := ent.NewDefaultUserConfig()
	// SearchProbe will be set per-sweep, start with default
	// RescoreLimit stays at default (350)
	hfreshUC.Multivector.Enabled = true
	hfreshUC.Multivector.MuveraConfig.Enabled = true
	hfreshUC.Multivector.MuveraConfig.KSim = enthnsw.DefaultMultivectorKSim
	hfreshUC.Multivector.MuveraConfig.DProjections = enthnsw.DefaultMultivectorDProjections
	hfreshUC.Multivector.MuveraConfig.Repetitions = enthnsw.DefaultMultivectorRepetitions

	hfreshStore := testinghelpers.NewDummyStore(t)
	hfreshIdx, err := New(hfreshCfg, hfreshUC, hfreshStore)
	require.NoError(t, err)

	defer func() {
		_ = hfreshIdx.Shutdown(ctx)
		scheduler.Close(ctx)
	}()

	// =========================================================================
	// Insert documents into both indexes
	// =========================================================================
	t.Log("Inserting documents into both indexes...")
	insertStart := time.Now()

	for i, doc := range docs {
		mvStore[doc.ID] = doc.Vectors
		hnswErr := hnswIdx.AddMulti(ctx, doc.ID, doc.Vectors)
		require.NoError(t, hnswErr, "failed to insert doc %d into HNSW", doc.ID)
		hfreshErr := hfreshIdx.AddMulti(ctx, doc.ID, doc.Vectors)
		require.NoError(t, hfreshErr, "failed to insert doc %d into HFresh", doc.ID)
		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)

	numCentroids := hfreshIdx.PostingMap.Size()
	t.Logf("Indexes built in %.2fs, HFresh has %d centroids", time.Since(insertStart).Seconds(), numCentroids)

	// =========================================================================
	// Print effective configuration
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("       CONFIGURATION VERIFICATION")
	t.Log("================================================================================")
	t.Logf("k = %d", k)
	t.Logf("HNSW overfetch = %d (candidates = k * overfetch = %d)", hnswOverfetch, k*hnswOverfetch)
	t.Logf("HFresh rescoreLimit = %d (DEFAULT)", hfreshRescoreLimit)
	t.Logf("HFresh default searchProbe = %d", ent.DefaultSearchProbe)
	t.Logf("HFresh total centroids = %d", numCentroids)
	t.Log("")
	t.Log("MUVERA SEARCH PATH ANALYSIS:")
	t.Log("  SearchByMultiVector calls SearchByVector(queryFlat, rescoreLimit)")
	t.Log("  SearchByVector uses: candidateCentroidNum = max(k, searchProbe)")
	t.Log("    where k = rescoreLimit from MUVERA path")
	t.Logf("  With rescoreLimit=%d:", hfreshRescoreLimit)
	for _, sp := range sweepValues {
		effectiveCentroids := hfreshRescoreLimit
		if sp > hfreshRescoreLimit {
			effectiveCentroids = sp
		}
		dominated := ""
		if sp <= hfreshRescoreLimit {
			dominated = " ← dominated by rescoreLimit"
		}
		t.Logf("    searchProbe=%d → candidateCentroidNum = max(%d, %d) = %d%s",
			sp, hfreshRescoreLimit, sp, effectiveCentroids, dominated)
	}

	// =========================================================================
	// Print sample query breakdowns BEFORE sweep
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("       SAMPLE QUERY RECALL BREAKDOWN")
	t.Log("================================================================================")
	t.Log("")
	t.Log("Metric definitions:")
	t.Logf("  strict_recall@%d   = hits(returned_top_%d ∩ gt_top_%d) / %d", k, k, k, k)
	t.Logf("  relaxed_recall@%d  = hits(returned_top_%d ∩ gt_top_100) / %d", k, k, k)
	t.Log("")

	// Run a quick search with default settings to get sample data
	for qi := 0; qi < 5 && qi < len(queries); qi++ {
		query := queries[qi]
		gtStrict := gtStrictSets[query.ID]
		gtRelaxed := gtRelaxedSets[query.ID]
		gtTop10 := gtOrdered[query.ID][:min(k, len(gtOrdered[query.ID]))]

		// Get results from both indexes
		hnswIDs, _, _ := hnswIdx.SearchByMultiVector(ctx, query.Vectors, k, nil)
		hfreshIDs, _, _ := hfreshIdx.SearchByMultiVector(ctx, query.Vectors, k, nil)

		var hnswStrictHits, hnswRelaxedHits int
		for _, id := range hnswIDs {
			if _, ok := gtStrict[id]; ok {
				hnswStrictHits++
			}
			if _, ok := gtRelaxed[id]; ok {
				hnswRelaxedHits++
			}
		}

		var hfreshStrictHits, hfreshRelaxedHits int
		for _, id := range hfreshIDs {
			if _, ok := gtStrict[id]; ok {
				hfreshStrictHits++
			}
			if _, ok := gtRelaxed[id]; ok {
				hfreshRelaxedHits++
			}
		}

		t.Logf("Query %s:", query.ID)
		t.Logf("  gt_top_10:        %v", gtTop10)
		t.Logf("  HNSW returned:    %v", hnswIDs)
		t.Logf("  HFresh returned:  %v", hfreshIDs)
		t.Logf("  HNSW:   strict_hits=%d/%d (%.2f), relaxed_hits=%d/%d (%.2f)",
			hnswStrictHits, k, float64(hnswStrictHits)/float64(k),
			hnswRelaxedHits, k, float64(hnswRelaxedHits)/float64(k))
		t.Logf("  HFresh: strict_hits=%d/%d (%.2f), relaxed_hits=%d/%d (%.2f)",
			hfreshStrictHits, k, float64(hfreshStrictHits)/float64(k),
			hfreshRelaxedHits, k, float64(hfreshRelaxedHits)/float64(k))
		t.Log("")
	}

	// =========================================================================
	// Run sweep with TraceCollector for verification
	// =========================================================================
	t.Log("================================================================================")
	t.Log("       HNSW vs HFresh CONTROLLED SWEEP")
	t.Log("================================================================================")
	t.Log("")
	t.Log("Value | HNSW strict@10 | HNSW relax@10 | HFresh strict@10 | HFresh relax@10 | Eff.Cent | HNSW ms | HFresh ms")
	t.Log("------|----------------|---------------|------------------|-----------------|----------|---------|----------")

	report := ControlledSweepReport{
		Timestamp:          time.Now().UTC().Format(time.RFC3339),
		NumDocs:            len(docs),
		NumQueries:         len(queries),
		NumCentroids:       numCentroids,
		K:                  k,
		HNSWOverfetch:      hnswOverfetch,
		HFreshRescoreLimit: hfreshRescoreLimit,
		HFreshDefaultProbe: int(ent.DefaultSearchProbe),
		SweepValues:        sweepValues,
		Results:            make([]ControlledSweepResult, 0, len(sweepValues)),
	}

	// Track if searchProbe ever changes effective behavior
	var searchProbeEffective bool

	for _, value := range sweepValues {
		// Set HNSW EF
		err := hnswIdx.UpdateUserConfig(enthnsw.UserConfig{EF: value}, func() {})
		require.NoError(t, err)
		effectiveEF := value
		if effectiveEF < k*hnswOverfetch {
			effectiveEF = k * hnswOverfetch
		}

		// Set HFresh searchProbe
		atomic.StoreUint32(&hfreshIdx.searchProbe, uint32(value))

		// Track if any searchProbe value exceeds rescoreLimit (affects candidateCentroidNum)
		if value > hfreshRescoreLimit {
			searchProbeEffective = true
		}

		var hnswStrictSum, hnswRelaxedSum, hfreshStrictSum, hfreshRelaxedSum float64
		var hnswTimeSum, hfreshTimeSum time.Duration
		var totalCentroidsSearched, totalCandidatesRescored int

		for _, query := range queries {
			gtStrict := gtStrictSets[query.ID]
			gtRelaxed := gtRelaxedSets[query.ID]
			if len(gtStrict) == 0 {
				continue
			}

			// HNSW search
			hnswStart := time.Now()
			hnswIDs, _, err := hnswIdx.SearchByMultiVector(ctx, query.Vectors, k, nil)
			hnswTimeSum += time.Since(hnswStart)
			require.NoError(t, err)

			var hnswStrictHits, hnswRelaxedHits int
			for _, id := range hnswIDs {
				if _, ok := gtStrict[id]; ok {
					hnswStrictHits++
				}
				if _, ok := gtRelaxed[id]; ok {
					hnswRelaxedHits++
				}
			}
			hnswStrictSum += float64(hnswStrictHits) / float64(k)
			hnswRelaxedSum += float64(hnswRelaxedHits) / float64(k)

			// HFresh search with TraceCollector
			collector := NewSearchTraceCollector(query.ID)
			traceCtx := ContextWithTraceCollector(ctx, collector)

			hfreshStart := time.Now()
			hfreshIDs, _, err := hfreshIdx.SearchByMultiVector(traceCtx, query.Vectors, k, nil)
			hfreshTimeSum += time.Since(hfreshStart)
			require.NoError(t, err)

			trace := collector.Trace()
			totalCentroidsSearched += len(trace.SelectedCentroids)
			totalCandidatesRescored += trace.ScanStats.UniqueEnumerated

			var hfreshStrictHits, hfreshRelaxedHits int
			for _, id := range hfreshIDs {
				if _, ok := gtStrict[id]; ok {
					hfreshStrictHits++
				}
				if _, ok := gtRelaxed[id]; ok {
					hfreshRelaxedHits++
				}
			}
			hfreshStrictSum += float64(hfreshStrictHits) / float64(k)
			hfreshRelaxedSum += float64(hfreshRelaxedHits) / float64(k)
		}

		numQ := float64(len(queries))
		avgCentroidsSearched := float64(totalCentroidsSearched) / numQ
		avgCandidatesRescored := float64(totalCandidatesRescored) / numQ

		result := ControlledSweepResult{
			Value:                    value,
			HNSWStrictRecall:         hnswStrictSum / numQ,
			HNSWRelaxedRecall:        hnswRelaxedSum / numQ,
			HNSWEffectiveEF:          effectiveEF,
			HNSWAvgMs:                float64(hnswTimeSum.Milliseconds()) / numQ,
			HFreshStrictRecall:       hfreshStrictSum / numQ,
			HFreshRelaxedRecall:      hfreshRelaxedSum / numQ,
			HFreshConfiguredProbe:    value,
			HFreshEffectiveCentroids: avgCentroidsSearched,
			HFreshCandidatesRescored: avgCandidatesRescored,
			HFreshAvgMs:              float64(hfreshTimeSum.Milliseconds()) / numQ,
		}
		report.Results = append(report.Results, result)

		t.Logf("%-5d | %-14.4f | %-13.4f | %-16.4f | %-15.4f | %-8.0f | %-7.1f | %.1f",
			value,
			result.HNSWStrictRecall, result.HNSWRelaxedRecall,
			result.HFreshStrictRecall, result.HFreshRelaxedRecall,
			avgCentroidsSearched,
			result.HNSWAvgMs, result.HFreshAvgMs)
	}

	// =========================================================================
	// Wiring validation
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("       WIRING VALIDATION")
	t.Log("================================================================================")

	// Check if centroids searched actually varied
	var minCentroids, maxCentroids float64 = 1e9, 0
	for _, r := range report.Results {
		if r.HFreshEffectiveCentroids < minCentroids {
			minCentroids = r.HFreshEffectiveCentroids
		}
		if r.HFreshEffectiveCentroids > maxCentroids {
			maxCentroids = r.HFreshEffectiveCentroids
		}
	}

	centroidVariation := maxCentroids - minCentroids
	var wiringStatus string

	if centroidVariation < 10 {
		wiringStatus = fmt.Sprintf("INVALID: Centroids searched did not vary (%.0f to %.0f). "+
			"searchProbe is dominated by rescoreLimit=%d. "+
			"The sweep does NOT measure searchProbe effect.",
			minCentroids, maxCentroids, hfreshRescoreLimit)
		t.Logf("⚠️  %s", wiringStatus)
	} else if !searchProbeEffective {
		wiringStatus = fmt.Sprintf("PARTIAL: searchProbe values [16-%d] are all ≤ rescoreLimit=%d, "+
			"so candidateCentroidNum = rescoreLimit for most sweep values. "+
			"Only searchProbe > %d would show different behavior.",
			sweepValues[len(sweepValues)-2], hfreshRescoreLimit, hfreshRescoreLimit)
		t.Logf("⚠️  %s", wiringStatus)
	} else {
		wiringStatus = fmt.Sprintf("VALID: Centroids searched varied from %.0f to %.0f. "+
			"searchProbe affects search behavior for values > rescoreLimit=%d.",
			minCentroids, maxCentroids, hfreshRescoreLimit)
		t.Logf("✓  %s", wiringStatus)
	}

	report.WiringValidation = wiringStatus

	// Check if HFresh strict recall varied
	var minStrictRecall, maxStrictRecall float64 = 1, 0
	for _, r := range report.Results {
		if r.HFreshStrictRecall < minStrictRecall {
			minStrictRecall = r.HFreshStrictRecall
		}
		if r.HFreshStrictRecall > maxStrictRecall {
			maxStrictRecall = r.HFreshStrictRecall
		}
	}
	strictRecallVariation := maxStrictRecall - minStrictRecall

	t.Logf("")
	t.Logf("HFresh strict recall range: %.4f to %.4f (variation: %.4f)", minStrictRecall, maxStrictRecall, strictRecallVariation)
	if strictRecallVariation < 0.01 {
		t.Log("⚠️  HFresh strict recall is nearly constant - sweep may not be effective")
	}

	// =========================================================================
	// Summary - using STRICT recall for comparison
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("       SUMMARY (STRICT RECALL@10)")
	t.Log("================================================================================")

	var bestHNSW, bestHFresh ControlledSweepResult
	for _, r := range report.Results {
		if r.HNSWStrictRecall > bestHNSW.HNSWStrictRecall {
			bestHNSW = r
		}
		if r.HFreshStrictRecall > bestHFresh.HFreshStrictRecall {
			bestHFresh = r
		}
	}

	t.Logf("Best HNSW: EF=%d → strict_recall=%.4f, relaxed_recall=%.4f",
		bestHNSW.Value, bestHNSW.HNSWStrictRecall, bestHNSW.HNSWRelaxedRecall)
	t.Logf("Best HFresh: searchProbe=%d → strict_recall=%.4f, relaxed_recall=%.4f (centroids=%.0f)",
		bestHFresh.Value, bestHFresh.HFreshStrictRecall, bestHFresh.HFreshRelaxedRecall, bestHFresh.HFreshEffectiveCentroids)

	var avgStrictDiff float64
	for _, r := range report.Results {
		avgStrictDiff += r.HNSWStrictRecall - r.HFreshStrictRecall
	}
	avgStrictDiff /= float64(len(report.Results))

	var interpretation string
	if math.Abs(avgStrictDiff) < 0.02 {
		interpretation = fmt.Sprintf("COMPARABLE: avg strict recall diff = %.2f%%", avgStrictDiff*100)
	} else if avgStrictDiff > 0 {
		interpretation = fmt.Sprintf("HNSW BETTER: avg strict +%.2f%%", avgStrictDiff*100)
	} else {
		interpretation = fmt.Sprintf("HFRESH BETTER: avg strict +%.2f%%", -avgStrictDiff*100)
	}
	t.Logf("Interpretation (strict recall): %s", interpretation)
	report.Interpretation = interpretation

	// Write report
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/hnsw_vs_hfresh_sweep_v2.json"
	}
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(outputPath, reportJSON, 0644)
	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}

// RescoreLimitSweepResult contains results for a single rescoreLimit configuration.
type RescoreLimitSweepResult struct {
	RescoreLimit         int     `json:"rescore_limit"`
	StrictRecall         float64 `json:"strict_recall"`
	RelaxedRecall        float64 `json:"relaxed_recall"`
	EffectiveCentroids   float64 `json:"effective_centroids_avg"`
	CandidatesRescored   float64 `json:"candidates_rescored_avg"`
	AvgMs                float64 `json:"avg_ms"`
}

// RescoreLimitSweepReport is the overall experiment report.
type RescoreLimitSweepReport struct {
	Timestamp    string                    `json:"timestamp"`
	NumDocs      int                       `json:"num_docs"`
	NumQueries   int                       `json:"num_queries"`
	NumCentroids int                       `json:"num_centroids"`
	K            int                       `json:"k"`
	SearchProbe  int                       `json:"search_probe"`
	SweepValues  []int                     `json:"sweep_values"`
	Results      []RescoreLimitSweepResult `json:"results"`
	Analysis     string                    `json:"analysis"`
}

// TestHFreshRescoreLimitSweep sweeps rescoreLimit to determine if HFresh recall
// is primarily limited by the rescoring budget.
//
// Run with:
//
//	go test -v -timeout 60m -run TestHFreshRescoreLimitSweep ./adapters/repos/db/vector/hfresh/... \
//	  -docs=/tmp/hfresh_diag_lotte_100q/docs.jsonl \
//	  -queries=/tmp/hfresh_diag_lotte_100q/queries.jsonl \
//	  -groundtruth=/tmp/hfresh_diag_lotte_100q/gt.jsonl \
//	  -out=/tmp/hfresh_diag_lotte_100q/rescore_limit_sweep.json
func TestHFreshRescoreLimitSweep(t *testing.T) {
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
	}

	ctx := context.Background()

	// Load data
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	gt, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(gt))

	// Configuration
	k := 10
	searchProbe := 64 // Fixed searchProbe
	rescoreLimitSweep := []int{64, 128, 256, 350, 512, 1024, 2048, 4096}

	// Build document map for lookups
	docMap := make(map[uint64]*DocInput)
	for i := range docs {
		docMap[docs[i].ID] = &docs[i]
	}

	// Get dimensions from first doc
	var dims int
	if len(docs) > 0 && len(docs[0].Vectors) > 0 && len(docs[0].Vectors[0]) > 0 {
		dims = len(docs[0].Vectors[0])
	}
	require.Greater(t, dims, 0, "could not determine vector dimensions")

	// Create ground truth sets (strict = top-k, relaxed = all GT)
	gtStrictSets := make(map[string]map[uint64]struct{})
	gtRelaxedSets := make(map[string]map[uint64]struct{})
	for qid, gtDocs := range gt {
		gtStrictSets[qid] = make(map[uint64]struct{})
		gtRelaxedSets[qid] = make(map[uint64]struct{})
		for i, docID := range gtDocs {
			if i < k {
				gtStrictSets[qid][docID] = struct{}{}
			}
			gtRelaxedSets[qid][docID] = struct{}{}
		}
	}

	// Create multi-vector store for late interaction
	mvStore := make(map[uint64][][]float32)

	// =========================================================================
	// Create HFresh+MUVERA index
	// =========================================================================
	t.Log("")
	t.Log("Creating HFresh+MUVERA index...")

	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.ErrorLevel)

	hfreshCfg := DefaultConfig()
	hfreshCfg.RootPath = t.TempDir()

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	hfreshCfg.Scheduler = scheduler

	hfreshCfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh_rescore_sweep_centroids",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	hfreshCfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	hfreshCfg.Logger = logger
	hfreshCfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		if doc, ok := docMap[id]; ok && len(doc.Vectors) > 0 {
			return doc.Vectors[0], nil
		}
		return nil, fmt.Errorf("vector %d not found", id)
	}
	hfreshCfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
		if doc, ok := docMap[id]; ok {
			return doc.Vectors, nil
		}
		return nil, fmt.Errorf("multi-vector %d not found", id)
	}

	scheduler.Start()

	// Use NewDefaultUserConfig and only modify what's needed
	hfreshUC := ent.NewDefaultUserConfig()
	hfreshUC.SearchProbe = uint32(searchProbe)
	hfreshUC.RQ.RescoreLimit = rescoreLimitSweep[0] // Start with first value, will be changed
	hfreshUC.Multivector.Enabled = true
	hfreshUC.Multivector.MuveraConfig.Enabled = true
	hfreshUC.Multivector.MuveraConfig.KSim = enthnsw.DefaultMultivectorKSim
	hfreshUC.Multivector.MuveraConfig.DProjections = enthnsw.DefaultMultivectorDProjections
	hfreshUC.Multivector.MuveraConfig.Repetitions = enthnsw.DefaultMultivectorRepetitions

	hfreshStore := testinghelpers.NewDummyStore(t)
	hfreshIdx, err := New(hfreshCfg, hfreshUC, hfreshStore)
	require.NoError(t, err)

	defer func() {
		_ = hfreshIdx.Shutdown(ctx)
		scheduler.Close(ctx)
	}()

	// =========================================================================
	// Insert documents
	// =========================================================================
	t.Log("Inserting documents...")
	insertStart := time.Now()

	for i, doc := range docs {
		mvStore[doc.ID] = doc.Vectors
		hfreshErr := hfreshIdx.AddMulti(ctx, doc.ID, doc.Vectors)
		require.NoError(t, hfreshErr, "failed to insert doc %d into HFresh", doc.ID)
		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)

	numCentroids := hfreshIdx.PostingMap.Size()
	t.Logf("Index built in %.2fs, HFresh has %d centroids", time.Since(insertStart).Seconds(), numCentroids)

	// =========================================================================
	// Configuration verification
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("       RESCORE LIMIT SWEEP CONFIGURATION")
	t.Log("================================================================================")
	t.Logf("k = %d", k)
	t.Logf("searchProbe = %d (FIXED)", searchProbe)
	t.Logf("rescoreLimit sweep: %v", rescoreLimitSweep)
	t.Logf("HFresh total centroids = %d", numCentroids)
	t.Log("")
	t.Log("MUVERA SEARCH PATH ANALYSIS:")
	t.Log("  SearchByMultiVector calls SearchByVector(queryFlat, rescoreLimit)")
	t.Log("  SearchByVector uses: candidateCentroidNum = max(rescoreLimit, searchProbe)")
	t.Logf("  With searchProbe=%d:", searchProbe)
	for _, rl := range rescoreLimitSweep {
		effectiveCentroids := rl
		if searchProbe > rl {
			effectiveCentroids = searchProbe
		}
		note := ""
		if rl < searchProbe {
			note = fmt.Sprintf(" ← dominated by searchProbe=%d", searchProbe)
		}
		t.Logf("    rescoreLimit=%d → candidateCentroidNum = max(%d, %d) = %d%s",
			rl, rl, searchProbe, effectiveCentroids, note)
	}

	// =========================================================================
	// Run rescoreLimit sweep
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("       RESCORE LIMIT SWEEP RESULTS")
	t.Log("================================================================================")
	t.Log("")
	t.Log("RescoreLimit | Strict@10 | Relaxed@10 | Eff.Cent | Candidates | Avg ms")
	t.Log("-------------|-----------|------------|----------|------------|-------")

	report := RescoreLimitSweepReport{
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		NumDocs:      len(docs),
		NumQueries:   len(queries),
		NumCentroids: numCentroids,
		K:            k,
		SearchProbe:  searchProbe,
		SweepValues:  rescoreLimitSweep,
		Results:      make([]RescoreLimitSweepResult, 0, len(rescoreLimitSweep)),
	}

	for _, rescoreLimit := range rescoreLimitSweep {
		// Update rescoreLimit atomically
		atomic.StoreUint32(&hfreshIdx.rescoreLimit, uint32(rescoreLimit))

		var strictSum, relaxedSum float64
		var timeSum time.Duration
		var totalCentroidsSearched, totalCandidatesRescored int

		for _, query := range queries {
			gtStrict := gtStrictSets[query.ID]
			gtRelaxed := gtRelaxedSets[query.ID]
			if len(gtStrict) == 0 {
				continue
			}

			// HFresh search with TraceCollector
			collector := NewSearchTraceCollector(query.ID)
			traceCtx := ContextWithTraceCollector(ctx, collector)

			start := time.Now()
			ids, _, err := hfreshIdx.SearchByMultiVector(traceCtx, query.Vectors, k, nil)
			timeSum += time.Since(start)
			require.NoError(t, err)

			trace := collector.Trace()
			totalCentroidsSearched += len(trace.SelectedCentroids)
			totalCandidatesRescored += trace.ScanStats.UniqueEnumerated

			var strictHits, relaxedHits int
			for _, id := range ids {
				if _, ok := gtStrict[id]; ok {
					strictHits++
				}
				if _, ok := gtRelaxed[id]; ok {
					relaxedHits++
				}
			}
			strictSum += float64(strictHits) / float64(k)
			relaxedSum += float64(relaxedHits) / float64(k)
		}

		numQ := float64(len(queries))
		avgCentroids := float64(totalCentroidsSearched) / numQ
		avgCandidates := float64(totalCandidatesRescored) / numQ

		result := RescoreLimitSweepResult{
			RescoreLimit:       rescoreLimit,
			StrictRecall:       strictSum / numQ,
			RelaxedRecall:      relaxedSum / numQ,
			EffectiveCentroids: avgCentroids,
			CandidatesRescored: avgCandidates,
			AvgMs:              float64(timeSum.Milliseconds()) / numQ,
		}
		report.Results = append(report.Results, result)

		t.Logf("%-12d | %-9.4f | %-10.4f | %-8.0f | %-10.0f | %.1f",
			rescoreLimit,
			result.StrictRecall, result.RelaxedRecall,
			avgCentroids, avgCandidates,
			result.AvgMs)
	}

	// =========================================================================
	// Analysis
	// =========================================================================
	t.Log("")
	t.Log("================================================================================")
	t.Log("       ANALYSIS: Is HFresh recall limited by rescoreLimit?")
	t.Log("================================================================================")

	// Find recall at different rescoreLimit values
	var recallAt64, recallAt350, recallAt4096 float64
	for _, r := range report.Results {
		switch r.RescoreLimit {
		case 64:
			recallAt64 = r.StrictRecall
		case 350:
			recallAt350 = r.StrictRecall
		case 4096:
			recallAt4096 = r.StrictRecall
		}
	}

	// Check if recall increases significantly with rescoreLimit
	improvement64to350 := (recallAt350 - recallAt64) / recallAt64 * 100
	improvement350to4096 := (recallAt4096 - recallAt350) / recallAt350 * 100
	totalImprovement := (recallAt4096 - recallAt64) / recallAt64 * 100

	t.Logf("")
	t.Logf("Strict recall at rescoreLimit=64:   %.4f", recallAt64)
	t.Logf("Strict recall at rescoreLimit=350:  %.4f (default)", recallAt350)
	t.Logf("Strict recall at rescoreLimit=4096: %.4f", recallAt4096)
	t.Logf("")
	t.Logf("Improvement 64→350:   %+.1f%%", improvement64to350)
	t.Logf("Improvement 350→4096: %+.1f%%", improvement350to4096)
	t.Logf("Total improvement:    %+.1f%%", totalImprovement)

	var analysis string
	if totalImprovement > 20 {
		analysis = fmt.Sprintf("YES: HFresh recall is significantly limited by rescoreLimit. "+
			"Increasing from 64 to 4096 improves strict recall by %.1f%%. "+
			"The default rescoreLimit=350 may be insufficient for this dataset.",
			totalImprovement)
	} else if totalImprovement > 5 {
		analysis = fmt.Sprintf("PARTIALLY: HFresh recall is somewhat limited by rescoreLimit. "+
			"Increasing from 64 to 4096 improves strict recall by %.1f%%. "+
			"Higher rescoreLimit helps but is not the primary bottleneck.",
			totalImprovement)
	} else {
		analysis = fmt.Sprintf("NO: HFresh recall is NOT primarily limited by rescoreLimit. "+
			"Increasing from 64 to 4096 only improves strict recall by %.1f%%. "+
			"The bottleneck is elsewhere (routing, FDE approximation, etc.).",
			totalImprovement)
	}

	t.Logf("")
	t.Logf("CONCLUSION: %s", analysis)
	report.Analysis = analysis

	// Write report
	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/rescore_limit_sweep.json"
	}
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(outputPath, reportJSON, 0644)
	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}

// TestRescoreLimitSemanticAudit audits the exact semantic role of rescoreLimit
// in HFresh+MUVERA to determine whether it affects:
// 1. Only the final MaxSim rerank budget
// 2. Or also the candidate generation stages (centroid selection, posting scan, etc.)
//
// Run with:
//
//	go test -v -timeout 30m -run TestRescoreLimitSemanticAudit ./adapters/repos/db/vector/hfresh/... \
//	  -docs=/tmp/hfresh_diag_lotte_100q/docs.jsonl \
//	  -queries=/tmp/hfresh_diag_lotte_100q/queries.jsonl \
//	  -groundtruth=/tmp/hfresh_diag_lotte_100q/gt.jsonl
func TestRescoreLimitSemanticAudit(t *testing.T) {
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
	}

	ctx := context.Background()

	// Load data
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	gt, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(gt))

	// Test parameters
	k := 10
	fixedSearchProbe := 64
	rescoreLimits := []int{64, 128, 256, 350, 512, 1024, 2048, 4096}

	// Build document map for lookups
	docMap := make(map[uint64]*DocInput)
	for i := range docs {
		docMap[docs[i].ID] = &docs[i]
	}

	// Get dimensions from first doc
	var dims int
	if len(docs) > 0 && len(docs[0].Vectors) > 0 && len(docs[0].Vectors[0]) > 0 {
		dims = len(docs[0].Vectors[0])
	}
	require.Greater(t, dims, 0, "could not determine vector dimensions")

	// Build GT sets for recall computation
	gtStrictSets := make(map[string]map[uint64]struct{})
	gtRelaxedSets := make(map[string]map[uint64]struct{})
	for qid, gtIDs := range gt {
		strictSet := make(map[uint64]struct{})
		relaxedSet := make(map[uint64]struct{})
		for i, id := range gtIDs {
			if i < k {
				strictSet[id] = struct{}{}
			}
			if i < 100 {
				relaxedSet[id] = struct{}{}
			}
		}
		gtStrictSets[qid] = strictSet
		gtRelaxedSets[qid] = relaxedSet
	}

	// Create HFresh index
	t.Log("")
	t.Log("Creating HFresh+MUVERA index...")

	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.ErrorLevel)

	hfreshCfg := DefaultConfig()
	hfreshCfg.RootPath = t.TempDir()

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	hfreshCfg.Scheduler = scheduler

	hfreshCfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh_semantic_audit_centroids",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	hfreshCfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	hfreshCfg.Logger = logger
	hfreshCfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		if doc, ok := docMap[id]; ok && len(doc.Vectors) > 0 {
			return doc.Vectors[0], nil
		}
		return nil, fmt.Errorf("vector %d not found", id)
	}
	hfreshCfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
		if doc, ok := docMap[id]; ok {
			return doc.Vectors, nil
		}
		return nil, fmt.Errorf("multi-vector %d not found", id)
	}

	scheduler.Start()

	// Use NewDefaultUserConfig and configure for MUVERA
	hfreshUC := ent.NewDefaultUserConfig()
	hfreshUC.SearchProbe = uint32(fixedSearchProbe)
	hfreshUC.RQ.RescoreLimit = rescoreLimits[0] // Start with first value
	hfreshUC.Multivector.Enabled = true
	hfreshUC.Multivector.MuveraConfig.Enabled = true
	hfreshUC.Multivector.MuveraConfig.KSim = enthnsw.DefaultMultivectorKSim
	hfreshUC.Multivector.MuveraConfig.DProjections = enthnsw.DefaultMultivectorDProjections
	hfreshUC.Multivector.MuveraConfig.Repetitions = enthnsw.DefaultMultivectorRepetitions

	hfreshStore := testinghelpers.NewDummyStore(t)
	hfreshIdx, err := New(hfreshCfg, hfreshUC, hfreshStore)
	require.NoError(t, err)

	defer func() {
		_ = hfreshIdx.Shutdown(ctx)
		scheduler.Close(ctx)
	}()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()

	for i, doc := range docs {
		hfreshErr := hfreshIdx.AddMulti(ctx, doc.ID, doc.Vectors)
		require.NoError(t, hfreshErr, "failed to insert doc %d into HFresh", doc.ID)
		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)

	numCentroids := hfreshIdx.PostingMap.Size()
	t.Logf("Index built in %.2fs, HFresh has %d centroids", time.Since(insertStart).Seconds(), numCentroids)

	// Result struct
	type SemanticAuditResult struct {
		RescoreLimit         int     `json:"rescore_limit"`
		CandidateCentroidNum int     `json:"candidate_centroid_num"`
		SelectedCentroids    float64 `json:"selected_centroids_avg"`
		UniqueEnumerated     float64 `json:"unique_enumerated_avg"`
		TotalScanned         float64 `json:"total_scanned_avg"`
		CandidatesPassed     float64 `json:"candidates_passed_to_maxsim_avg"`
		StrictRecall         float64 `json:"strict_recall"`
		RelaxedRecall        float64 `json:"relaxed_recall"`
		AvgLatencyMs         float64 `json:"avg_latency_ms"`
	}

	var results []SemanticAuditResult

	// Header
	t.Logf("")
	t.Logf("================================================================================")
	t.Logf("       RESCORE LIMIT SEMANTIC AUDIT")
	t.Logf("================================================================================")
	t.Logf("k = %d", k)
	t.Logf("searchProbe = %d (FIXED)", fixedSearchProbe)
	t.Logf("rescoreLimit sweep: %v", rescoreLimits)
	t.Logf("HFresh total centroids = %d", numCentroids)
	t.Logf("")
	t.Logf("ANALYSIS: candidateCentroidNum = max(rescoreLimit, searchProbe)")
	t.Logf("  When rescoreLimit >= searchProbe: candidateCentroidNum = rescoreLimit")
	t.Logf("  This means rescoreLimit affects BOTH centroid selection AND MaxSim budget")
	t.Logf("")
	t.Logf("================================================================================")
	t.Logf("       DETAILED METRICS PER RESCORE LIMIT")
	t.Logf("================================================================================")
	t.Logf("")
	t.Logf("RescoreLimit | CandCentNum | SelectedCent | UniqueEnum | TotalScan | MaxSimCand | Strict@%d | Relax@%d | ms", k, k)
	t.Logf("-------------|-------------|--------------|------------|-----------|------------|----------|---------|----")

	for _, rescoreLimit := range rescoreLimits {
		// Compute expected candidateCentroidNum
		expectedCandidateCentroidNum := max(rescoreLimit, fixedSearchProbe)

		// Override rescoreLimit atomically
		atomic.StoreUint32(&hfreshIdx.rescoreLimit, uint32(rescoreLimit))

		var strictSum, relaxedSum float64
		var totalSelectedCentroids, totalUniqueEnumerated, totalScanned int
		var timeSum time.Duration
		validQueries := 0

		for _, query := range queries {
			gtStrict := gtStrictSets[query.ID]
			gtRelaxed := gtRelaxedSets[query.ID]
			if len(gtStrict) == 0 {
				continue
			}

			// Create trace collector for this query
			collector := NewSearchTraceCollector(query.ID)
			traceCtx := ContextWithTraceCollector(ctx, collector)

			start := time.Now()
			ids, _, err := hfreshIdx.SearchByMultiVector(traceCtx, query.Vectors, k, nil)
			timeSum += time.Since(start)
			require.NoError(t, err)

			trace := collector.Trace()
			totalSelectedCentroids += len(trace.SelectedCentroids)
			totalUniqueEnumerated += trace.ScanStats.UniqueEnumerated
			totalScanned += trace.ScanStats.TotalScanned

			var strictHits, relaxedHits int
			for _, id := range ids {
				if _, ok := gtStrict[id]; ok {
					strictHits++
				}
				if _, ok := gtRelaxed[id]; ok {
					relaxedHits++
				}
			}
			strictSum += float64(strictHits) / float64(k)
			relaxedSum += float64(relaxedHits) / float64(k)
			validQueries++
		}

		if validQueries == 0 {
			t.Logf("No valid queries for rescoreLimit=%d", rescoreLimit)
			continue
		}

		numQ := float64(validQueries)
		avgSelectedCentroids := float64(totalSelectedCentroids) / numQ
		avgUniqueEnumerated := float64(totalUniqueEnumerated) / numQ
		avgTotalScanned := float64(totalScanned) / numQ
		avgLatencyMs := float64(timeSum.Milliseconds()) / numQ
		strictRecall := strictSum / numQ
		relaxedRecall := relaxedSum / numQ

		// The candidates passed to MaxSim = min(uniqueEnumerated, rescoreLimit)
		candidatesPassedToMaxSim := math.Min(avgUniqueEnumerated, float64(rescoreLimit))

		result := SemanticAuditResult{
			RescoreLimit:         rescoreLimit,
			CandidateCentroidNum: expectedCandidateCentroidNum,
			SelectedCentroids:    avgSelectedCentroids,
			UniqueEnumerated:     avgUniqueEnumerated,
			TotalScanned:         avgTotalScanned,
			CandidatesPassed:     candidatesPassedToMaxSim,
			StrictRecall:         strictRecall,
			RelaxedRecall:        relaxedRecall,
			AvgLatencyMs:         avgLatencyMs,
		}
		results = append(results, result)

		t.Logf("%-12d | %-11d | %-12.0f | %-10.0f | %-9.0f | %-10.0f | %.4f   | %.4f  | %.1f",
			rescoreLimit,
			expectedCandidateCentroidNum,
			avgSelectedCentroids,
			avgUniqueEnumerated,
			avgTotalScanned,
			candidatesPassedToMaxSim,
			strictRecall,
			relaxedRecall,
			avgLatencyMs)
	}

	// Analysis
	t.Logf("")
	t.Logf("================================================================================")
	t.Logf("       SEMANTIC ANALYSIS")
	t.Logf("================================================================================")
	t.Logf("")

	if len(results) >= 2 {
		first := results[0]
		last := results[len(results)-1]

		centroidIncrease := last.SelectedCentroids - first.SelectedCentroids
		uniqueEnumIncrease := last.UniqueEnumerated - first.UniqueEnumerated
		totalScanIncrease := last.TotalScanned - first.TotalScanned

		t.Logf("From rescoreLimit=%d to rescoreLimit=%d:", first.RescoreLimit, last.RescoreLimit)
		t.Logf("  - SelectedCentroids:  %.0f → %.0f (Δ = %.0f)", first.SelectedCentroids, last.SelectedCentroids, centroidIncrease)
		t.Logf("  - UniqueEnumerated:   %.0f → %.0f (Δ = %.0f)", first.UniqueEnumerated, last.UniqueEnumerated, uniqueEnumIncrease)
		t.Logf("  - TotalScanned:       %.0f → %.0f (Δ = %.0f)", first.TotalScanned, last.TotalScanned, totalScanIncrease)
		t.Logf("  - CandidatesPassedToMaxSim: %.0f → %.0f", first.CandidatesPassed, last.CandidatesPassed)
		t.Logf("  - StrictRecall: %.4f → %.4f (Δ = %.4f)", first.StrictRecall, last.StrictRecall, last.StrictRecall-first.StrictRecall)
		t.Logf("")

		if centroidIncrease > 0 {
			t.Logf("FINDING: rescoreLimit DOES affect centroid selection.")
			t.Logf("  candidateCentroidNum = max(rescoreLimit, searchProbe)")
			t.Logf("  Since rescoreLimit > searchProbe for most values, rescoreLimit directly")
			t.Logf("  controls how many centroids are searched.")
		} else {
			t.Logf("FINDING: rescoreLimit does NOT affect centroid selection.")
			t.Logf("  searchProbe dominates the centroid selection.")
		}

		t.Logf("")
		if totalScanIncrease > 0 {
			t.Logf("FINDING: rescoreLimit DOES affect posting scan count.")
			t.Logf("  More centroids → more posting lists scanned → more vectors scanned.")
		} else {
			t.Logf("FINDING: rescoreLimit does NOT affect posting scan count.")
		}

		t.Logf("")
		if uniqueEnumIncrease > 0 {
			t.Logf("FINDING: rescoreLimit DOES affect unique candidate enumeration.")
			t.Logf("  More centroids/scans → more unique candidates discovered.")
		} else {
			t.Logf("FINDING: rescoreLimit does NOT significantly affect unique candidate enumeration.")
		}

		t.Logf("")
		t.Logf("CONCLUSION:")
		t.Logf("  rescoreLimit is NOT just a final MaxSim budget.")
		t.Logf("  In the MUVERA path, rescoreLimit is passed as 'k' to SearchByVector,")
		t.Logf("  where candidateCentroidNum = max(k, searchProbe) = max(rescoreLimit, searchProbe).")
		t.Logf("  This means increasing rescoreLimit increases the search breadth at EVERY stage:")
		t.Logf("    1. Centroids selected")
		t.Logf("    2. Posting lists scanned")
		t.Logf("    3. Unique candidates enumerated")
		t.Logf("    4. RQ1-ranked candidates")
		t.Logf("    5. Final MaxSim reranking budget")
	}

	// Write report
	type Report struct {
		Config struct {
			K             int   `json:"k"`
			SearchProbe   int   `json:"search_probe"`
			RescoreLimits []int `json:"rescore_limits"`
			TotalCentroid int   `json:"total_centroids"`
		} `json:"config"`
		Results  []SemanticAuditResult `json:"results"`
		Analysis string                `json:"analysis"`
	}

	report := Report{}
	report.Config.K = k
	report.Config.SearchProbe = fixedSearchProbe
	report.Config.RescoreLimits = rescoreLimits
	report.Config.TotalCentroid = numCentroids
	report.Results = results
	report.Analysis = "rescoreLimit affects ALL stages: centroid selection, posting scan, candidate enumeration, and MaxSim budget"

	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/rescore_limit_semantic_audit.json"
	}
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(outputPath, reportJSON, 0644)
	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}

// TestRoutingVsRerankBudgetGrid runs a 2D grid experiment to decouple:
//   - routingBudget: controls centroid selection and posting scan
//   - rerankBudget: controls RQ1 ranking depth and MaxSim candidates
//
// This allows us to measure which budget matters more for recall loss:
//   - Routing/candidate generation failure (GT docs not scanned)
//   - Approximate ranking / rescoring budget failure (GT docs scanned but ranked below cutoff)
//
// Run with:
//
//	go test -v -timeout 120m -run TestRoutingVsRerankBudgetGrid ./adapters/repos/db/vector/hfresh/... \
//	  -docs=/tmp/hfresh_diag_lotte_100q/docs.jsonl \
//	  -queries=/tmp/hfresh_diag_lotte_100q/queries.jsonl \
//	  -groundtruth=/tmp/hfresh_diag_lotte_100q/gt.jsonl
func TestRoutingVsRerankBudgetGrid(t *testing.T) {
	if *flagDocsPath == "" || *flagQueriesPath == "" || *flagGroundTruthPath == "" {
		t.Skip("Skipping: requires -docs, -queries, and -groundtruth flags")
	}

	ctx := context.Background()

	// Load data
	t.Log("Loading documents...")
	docs, err := loadDocs(*flagDocsPath)
	require.NoError(t, err)
	t.Logf("Loaded %d documents", len(docs))

	t.Log("Loading queries...")
	queries, err := loadQueries(*flagQueriesPath)
	require.NoError(t, err)
	t.Logf("Loaded %d queries", len(queries))

	t.Log("Loading ground-truth...")
	gt, err := loadGroundTruth(*flagGroundTruthPath)
	require.NoError(t, err)
	t.Logf("Loaded ground-truth for %d queries", len(gt))

	// Test parameters
	k := 10
	budgetValues := []int{64, 128, 256, 350, 512, 1024, 2048, 4096}

	// Build document map for lookups
	docMap := make(map[uint64]*DocInput)
	for i := range docs {
		docMap[docs[i].ID] = &docs[i]
	}

	// Get dimensions from first doc
	var dims int
	if len(docs) > 0 && len(docs[0].Vectors) > 0 && len(docs[0].Vectors[0]) > 0 {
		dims = len(docs[0].Vectors[0])
	}
	require.Greater(t, dims, 0, "could not determine vector dimensions")

	// Build GT sets for recall computation
	gtStrictSets := make(map[string]map[uint64]struct{})
	gtRelaxedSets := make(map[string]map[uint64]struct{})
	for qid, gtIDs := range gt {
		strictSet := make(map[uint64]struct{})
		relaxedSet := make(map[uint64]struct{})
		for i, id := range gtIDs {
			if i < k {
				strictSet[id] = struct{}{}
			}
			if i < 100 {
				relaxedSet[id] = struct{}{}
			}
		}
		gtStrictSets[qid] = strictSet
		gtRelaxedSets[qid] = relaxedSet
	}

	// Create HFresh index
	t.Log("")
	t.Log("Creating HFresh+MUVERA index...")

	logger, _ := test.NewNullLogger()
	logger.SetLevel(logrus.ErrorLevel)

	hfreshCfg := DefaultConfig()
	hfreshCfg.RootPath = t.TempDir()

	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: logger,
		},
	)
	hfreshCfg.Scheduler = scheduler

	hfreshCfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "hfresh_routing_vs_rerank_centroids",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewL2SquaredProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}

	hfreshCfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	hfreshCfg.Logger = logger
	hfreshCfg.VectorForIDThunk = func(ctx context.Context, id uint64) ([]float32, error) {
		if doc, ok := docMap[id]; ok && len(doc.Vectors) > 0 {
			return doc.Vectors[0], nil
		}
		return nil, fmt.Errorf("vector %d not found", id)
	}
	hfreshCfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
		if doc, ok := docMap[id]; ok {
			return doc.Vectors, nil
		}
		return nil, fmt.Errorf("multi-vector %d not found", id)
	}

	scheduler.Start()

	// Use NewDefaultUserConfig and configure for MUVERA
	hfreshUC := ent.NewDefaultUserConfig()
	hfreshUC.SearchProbe = 64 // Will be overridden by routingBudget in diagnostic search
	hfreshUC.RQ.RescoreLimit = 350 // Will be overridden by rerankBudget in diagnostic search
	hfreshUC.Multivector.Enabled = true
	hfreshUC.Multivector.MuveraConfig.Enabled = true
	hfreshUC.Multivector.MuveraConfig.KSim = enthnsw.DefaultMultivectorKSim
	hfreshUC.Multivector.MuveraConfig.DProjections = enthnsw.DefaultMultivectorDProjections
	hfreshUC.Multivector.MuveraConfig.Repetitions = enthnsw.DefaultMultivectorRepetitions

	hfreshStore := testinghelpers.NewDummyStore(t)
	hfreshIdx, err := New(hfreshCfg, hfreshUC, hfreshStore)
	require.NoError(t, err)

	defer func() {
		_ = hfreshIdx.Shutdown(ctx)
		scheduler.Close(ctx)
	}()

	// Insert documents
	t.Log("Inserting documents...")
	insertStart := time.Now()

	for i, doc := range docs {
		hfreshErr := hfreshIdx.AddMulti(ctx, doc.ID, doc.Vectors)
		require.NoError(t, hfreshErr, "failed to insert doc %d into HFresh", doc.ID)
		if (i+1)%10000 == 0 || i == len(docs)-1 {
			t.Logf("Inserted %d/%d documents", i+1, len(docs))
		}
	}
	time.Sleep(200 * time.Millisecond)

	numCentroids := hfreshIdx.PostingMap.Size()
	t.Logf("Index built in %.2fs, HFresh has %d centroids", time.Since(insertStart).Seconds(), numCentroids)

	// Result struct for grid experiment
	type GridResult struct {
		RoutingBudget        int     `json:"routing_budget"`
		RerankBudget         int     `json:"rerank_budget"`
		StrictRecall         float64 `json:"strict_recall"`
		RelaxedRecall        float64 `json:"relaxed_recall"`
		AvgSelectedCentroids float64 `json:"avg_selected_centroids"`
		AvgUniqueDocs        float64 `json:"avg_unique_docs"`
		AvgScanned           float64 `json:"avg_scanned"`
		AvgMaxSimCandidates  float64 `json:"avg_maxsim_candidates"`
		AvgMs                float64 `json:"avg_ms"`
	}

	var results []GridResult

	// Header
	t.Logf("")
	t.Logf("================================================================================")
	t.Logf("       ROUTING vs RERANK BUDGET 2D GRID EXPERIMENT")
	t.Logf("================================================================================")
	t.Logf("k = %d", k)
	t.Logf("Budget values: %v", budgetValues)
	t.Logf("HFresh total centroids = %d", numCentroids)
	t.Logf("")
	t.Logf("This experiment decouples two effects currently tied to rescoreLimit:")
	t.Logf("  - routingBudget: controls centroid selection and posting scan")
	t.Logf("  - rerankBudget: controls RQ1 ranking depth and MaxSim candidates")
	t.Logf("")
	t.Logf("================================================================================")
	t.Logf("       GRID RESULTS")
	t.Logf("================================================================================")
	t.Logf("")
	t.Logf("RoutingBudget | RerankBudget | Strict@%d | Relax@%d | UniqueDocs | Scanned | MaxSimCand | ms", k, k)
	t.Logf("--------------|--------------|----------|---------|------------|---------|------------|----")

	for _, routingBudget := range budgetValues {
		for _, rerankBudget := range budgetValues {
			var strictSum, relaxedSum float64
			var totalSelectedCentroids, totalUniqueDocs, totalScanned, totalMaxSimCandidates int
			var timeSum time.Duration
			validQueries := 0

			for _, query := range queries {
				gtStrict := gtStrictSets[query.ID]
				gtRelaxed := gtRelaxedSets[query.ID]
				if len(gtStrict) == 0 {
					continue
				}

				start := time.Now()
				ids, _, trace, err := hfreshIdx.SearchByMultiVectorDiagnostic(ctx, query.Vectors, k, routingBudget, rerankBudget, nil)
				timeSum += time.Since(start)
				require.NoError(t, err)

				if trace != nil {
					totalSelectedCentroids += trace.SelectedCentroids
					totalUniqueDocs += trace.UniqueDocsEnumerated
					totalScanned += trace.TotalVectorsScanned
					totalMaxSimCandidates += trace.CandidatesPassedToMaxSim
				}

				var strictHits, relaxedHits int
				for _, id := range ids {
					if _, ok := gtStrict[id]; ok {
						strictHits++
					}
					if _, ok := gtRelaxed[id]; ok {
						relaxedHits++
					}
				}
				strictSum += float64(strictHits) / float64(k)
				relaxedSum += float64(relaxedHits) / float64(k)
				validQueries++
			}

			if validQueries == 0 {
				continue
			}

			numQ := float64(validQueries)
			result := GridResult{
				RoutingBudget:        routingBudget,
				RerankBudget:         rerankBudget,
				StrictRecall:         strictSum / numQ,
				RelaxedRecall:        relaxedSum / numQ,
				AvgSelectedCentroids: float64(totalSelectedCentroids) / numQ,
				AvgUniqueDocs:        float64(totalUniqueDocs) / numQ,
				AvgScanned:           float64(totalScanned) / numQ,
				AvgMaxSimCandidates:  float64(totalMaxSimCandidates) / numQ,
				AvgMs:                float64(timeSum.Milliseconds()) / numQ,
			}
			results = append(results, result)

			t.Logf("%-13d | %-12d | %.4f   | %.4f  | %-10.0f | %-7.0f | %-10.0f | %.1f",
				routingBudget,
				rerankBudget,
				result.StrictRecall,
				result.RelaxedRecall,
				result.AvgUniqueDocs,
				result.AvgScanned,
				result.AvgMaxSimCandidates,
				result.AvgMs)
		}
	}

	// Analysis
	t.Logf("")
	t.Logf("================================================================================")
	t.Logf("       ANALYSIS")
	t.Logf("================================================================================")
	t.Logf("")

	// Helper to find result
	findResult := func(routing, rerank int) *GridResult {
		for i := range results {
			if results[i].RoutingBudget == routing && results[i].RerankBudget == rerank {
				return &results[i]
			}
		}
		return nil
	}

	// Analysis 1: Fixed rerankBudget=350, vary routingBudget
	t.Logf("ANALYSIS 1: Routing impact (rerankBudget=350 fixed)")
	t.Logf("-----------------------------------------------------")
	fixedRerank := 350
	for _, routing := range budgetValues {
		r := findResult(routing, fixedRerank)
		if r != nil {
			t.Logf("  routingBudget=%4d: strict=%.4f, relaxed=%.4f, uniqueDocs=%.0f",
				routing, r.StrictRecall, r.RelaxedRecall, r.AvgUniqueDocs)
		}
	}

	// Analysis 2: Fixed routingBudget=350, vary rerankBudget
	t.Logf("")
	t.Logf("ANALYSIS 2: Rerank impact (routingBudget=350 fixed)")
	t.Logf("-----------------------------------------------------")
	fixedRouting := 350
	for _, rerank := range budgetValues {
		r := findResult(fixedRouting, rerank)
		if r != nil {
			t.Logf("  rerankBudget=%4d: strict=%.4f, relaxed=%.4f, maxSimCand=%.0f",
				rerank, r.StrictRecall, r.RelaxedRecall, r.AvgMaxSimCandidates)
		}
	}

	// Analysis 3: Key comparisons
	t.Logf("")
	t.Logf("ANALYSIS 3: Key comparisons")
	t.Logf("----------------------------")

	baseline := findResult(350, 350)
	highRouting := findResult(4096, 350)
	highRerank := findResult(350, 4096)
	highBoth := findResult(4096, 4096)

	if baseline != nil {
		t.Logf("Baseline (350, 350):      strict=%.4f, relaxed=%.4f", baseline.StrictRecall, baseline.RelaxedRecall)
	}
	if highRouting != nil {
		t.Logf("High routing (4096, 350): strict=%.4f, relaxed=%.4f", highRouting.StrictRecall, highRouting.RelaxedRecall)
		if baseline != nil {
			t.Logf("  Δ from baseline: strict=%+.4f, relaxed=%+.4f",
				highRouting.StrictRecall-baseline.StrictRecall,
				highRouting.RelaxedRecall-baseline.RelaxedRecall)
		}
	}
	if highRerank != nil {
		t.Logf("High rerank (350, 4096):  strict=%.4f, relaxed=%.4f", highRerank.StrictRecall, highRerank.RelaxedRecall)
		if baseline != nil {
			t.Logf("  Δ from baseline: strict=%+.4f, relaxed=%+.4f",
				highRerank.StrictRecall-baseline.StrictRecall,
				highRerank.RelaxedRecall-baseline.RelaxedRecall)
		}
	}
	if highBoth != nil {
		t.Logf("High both (4096, 4096):   strict=%.4f, relaxed=%.4f", highBoth.StrictRecall, highBoth.RelaxedRecall)
		if baseline != nil {
			t.Logf("  Δ from baseline: strict=%+.4f, relaxed=%+.4f",
				highBoth.StrictRecall-baseline.StrictRecall,
				highBoth.RelaxedRecall-baseline.RelaxedRecall)
		}
	}

	// Determine dominant factor
	t.Logf("")
	t.Logf("CONCLUSION:")
	t.Logf("-----------")
	if highRouting != nil && highRerank != nil && baseline != nil {
		routingGain := highRouting.StrictRecall - baseline.StrictRecall
		rerankGain := highRerank.StrictRecall - baseline.StrictRecall

		if routingGain > rerankGain*1.5 {
			t.Logf("ROUTING DOMINATES: Increasing routingBudget (4096 vs 350) gives +%.4f strict recall,", routingGain)
			t.Logf("  while increasing rerankBudget gives only +%.4f.", rerankGain)
			t.Logf("  → HFresh recall loss is primarily due to insufficient centroid/posting coverage.")
		} else if rerankGain > routingGain*1.5 {
			t.Logf("RERANK DOMINATES: Increasing rerankBudget (4096 vs 350) gives +%.4f strict recall,", rerankGain)
			t.Logf("  while increasing routingBudget gives only +%.4f.", routingGain)
			t.Logf("  → HFresh recall loss is primarily due to RQ1 compression / approximate ranking.")
		} else {
			t.Logf("BOTH MATTER: Increasing routingBudget gives +%.4f, rerankBudget gives +%.4f.", routingGain, rerankGain)
			t.Logf("  → The current rescoreLimit improvement comes from widening the whole funnel.")
			t.Logf("  → Cannot describe it as simply 'more rescoring'.")
		}
	}

	// Write report
	type Report struct {
		Config struct {
			K             int   `json:"k"`
			BudgetValues  []int `json:"budget_values"`
			TotalCentroid int   `json:"total_centroids"`
			NumDocs       int   `json:"num_docs"`
			NumQueries    int   `json:"num_queries"`
		} `json:"config"`
		Results []GridResult `json:"results"`
	}

	report := Report{}
	report.Config.K = k
	report.Config.BudgetValues = budgetValues
	report.Config.TotalCentroid = numCentroids
	report.Config.NumDocs = len(docs)
	report.Config.NumQueries = len(queries)
	report.Results = results

	outputPath := *flagOutputPath
	if outputPath == "" {
		outputPath = "/tmp/hfresh_diag_lotte_100q/routing_vs_rerank_grid.json"
	}
	reportJSON, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(outputPath, reportJSON, 0644)
	t.Logf("")
	t.Logf("Report written to: %s", outputPath)
}
