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
	"os"
	"sort"
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
	flagDocsPath        = flag.String("docs", "", "Path to documents JSONL file")
	flagQueriesPath     = flag.String("queries", "", "Path to queries JSONL file")
	flagGroundTruthPath = flag.String("groundtruth", "", "Path to ground-truth JSONL file")
	flagK               = flag.Int("k", 10, "Number of results to return per query")
	flagSearchProbe     = flag.Int("searchprobe", 64, "Number of centroids to probe")
	flagRescoreLimit    = flag.Int("rescorelimit", 100, "Maximum candidates for rescoring")
	flagOutputPath      = flag.String("out", "", "Path to output report JSON file")
	flagVerbose         = flag.Bool("verbose", false, "Enable verbose logging")
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
