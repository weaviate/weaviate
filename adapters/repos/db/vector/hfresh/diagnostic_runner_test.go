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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// DiagnosticConfig configures the diagnostic runner.
type DiagnosticConfig struct {
	// NumDocs is the number of documents to insert.
	NumDocs int

	// VectorsPerDoc is the number of vectors per document (multi-vector).
	VectorsPerDoc int

	// VectorDims is the dimensionality of each vector.
	VectorDims int

	// NumQueries is the number of queries to run.
	NumQueries int

	// K is the number of results to return per query.
	K int

	// GroundTruthK is the number of ground-truth results per query.
	// Must be >= K.
	GroundTruthK int

	// OutputDir is the directory to write JSONL traces and reports.
	OutputDir string
}

// DefaultDiagnosticConfig returns a configuration suitable for quick testing.
func DefaultDiagnosticConfig() DiagnosticConfig {
	return DiagnosticConfig{
		NumDocs:       100,
		VectorsPerDoc: 4,
		VectorDims:    128, // Must match MUVERA encoder expectations
		NumQueries:    10,
		K:             10,
		GroundTruthK:  10,
		OutputDir:     "",
	}
}

// DiagnosticTrace is the trace format exported to JSONL.
type DiagnosticTrace struct {
	QueryID           string    `json:"query_id"`
	SelectedCentroids []uint64  `json:"selected_centroids"`
	ApproxTopIDs      []uint64  `json:"approx_top_ids"`
	ReturnedIDs       []uint64  `json:"returned_ids"`
	ScanStats         ScanStats `json:"scan_stats"`
	GroundTruthIDs    []uint64  `json:"ground_truth_ids,omitempty"`
	TimestampNs       int64     `json:"timestamp_ns"`
}

// DiagnosticReport summarizes recall attribution across all queries.
type DiagnosticReport struct {
	Config           DiagnosticConfig `json:"config"`
	TotalQueries     int              `json:"total_queries"`
	TotalGroundTruth int              `json:"total_ground_truth"`
	RoutingFailures  int              `json:"routing_failures"`
	ApproxFailures   int              `json:"approx_failures"`
	ExactFailures    int              `json:"exact_failures"`
	Successes        int              `json:"successes"`
	Recall           float64          `json:"recall"`
	AvgRecall        float64          `json:"avg_recall"`
	PerQueryRecall   []float64        `json:"per_query_recall"`
}

// PostingMembership maps docID -> list of posting/centroid IDs.
type PostingMembership map[uint64][]uint64

// BuildPostingMembership constructs the posting membership map from the index.
func BuildPostingMembership(h *HFresh) PostingMembership {
	membership := make(PostingMembership)

	for postingID, metadata := range h.PostingMap.Iter() {
		for docID := range metadata.Iter() {
			membership[docID] = append(membership[docID], postingID)
		}
	}

	return membership
}

// ClassifyRecallFailures classifies each ground-truth ID into failure categories.
func ClassifyRecallFailures(
	groundTruth []uint64,
	selectedCentroids []uint64,
	approxTopIDs []uint64,
	returnedIDs []uint64,
	membership PostingMembership,
) (routingFailures, approxFailures, exactFailures, successes []uint64) {
	// Build sets for fast lookup
	selectedSet := make(map[uint64]bool)
	for _, c := range selectedCentroids {
		selectedSet[c] = true
	}

	approxSet := make(map[uint64]bool)
	for _, id := range approxTopIDs {
		approxSet[id] = true
	}

	returnedSet := make(map[uint64]bool)
	for _, id := range returnedIDs {
		returnedSet[id] = true
	}

	for _, gtID := range groundTruth {
		// Check if doc is in any selected posting
		docPostings := membership[gtID]
		inSelectedPosting := false
		for _, postingID := range docPostings {
			if selectedSet[postingID] {
				inSelectedPosting = true
				break
			}
		}

		if !inSelectedPosting {
			routingFailures = append(routingFailures, gtID)
			continue
		}

		if !approxSet[gtID] {
			approxFailures = append(approxFailures, gtID)
			continue
		}

		if !returnedSet[gtID] {
			exactFailures = append(exactFailures, gtID)
			continue
		}

		successes = append(successes, gtID)
	}

	return
}

// runDiagnostic runs the full diagnostic workflow.
func runDiagnostic(t *testing.T, cfg DiagnosticConfig) (*DiagnosticReport, []DiagnosticTrace) {
	t.Helper()

	// Create MUVERA-enabled HFresh index
	tf := createMuveraHFreshIndex(t)
	defer func() {
		_ = tf.Index.Shutdown(context.Background())
	}()

	// Generate and insert documents
	docs := generateTestMultiVectors(cfg.NumDocs, cfg.VectorsPerDoc, cfg.VectorDims)
	for docID, vectors := range docs {
		addMultiVectorToIndex(t, &tf, docID, vectors)
	}

	// Wait for background operations to settle
	time.Sleep(100 * time.Millisecond)

	// Build posting membership map
	membership := BuildPostingMembership(tf.Index)

	// Generate queries and ground-truth
	queries := generateTestMultiVectors(cfg.NumQueries, cfg.VectorsPerDoc, cfg.VectorDims)

	// For this diagnostic, we'll use simple ground-truth:
	// The ground-truth is the k documents with smallest docIDs that exist.
	// In a real benchmark, you'd compute brute-force MaxSim.
	// Build a consistent list of doc IDs to use as ground truth
	docIDList := make([]uint64, 0, len(docs))
	for docID := range docs {
		docIDList = append(docIDList, docID)
	}

	// Run traced queries
	var traces []DiagnosticTrace
	report := &DiagnosticReport{
		Config:         cfg,
		TotalQueries:   cfg.NumQueries,
		PerQueryRecall: make([]float64, 0, cfg.NumQueries),
	}

	qIdx := 0
	for _, queryVecs := range queries {
		queryID := fmt.Sprintf("query-%d", qIdx)
		// Simple ground truth: first K documents (this is a placeholder)
		// In real usage, you'd compute brute-force MaxSim scores
		gt := make([]uint64, 0, cfg.GroundTruthK)
		for _, docID := range docIDList {
			if len(gt) >= cfg.GroundTruthK {
				break
			}
			gt = append(gt, docID)
		}

		// Create trace collector
		collector := NewSearchTraceCollector(queryID)
		ctx := ContextWithTraceCollector(context.Background(), collector)

		// Run search
		returnedIDs, _, err := tf.Index.SearchByMultiVector(ctx, queryVecs, cfg.K, nil)
		require.NoError(t, err)

		// Get trace
		trace := collector.Trace()

		// Classify failures
		routing, approx, exact, success := ClassifyRecallFailures(
			gt,
			trace.SelectedCentroids,
			trace.ApproxTopIDs,
			returnedIDs,
			membership,
		)

		// Update report
		report.RoutingFailures += len(routing)
		report.ApproxFailures += len(approx)
		report.ExactFailures += len(exact)
		report.Successes += len(success)
		report.TotalGroundTruth += len(gt)

		queryRecall := 0.0
		if len(gt) > 0 {
			queryRecall = float64(len(success)) / float64(len(gt))
		}
		report.PerQueryRecall = append(report.PerQueryRecall, queryRecall)

		// Create diagnostic trace
		diagTrace := DiagnosticTrace{
			QueryID:           queryID,
			SelectedCentroids: trace.SelectedCentroids,
			ApproxTopIDs:      trace.ApproxTopIDs,
			ReturnedIDs:       returnedIDs,
			ScanStats:         trace.ScanStats,
			GroundTruthIDs:    gt,
			TimestampNs:       time.Now().UnixNano(),
		}
		traces = append(traces, diagTrace)

		qIdx++
	}

	// Compute final metrics
	if report.TotalGroundTruth > 0 {
		report.Recall = float64(report.Successes) / float64(report.TotalGroundTruth)
	}
	if len(report.PerQueryRecall) > 0 {
		sum := 0.0
		for _, r := range report.PerQueryRecall {
			sum += r
		}
		report.AvgRecall = sum / float64(len(report.PerQueryRecall))
	}

	return report, traces
}

// generateTestMultiVectors creates test multi-vectors.
// Returns map of docID -> [][]float32
func generateTestMultiVectors(count, vectorsPerDoc, dims int) map[uint64][][]float32 {
	docs := make(map[uint64][][]float32)
	for i := 0; i < count; i++ {
		docID := uint64(i)
		vectors := make([][]float32, vectorsPerDoc)
		for j := 0; j < vectorsPerDoc; j++ {
			vec := make([]float32, dims)
			for k := 0; k < dims; k++ {
				// Create somewhat distinct vectors
				vec[k] = float32(i*1000+j*100+k) / 100000.0
			}
			vectors[j] = vec
		}
		docs[docID] = vectors
	}
	return docs
}

// writeTracesJSONL writes traces to a JSONL file.
func writeTracesJSONL(traces []DiagnosticTrace, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, trace := range traces {
		if err := enc.Encode(trace); err != nil {
			return err
		}
	}
	return nil
}

// writeReportJSON writes the report to a JSON file.
func writeReportJSON(report *DiagnosticReport, path string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// TestDiagnosticRunner runs a full diagnostic and prints results.
// Run with: go test -v -run TestDiagnosticRunner ./adapters/repos/db/vector/hfresh/...
func TestDiagnosticRunner(t *testing.T) {
	cfg := DefaultDiagnosticConfig()
	cfg.OutputDir = t.TempDir()

	report, traces := runDiagnostic(t, cfg)

	// Print summary
	t.Logf("=== DIAGNOSTIC REPORT ===")
	t.Logf("Documents: %d, Vectors/Doc: %d, Dims: %d", cfg.NumDocs, cfg.VectorsPerDoc, cfg.VectorDims)
	t.Logf("Queries: %d, K: %d", cfg.NumQueries, cfg.K)
	t.Logf("")
	t.Logf("Total Ground Truth IDs: %d", report.TotalGroundTruth)
	t.Logf("Routing Failures:       %d (%.1f%%)", report.RoutingFailures, 100.0*float64(report.RoutingFailures)/float64(report.TotalGroundTruth))
	t.Logf("Approx Failures:        %d (%.1f%%)", report.ApproxFailures, 100.0*float64(report.ApproxFailures)/float64(report.TotalGroundTruth))
	t.Logf("Exact Failures:         %d (%.1f%%)", report.ExactFailures, 100.0*float64(report.ExactFailures)/float64(report.TotalGroundTruth))
	t.Logf("Successes:              %d (%.1f%%)", report.Successes, 100.0*float64(report.Successes)/float64(report.TotalGroundTruth))
	t.Logf("")
	t.Logf("Overall Recall:         %.4f", report.Recall)
	t.Logf("Average Query Recall:   %.4f", report.AvgRecall)

	// Write outputs if OutputDir is set
	if cfg.OutputDir != "" {
		tracesPath := filepath.Join(cfg.OutputDir, "traces.jsonl")
		err := writeTracesJSONL(traces, tracesPath)
		require.NoError(t, err)
		t.Logf("Traces written to: %s", tracesPath)

		reportPath := filepath.Join(cfg.OutputDir, "report.json")
		err = writeReportJSON(report, reportPath)
		require.NoError(t, err)
		t.Logf("Report written to: %s", reportPath)
	}
}

// TestDiagnosticWithCustomGroundTruth demonstrates using custom ground-truth.
func TestDiagnosticWithCustomGroundTruth(t *testing.T) {
	// Create MUVERA-enabled HFresh index
	tf := createMuveraHFreshIndex(t)
	defer func() {
		_ = tf.Index.Shutdown(context.Background())
	}()

	// Insert 20 documents
	const numDocs = 20
	const vectorsPerDoc = 4
	const dims = 128

	docs := generateTestMultiVectors(numDocs, vectorsPerDoc, dims)
	for docID, vectors := range docs {
		addMultiVectorToIndex(t, &tf, docID, vectors)
	}

	time.Sleep(100 * time.Millisecond)

	// Build posting membership
	membership := BuildPostingMembership(tf.Index)
	t.Logf("Posting membership built: %d documents mapped", len(membership))

	// Create a query (use doc 0's vectors as query)
	queryVecs := docs[0]

	// Define custom ground-truth (suppose we know docs 0, 1, 2, 3, 4 are relevant)
	groundTruth := []uint64{0, 1, 2, 3, 4}

	// Run traced search
	collector := NewSearchTraceCollector("custom-gt-query")
	ctx := ContextWithTraceCollector(context.Background(), collector)

	k := 5
	returnedIDs, _, err := tf.Index.SearchByMultiVector(ctx, queryVecs, k, nil)
	require.NoError(t, err)

	trace := collector.Trace()

	// Classify failures
	routing, approx, exact, success := ClassifyRecallFailures(
		groundTruth,
		trace.SelectedCentroids,
		trace.ApproxTopIDs,
		returnedIDs,
		membership,
	)

	t.Logf("=== CUSTOM GROUND-TRUTH TEST ===")
	t.Logf("Ground Truth: %v", groundTruth)
	t.Logf("Returned:     %v", returnedIDs)
	t.Logf("")
	t.Logf("Selected Centroids: %v", trace.SelectedCentroids)
	t.Logf("Approx Top IDs:     %v", trace.ApproxTopIDs)
	t.Logf("")
	t.Logf("Routing Failures: %v", routing)
	t.Logf("Approx Failures:  %v", approx)
	t.Logf("Exact Failures:   %v", exact)
	t.Logf("Successes:        %v", success)
	t.Logf("")
	t.Logf("Recall: %.4f", float64(len(success))/float64(len(groundTruth)))
}

// TestDiagnosticPostingMembership verifies posting membership is built correctly.
func TestDiagnosticPostingMembership(t *testing.T) {
	tf := createMuveraHFreshIndex(t)
	defer func() {
		_ = tf.Index.Shutdown(context.Background())
	}()

	// Insert 10 documents
	docs := generateTestMultiVectors(10, 4, 128)
	for docID, vectors := range docs {
		addMultiVectorToIndex(t, &tf, docID, vectors)
	}

	time.Sleep(100 * time.Millisecond)

	membership := BuildPostingMembership(tf.Index)

	// Each inserted document should have at least one posting
	for docID := range docs {
		postings := membership[docID]
		t.Logf("Doc %d -> Postings %v", docID, postings)
		require.NotEmpty(t, postings, "document %d should have posting membership", docID)
	}
}

// TestDiagnosticTraceRoundTrip verifies trace JSONL export/import.
func TestDiagnosticTraceRoundTrip(t *testing.T) {
	trace := DiagnosticTrace{
		QueryID:           "test-query-1",
		SelectedCentroids: []uint64{100, 101, 102},
		ApproxTopIDs:      []uint64{1, 2, 3, 4, 5},
		ReturnedIDs:       []uint64{1, 3, 5},
		ScanStats: ScanStats{
			TotalScanned:     200,
			UniqueEnumerated: 50,
			SkippedDeleted:   5,
			SkippedDuplicate: 10,
			SkippedAllowList: 0,
		},
		GroundTruthIDs: []uint64{1, 2, 3},
		TimestampNs:    time.Now().UnixNano(),
	}

	// Write to temp file
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_traces.jsonl")

	err := writeTracesJSONL([]DiagnosticTrace{trace}, path)
	require.NoError(t, err)

	// Read back
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var readTrace DiagnosticTrace
	err = json.Unmarshal(data, &readTrace)
	require.NoError(t, err)

	require.Equal(t, trace.QueryID, readTrace.QueryID)
	require.Equal(t, trace.SelectedCentroids, readTrace.SelectedCentroids)
	require.Equal(t, trace.ApproxTopIDs, readTrace.ApproxTopIDs)
	require.Equal(t, trace.ReturnedIDs, readTrace.ReturnedIDs)
	require.Equal(t, trace.GroundTruthIDs, readTrace.GroundTruthIDs)
}
