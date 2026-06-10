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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchTraceCollector_DisabledByDefault(t *testing.T) {
	// When no collector is in context, TraceCollectorFromContext returns nil
	ctx := context.Background()
	collector := TraceCollectorFromContext(ctx)
	assert.Nil(t, collector, "collector should be nil when not set in context")
}

func TestSearchTraceCollector_ContextPropagation(t *testing.T) {
	// Collector can be added to and retrieved from context
	ctx := context.Background()
	collector := NewSearchTraceCollector("test-query-1")

	ctxWithCollector := ContextWithTraceCollector(ctx, collector)
	retrieved := TraceCollectorFromContext(ctxWithCollector)

	assert.NotNil(t, retrieved, "collector should be retrievable from context")
	assert.Same(t, collector, retrieved, "retrieved collector should be the same instance")
}

func TestSearchTraceCollector_SearchParams(t *testing.T) {
	collector := NewSearchTraceCollector("query-123")

	collector.SetSearchParams(10, 64, 350, true)

	trace := collector.Trace()
	assert.Equal(t, "query-123", trace.QueryID)
	assert.Equal(t, 10, trace.K)
	assert.Equal(t, 64, trace.SearchProbe)
	assert.Equal(t, 350, trace.RescoreLimit)
	assert.True(t, trace.IsMuvera)
}

func TestSearchTraceCollector_SelectedCentroids(t *testing.T) {
	collector := NewSearchTraceCollector("query-456")

	centroids := []uint64{100, 101, 102, 103}
	collector.RecordSelectedCentroids(centroids)

	trace := collector.Trace()
	require.Len(t, trace.SelectedCentroids, 4)
	assert.Equal(t, uint64(100), trace.SelectedCentroids[0])
	assert.Equal(t, uint64(103), trace.SelectedCentroids[3])
}

func TestSearchTraceCollector_ScanStats(t *testing.T) {
	collector := NewSearchTraceCollector("query-789")

	collector.RecordScanStats(500, 200, 10, 20, 5)

	trace := collector.Trace()
	assert.Equal(t, 500, trace.ScanStats.TotalScanned)
	assert.Equal(t, 200, trace.ScanStats.UniqueEnumerated)
	assert.Equal(t, 10, trace.ScanStats.SkippedDeleted)
	assert.Equal(t, 20, trace.ScanStats.SkippedDuplicate)
	assert.Equal(t, 5, trace.ScanStats.SkippedAllowList)
}

func TestSearchTraceCollector_ApproxTopIDs(t *testing.T) {
	collector := NewSearchTraceCollector("query-approx")

	ids := []uint64{1001, 1002, 1003, 1004, 1005}
	collector.RecordApproxTopIDs(ids)

	trace := collector.Trace()
	require.Len(t, trace.ApproxTopIDs, 5)
	assert.Equal(t, uint64(1001), trace.ApproxTopIDs[0])
	assert.Equal(t, uint64(1005), trace.ApproxTopIDs[4])
}

func TestSearchTraceCollector_ReturnedIDs(t *testing.T) {
	collector := NewSearchTraceCollector("query-returned")

	ids := []uint64{1001, 1002, 1003}
	collector.RecordReturnedIDs(ids)

	trace := collector.Trace()
	require.Len(t, trace.ReturnedIDs, 3)
	assert.Equal(t, uint64(1001), trace.ReturnedIDs[0])
	assert.Equal(t, uint64(1003), trace.ReturnedIDs[2])
}

func TestSearchTraceCollector_FullTrace(t *testing.T) {
	collector := NewSearchTraceCollector("full-trace")

	// Simulate a complete search trace
	collector.SetSearchParams(10, 64, 100, true)
	collector.RecordSelectedCentroids([]uint64{100, 101, 102})
	collector.RecordScanStats(300, 150, 5, 10, 2)
	collector.RecordApproxTopIDs([]uint64{1001, 1002, 1003, 1004, 1005})
	collector.RecordReturnedIDs([]uint64{1001, 1003, 1005})

	trace := collector.Trace()

	// Verify all fields
	assert.Equal(t, "full-trace", trace.QueryID)
	assert.True(t, trace.IsMuvera)
	assert.Equal(t, 10, trace.K)
	assert.Equal(t, 64, trace.SearchProbe)
	assert.Equal(t, 100, trace.RescoreLimit)

	assert.Len(t, trace.SelectedCentroids, 3)
	assert.Equal(t, 300, trace.ScanStats.TotalScanned)
	assert.Equal(t, 150, trace.ScanStats.UniqueEnumerated)

	assert.Len(t, trace.ApproxTopIDs, 5)
	assert.Len(t, trace.ReturnedIDs, 3)
}

func TestSearchTraceCollector_ConcurrentSearches(t *testing.T) {
	// Verify that concurrent searches with different collectors don't interfere
	const numSearches = 10
	var wg sync.WaitGroup
	wg.Add(numSearches)

	collectors := make([]*SearchTraceCollector, numSearches)
	for i := 0; i < numSearches; i++ {
		collectors[i] = NewSearchTraceCollector("")
	}

	for i := 0; i < numSearches; i++ {
		go func(idx int) {
			defer wg.Done()

			collector := collectors[idx]
			baseID := uint64(1000 + idx*100)

			// Simulate search operations
			collector.SetSearchParams(10, 64, 100, true)
			collector.RecordSelectedCentroids([]uint64{uint64(idx*100 + 1), uint64(idx*100 + 2)})
			collector.RecordScanStats(100, 50, 1, 2, 0)

			approxIDs := make([]uint64, 5)
			for j := 0; j < 5; j++ {
				approxIDs[j] = baseID + uint64(j)
			}
			collector.RecordApproxTopIDs(approxIDs)

			returnedIDs := make([]uint64, 3)
			for j := 0; j < 3; j++ {
				returnedIDs[j] = baseID + uint64(j)
			}
			collector.RecordReturnedIDs(returnedIDs)
		}(i)
	}

	wg.Wait()

	// Verify each collector has its own isolated data
	for i := 0; i < numSearches; i++ {
		trace := collectors[i].Trace()
		baseID := uint64(1000 + i*100)

		// Check that returned IDs belong to this search
		require.Len(t, trace.ReturnedIDs, 3)
		for j, id := range trace.ReturnedIDs {
			assert.Equal(t, baseID+uint64(j), id,
				"search %d result %d has wrong ID", i, j)
		}

		// Check selected centroids are isolated
		require.Len(t, trace.SelectedCentroids, 2)
		assert.Equal(t, uint64(i*100+1), trace.SelectedCentroids[0])
	}
}

func TestSearchTraceCollector_EmptyTrace(t *testing.T) {
	// A fresh collector should return valid empty trace
	collector := NewSearchTraceCollector("")
	trace := collector.Trace()

	assert.Empty(t, trace.QueryID)
	assert.Equal(t, 0, trace.K)
	assert.Nil(t, trace.SelectedCentroids)
	assert.Nil(t, trace.ApproxTopIDs)
	assert.Nil(t, trace.ReturnedIDs)
	assert.Equal(t, 0, trace.ScanStats.TotalScanned)
}

func TestSearchTraceCollector_TraceIsCopy(t *testing.T) {
	// Verify that Trace() returns a copy, not a reference that could be corrupted
	collector := NewSearchTraceCollector("original")
	collector.SetSearchParams(10, 64, 100, true)

	trace1 := collector.Trace()

	// Modify the collector after getting the trace
	collector.SetSearchParams(20, 128, 200, false)

	trace2 := collector.Trace()

	// trace1 should have original values
	assert.Equal(t, 10, trace1.K)
	assert.Equal(t, 64, trace1.SearchProbe)
	assert.True(t, trace1.IsMuvera)

	// trace2 should have updated values
	assert.Equal(t, 20, trace2.K)
	assert.Equal(t, 128, trace2.SearchProbe)
	assert.False(t, trace2.IsMuvera)
}

func TestSearchTraceCollector_SlicesAreCopied(t *testing.T) {
	// Verify that recorded slices are copied, not referenced
	collector := NewSearchTraceCollector("copy-test")

	centroids := []uint64{100, 101, 102}
	collector.RecordSelectedCentroids(centroids)

	// Modify original slice
	centroids[0] = 999

	trace := collector.Trace()
	// Trace should have original value
	assert.Equal(t, uint64(100), trace.SelectedCentroids[0])
}

// Integration test: verify tracing doesn't change search results
func TestSearchTrace_MuveraSearchResultsUnchanged(t *testing.T) {
	tf := createMuveraHFreshIndex(t)
	defer tf.Index.Shutdown(context.Background())

	// Add some multi-vector documents
	dim := 128
	numDocs := 20
	tokensPerDoc := 4

	for docID := uint64(0); docID < uint64(numDocs); docID++ {
		vectors := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dim)
			for k := 0; k < dim; k++ {
				vec[k] = float32(docID+1) * float32(j+1) * float32(k+1) / 1000.0
			}
			vectors[j] = vec
		}
		addMultiVectorToIndex(t, &tf, docID, vectors)
	}

	// Create a query
	queryVectors := make([][]float32, 2)
	for i := 0; i < 2; i++ {
		vec := make([]float32, dim)
		for k := 0; k < dim; k++ {
			vec[k] = float32(i+1) * float32(k+1) / 500.0
		}
		queryVectors[i] = vec
	}

	// Search WITHOUT tracing
	ctx1 := context.Background()
	ids1, dists1, err := tf.Index.SearchByMultiVector(ctx1, queryVectors, 5, nil)
	require.NoError(t, err)

	// Search WITH tracing
	collector := NewSearchTraceCollector("test-query")
	ctx2 := ContextWithTraceCollector(context.Background(), collector)
	ids2, dists2, err := tf.Index.SearchByMultiVector(ctx2, queryVectors, 5, nil)
	require.NoError(t, err)

	// Results should be identical
	require.Equal(t, len(ids1), len(ids2), "result count should match")
	for i := range ids1 {
		assert.Equal(t, ids1[i], ids2[i], "result ID at position %d should match", i)
		assert.InDelta(t, dists1[i], dists2[i], 0.0001, "result distance at position %d should match", i)
	}
}

// Integration test: verify trace captures expected data during MUVERA search
func TestSearchTrace_MuveraSearchTracePopulated(t *testing.T) {
	tf := createMuveraHFreshIndex(t)
	defer tf.Index.Shutdown(context.Background())

	// Add multi-vector documents
	dim := 128
	numDocs := 15
	tokensPerDoc := 3

	for docID := uint64(0); docID < uint64(numDocs); docID++ {
		vectors := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dim)
			for k := 0; k < dim; k++ {
				vec[k] = float32(docID+1) * float32(j+1) * float32(k+1) / 1000.0
			}
			vectors[j] = vec
		}
		addMultiVectorToIndex(t, &tf, docID, vectors)
	}

	// Create query
	queryVectors := make([][]float32, 2)
	for i := 0; i < 2; i++ {
		vec := make([]float32, dim)
		for k := 0; k < dim; k++ {
			vec[k] = float32(i+1) * float32(k+1) / 500.0
		}
		queryVectors[i] = vec
	}

	// Search with tracing
	collector := NewSearchTraceCollector("muvera-test")
	ctx := ContextWithTraceCollector(context.Background(), collector)
	ids, _, err := tf.Index.SearchByMultiVector(ctx, queryVectors, 5, nil)
	require.NoError(t, err)

	trace := collector.Trace()

	// Verify search parameters were recorded
	assert.Equal(t, "muvera-test", trace.QueryID)
	assert.True(t, trace.IsMuvera, "should be marked as MUVERA search")
	assert.Greater(t, trace.RescoreLimit, 0, "rescoreLimit should be set")
	assert.Greater(t, trace.SearchProbe, 0, "searchProbe should be set")

	// Verify selected centroids were recorded
	assert.NotEmpty(t, trace.SelectedCentroids, "should have selected centroids")

	// Verify scan stats were recorded
	assert.Greater(t, trace.ScanStats.TotalScanned, 0, "should have scanned vectors")
	assert.Greater(t, trace.ScanStats.UniqueEnumerated, 0, "should have enumerated candidates")

	// Verify approximate top IDs were recorded
	assert.NotEmpty(t, trace.ApproxTopIDs, "should have approx top IDs")

	// Verify returned IDs match actual results
	require.Len(t, trace.ReturnedIDs, len(ids), "returned IDs count should match")
	for i, id := range ids {
		assert.Equal(t, id, trace.ReturnedIDs[i], "returned ID should match at position %d", i)
	}

	// Verify internal consistency: returned IDs should be subset of approx top IDs
	approxSet := make(map[uint64]bool)
	for _, id := range trace.ApproxTopIDs {
		approxSet[id] = true
	}
	for _, id := range trace.ReturnedIDs {
		assert.True(t, approxSet[id], "returned ID %d should be in approx top IDs", id)
	}
}

// Test that demonstrates recall attribution workflow
func TestSearchTrace_RecallAttributionWorkflow(t *testing.T) {
	// This test demonstrates how an external benchmark would use the trace
	// to classify recall failures. It uses mock data to show the workflow.

	collector := NewSearchTraceCollector("attribution-demo")

	// Simulate trace data from a search
	collector.SetSearchParams(5, 64, 100, true)
	collector.RecordSelectedCentroids([]uint64{100, 101, 102})
	collector.RecordScanStats(200, 80, 5, 10, 2)
	collector.RecordApproxTopIDs([]uint64{1, 3, 4, 5, 6, 7, 8, 9, 10}) // ID 2 intentionally excluded for approx failure test
	collector.RecordReturnedIDs([]uint64{1, 3, 5, 7, 9})

	trace := collector.Trace()

	// External benchmark has ground truth: [1, 2, 3, 11, 12]
	// and posting membership: {1: [100], 2: [100], 3: [101], 11: [200], 12: [201]}
	groundTruth := []uint64{1, 2, 3, 11, 12}
	postingMembership := map[uint64][]uint64{
		1: {100}, 2: {100}, 3: {101}, 11: {200}, 12: {201},
	}

	// Build sets for fast lookup
	selectedCentroidSet := make(map[uint64]bool)
	for _, c := range trace.SelectedCentroids {
		selectedCentroidSet[c] = true
	}

	approxTopSet := make(map[uint64]bool)
	for _, id := range trace.ApproxTopIDs {
		approxTopSet[id] = true
	}

	returnedSet := make(map[uint64]bool)
	for _, id := range trace.ReturnedIDs {
		returnedSet[id] = true
	}

	// Classify each ground truth ID
	var routingFailures, approxFailures, exactFailures, successes []uint64

	for _, gtID := range groundTruth {
		// Check if GT doc is in any selected posting
		inSelectedPosting := false
		for _, postingID := range postingMembership[gtID] {
			if selectedCentroidSet[postingID] {
				inSelectedPosting = true
				break
			}
		}

		if !inSelectedPosting {
			routingFailures = append(routingFailures, gtID)
			continue
		}

		if !approxTopSet[gtID] {
			approxFailures = append(approxFailures, gtID)
			continue
		}

		if !returnedSet[gtID] {
			exactFailures = append(exactFailures, gtID)
			continue
		}

		successes = append(successes, gtID)
	}

	// Verify classification
	assert.ElementsMatch(t, []uint64{11, 12}, routingFailures, "IDs 11, 12 should be routing failures (postings 200, 201 not selected)")
	assert.ElementsMatch(t, []uint64{2}, approxFailures, "ID 2 should be approx failure (in posting 100 but not in approx top)")
	assert.Empty(t, exactFailures, "no exact failures expected")
	assert.ElementsMatch(t, []uint64{1, 3}, successes, "IDs 1, 3 should be successes")
}
