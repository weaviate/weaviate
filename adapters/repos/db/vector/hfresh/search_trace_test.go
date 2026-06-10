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

func TestSearchTraceCollector_CentroidSearch(t *testing.T) {
	collector := NewSearchTraceCollector("query-456")

	// Record centroid search stats
	collector.RecordCentroidSearch(64, 50, 5, 3)

	// Record selected postings
	collector.RecordSelectedPosting(100, 0, 0.1, 150)
	collector.RecordSelectedPosting(101, 1, 0.15, 200)
	collector.RecordSelectedPosting(102, 2, 0.2, 175)

	trace := collector.Trace()
	assert.Equal(t, 64, trace.CentroidSearch.CandidateCentroidNum)
	assert.Equal(t, 50, trace.CentroidSearch.CentroidsReturned)
	assert.Equal(t, 5, trace.CentroidSearch.FilteredByDistance)
	assert.Equal(t, 3, trace.CentroidSearch.FilteredByEmptyPosting)

	require.Len(t, trace.CentroidSearch.SelectedPostings, 3)
	assert.Equal(t, uint64(100), trace.CentroidSearch.SelectedPostings[0].CentroidID)
	assert.Equal(t, 0, trace.CentroidSearch.SelectedPostings[0].SelectionRank)
	assert.InDelta(t, 0.1, trace.CentroidSearch.SelectedPostings[0].CentroidDistance, 0.001)
	assert.Equal(t, 150, trace.CentroidSearch.SelectedPostings[0].PostingSize)
}

func TestSearchTraceCollector_PostingScan(t *testing.T) {
	collector := NewSearchTraceCollector("query-789")

	// Record posting scan stats
	collector.RecordPostingScanStats(500, 10, 20, 5)

	// Record candidates
	collector.RecordCandidate(1001, 100, 0.5)
	collector.RecordCandidate(1002, 100, 0.6)
	collector.RecordCandidate(1003, 101, 0.55)

	trace := collector.Trace()
	assert.Equal(t, 500, trace.PostingScan.TotalVectorsScanned)
	assert.Equal(t, 10, trace.PostingScan.SkippedDeleted)
	assert.Equal(t, 20, trace.PostingScan.SkippedDuplicate)
	assert.Equal(t, 5, trace.PostingScan.SkippedAllowList)

	require.Len(t, trace.PostingScan.CandidatesEnumerated, 3)
	assert.Equal(t, uint64(1001), trace.PostingScan.CandidatesEnumerated[0].DocID)
	assert.Equal(t, uint64(100), trace.PostingScan.CandidatesEnumerated[0].CentroidID)
	assert.InDelta(t, 0.5, trace.PostingScan.CandidatesEnumerated[0].ApproxScore, 0.001)
}

func TestSearchTraceCollector_DuplicateCandidates(t *testing.T) {
	collector := NewSearchTraceCollector("query-dup")

	// Record same candidate from different postings - only first should be stored
	collector.RecordCandidate(1001, 100, 0.5)
	collector.RecordCandidate(1001, 101, 0.45) // Duplicate, should be ignored

	trace := collector.Trace()
	require.Len(t, trace.PostingScan.CandidatesEnumerated, 1)
	assert.Equal(t, uint64(1001), trace.PostingScan.CandidatesEnumerated[0].DocID)
	assert.Equal(t, uint64(100), trace.PostingScan.CandidatesEnumerated[0].CentroidID) // First occurrence
}

func TestSearchTraceCollector_ApproximateRanking(t *testing.T) {
	collector := NewSearchTraceCollector("query-rank")

	// Record candidates first
	collector.RecordCandidate(1001, 100, 0.5)
	collector.RecordCandidate(1002, 100, 0.6)
	collector.RecordCandidate(1003, 101, 0.55)

	// Record approximate ranking
	topCandidates := []Result{
		{ID: 1001, Distance: 0.5},
		{ID: 1003, Distance: 0.55},
	}
	collector.RecordApproximateRanking(topCandidates)

	trace := collector.Trace()
	assert.Equal(t, 3, trace.ApproximateRanking.TotalCandidates)
	assert.Equal(t, 2, trace.ApproximateRanking.KeptForRescore)

	require.Len(t, trace.ApproximateRanking.TopCandidates, 2)
	assert.Equal(t, uint64(1001), trace.ApproximateRanking.TopCandidates[0].DocID)
	assert.Equal(t, 0, trace.ApproximateRanking.TopCandidates[0].ApproxRank)
	assert.True(t, trace.ApproximateRanking.TopCandidates[0].KeptForRescore)
}

func TestSearchTraceCollector_LateInteraction(t *testing.T) {
	collector := NewSearchTraceCollector("query-late")

	// Record late interaction stats
	collector.RecordLateInteractionStats(5)

	// Record rescored candidates
	collector.RecordLateInteractionCandidate(1001, 0.5, 0.48, 0, true)
	collector.RecordLateInteractionCandidate(1002, 0.6, 0.55, 1, true)
	collector.RecordLateInteractionCandidate(1003, 0.55, 0.60, -1, false) // Not returned

	trace := collector.Trace()
	assert.Equal(t, 5, trace.LateInteraction.CandidatesRescored)

	require.Len(t, trace.LateInteraction.RescoredCandidates, 3)
	assert.Equal(t, uint64(1001), trace.LateInteraction.RescoredCandidates[0].DocID)
	assert.InDelta(t, 0.5, trace.LateInteraction.RescoredCandidates[0].ApproxScore, 0.001)
	assert.InDelta(t, 0.48, trace.LateInteraction.RescoredCandidates[0].ExactScore, 0.001)
	assert.Equal(t, 0, trace.LateInteraction.RescoredCandidates[0].ExactRank)
	assert.True(t, trace.LateInteraction.RescoredCandidates[0].Returned)

	assert.Equal(t, uint64(1003), trace.LateInteraction.RescoredCandidates[2].DocID)
	assert.False(t, trace.LateInteraction.RescoredCandidates[2].Returned)
}

func TestSearchTraceCollector_FinalResults(t *testing.T) {
	collector := NewSearchTraceCollector("query-final")

	ids := []uint64{1001, 1002, 1003}
	scores := []float32{0.1, 0.2, 0.3}
	collector.RecordFinalResults(ids, scores)

	trace := collector.Trace()
	require.Len(t, trace.FinalResults, 3)

	assert.Equal(t, uint64(1001), trace.FinalResults[0].DocID)
	assert.InDelta(t, 0.1, trace.FinalResults[0].Score, 0.001)
	assert.Equal(t, 0, trace.FinalResults[0].Rank)

	assert.Equal(t, uint64(1003), trace.FinalResults[2].DocID)
	assert.InDelta(t, 0.3, trace.FinalResults[2].Score, 0.001)
	assert.Equal(t, 2, trace.FinalResults[2].Rank)
}

func TestSearchTraceCollector_InternalConsistency(t *testing.T) {
	// Verify that candidate counts are internally consistent
	collector := NewSearchTraceCollector("query-consistency")

	// Simulate a realistic search flow
	collector.SetSearchParams(10, 64, 100, true)
	collector.RecordCentroidSearch(64, 50, 5, 3)

	// Record 5 selected postings
	for i := 0; i < 5; i++ {
		collector.RecordSelectedPosting(uint64(100+i), i, float32(0.1+float64(i)*0.02), 100)
	}

	// Record posting scan with 200 candidates
	totalScanned := 500
	deleted := 10
	duplicates := 50
	allowListFiltered := 0
	candidateCount := 200
	collector.RecordPostingScanStats(totalScanned, deleted, duplicates, allowListFiltered)

	for i := 0; i < candidateCount; i++ {
		collector.RecordCandidate(uint64(1000+i), uint64(100+(i%5)), float32(0.3+float64(i)*0.001))
	}

	// Record approximate ranking (keep top 100)
	topCandidates := make([]Result, 100)
	for i := 0; i < 100; i++ {
		topCandidates[i] = Result{ID: uint64(1000 + i), Distance: float32(0.3 + float64(i)*0.001)}
	}
	collector.RecordApproximateRanking(topCandidates)

	trace := collector.Trace()

	// Verify consistency
	assert.Equal(t, candidateCount, trace.ApproximateRanking.TotalCandidates)
	assert.Equal(t, 100, trace.ApproximateRanking.KeptForRescore)
	assert.Equal(t, len(trace.PostingScan.CandidatesEnumerated), trace.ApproximateRanking.TotalCandidates)

	// Verify selected postings match centroid search
	assert.Equal(t, 5, len(trace.CentroidSearch.SelectedPostings))
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
			queryID := uint64(1000 + idx*100)

			// Simulate search operations
			collector.SetSearchParams(10, 64, 100, true)
			collector.RecordCentroidSearch(64, 50, 5, 3)

			for j := 0; j < 10; j++ {
				collector.RecordSelectedPosting(uint64(idx*100+j), j, float32(0.1), 50)
			}

			for j := 0; j < 50; j++ {
				collector.RecordCandidate(queryID+uint64(j), uint64(idx*100), float32(0.5))
			}

			topCandidates := make([]Result, 10)
			for j := 0; j < 10; j++ {
				topCandidates[j] = Result{ID: queryID + uint64(j), Distance: float32(0.5)}
			}
			collector.RecordApproximateRanking(topCandidates)

			ids := make([]uint64, 5)
			scores := make([]float32, 5)
			for j := 0; j < 5; j++ {
				ids[j] = queryID + uint64(j)
				scores[j] = float32(0.1 + float64(j)*0.01)
			}
			collector.RecordFinalResults(ids, scores)
		}(i)
	}

	wg.Wait()

	// Verify each collector has its own isolated data
	for i := 0; i < numSearches; i++ {
		trace := collectors[i].Trace()
		queryID := uint64(1000 + i*100)

		// Check that final results belong to this search
		require.Len(t, trace.FinalResults, 5)
		for j, result := range trace.FinalResults {
			assert.Equal(t, queryID+uint64(j), result.DocID,
				"search %d result %d has wrong DocID", i, j)
		}

		// Check selected postings are isolated
		require.Len(t, trace.CentroidSearch.SelectedPostings, 10)
		for j, posting := range trace.CentroidSearch.SelectedPostings {
			expectedCentroidID := uint64(i*100 + j)
			assert.Equal(t, expectedCentroidID, posting.CentroidID,
				"search %d posting %d has wrong CentroidID", i, j)
		}
	}
}

func TestSearchTraceCollector_EmptyTrace(t *testing.T) {
	// A fresh collector should return valid empty trace
	collector := NewSearchTraceCollector("")
	trace := collector.Trace()

	assert.Empty(t, trace.QueryID)
	assert.Equal(t, 0, trace.K)
	assert.Nil(t, trace.CentroidSearch.SelectedPostings)
	assert.Nil(t, trace.PostingScan.CandidatesEnumerated)
	assert.Nil(t, trace.ApproximateRanking.TopCandidates)
	assert.Nil(t, trace.FinalResults)
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

// Integration test: verify tracing doesn't change search results
func TestSearchTrace_MuveraSearchResultsUnchanged(t *testing.T) {
	tf := createMuveraHFreshIndex(t)
	defer tf.Index.Shutdown(context.Background())

	// Add some multi-vector documents
	// Each document has 4 tokens of dimension 128
	dim := 128
	numDocs := 20
	tokensPerDoc := 4

	for docID := uint64(0); docID < uint64(numDocs); docID++ {
		vectors := make([][]float32, tokensPerDoc)
		for j := 0; j < tokensPerDoc; j++ {
			vec := make([]float32, dim)
			for k := 0; k < dim; k++ {
				// Create deterministic vectors based on docID and token index
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
	ids, dists, err := tf.Index.SearchByMultiVector(ctx, queryVectors, 5, nil)
	require.NoError(t, err)

	trace := collector.Trace()

	// Verify search parameters were recorded
	assert.Equal(t, "muvera-test", trace.QueryID)
	assert.True(t, trace.IsMuvera, "should be marked as MUVERA search")
	assert.Greater(t, trace.RescoreLimit, 0, "rescoreLimit should be set")
	assert.Greater(t, trace.SearchProbe, 0, "searchProbe should be set")

	// Verify centroid search was recorded
	assert.Greater(t, trace.CentroidSearch.CandidateCentroidNum, 0, "should have candidate centroids")
	assert.GreaterOrEqual(t, trace.CentroidSearch.CentroidsReturned, 0, "should record centroids returned")
	assert.NotEmpty(t, trace.CentroidSearch.SelectedPostings, "should have selected postings")

	// Verify posting scan was recorded
	assert.Greater(t, trace.PostingScan.TotalVectorsScanned, 0, "should have scanned vectors")

	// Verify approximate ranking was recorded
	assert.Greater(t, trace.ApproximateRanking.TotalCandidates, 0, "should have candidates")
	assert.Greater(t, trace.ApproximateRanking.KeptForRescore, 0, "should have candidates for rescore")

	// Verify late interaction was recorded (MUVERA specific)
	assert.Greater(t, trace.LateInteraction.CandidatesRescored, 0, "should have rescored candidates")
	assert.NotEmpty(t, trace.LateInteraction.RescoredCandidates, "should have rescored candidate details")

	// Verify final results match returned values
	require.Len(t, trace.FinalResults, len(ids), "final results count should match")
	for i, result := range trace.FinalResults {
		assert.Equal(t, ids[i], result.DocID, "result DocID should match at position %d", i)
		assert.InDelta(t, dists[i], result.Score, 0.0001, "result score should match at position %d", i)
		assert.Equal(t, i, result.Rank, "result rank should match position")
	}

	// Verify internal consistency
	// All final results should appear in rescored candidates
	returnedIDs := make(map[uint64]bool)
	for _, id := range ids {
		returnedIDs[id] = true
	}

	returnedInRescored := 0
	for _, rc := range trace.LateInteraction.RescoredCandidates {
		if rc.Returned {
			returnedInRescored++
			assert.True(t, returnedIDs[rc.DocID], "returned candidate should be in final results")
		}
	}
	assert.Equal(t, len(ids), returnedInRescored, "returned count should match final results")
}
