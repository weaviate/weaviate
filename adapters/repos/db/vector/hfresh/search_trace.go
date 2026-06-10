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
)

// SearchTrace captures the internal state of an HFresh+MUVERA search for
// offline diagnostic analysis. Weaviate does not evaluate recall — it only
// records what happened. An external benchmark tool can later compare this
// trace against ground-truth results to classify recall loss.
//
// Usage:
//
//	collector := hfresh.NewSearchTraceCollector("query-123")
//	ctx := hfresh.ContextWithTraceCollector(ctx, collector)
//	ids, dists, err := index.SearchByMultiVector(ctx, vectors, k, nil)
//	trace := collector.Trace()
//	// Export trace for external analysis
type SearchTrace struct {
	// QueryID is an optional caller-provided identifier for correlation.
	QueryID string `json:"query_id,omitempty"`

	// IsMuvera indicates whether this was a MUVERA multi-vector search.
	IsMuvera bool `json:"is_muvera"`

	// Search parameters
	K            int `json:"k"`
	SearchProbe  int `json:"search_probe"`
	RescoreLimit int `json:"rescore_limit"`

	// CentroidSearch captures the centroid/posting selection stage.
	CentroidSearch CentroidSearchTrace `json:"centroid_search"`

	// PostingScan captures the posting enumeration stage.
	PostingScan PostingScanTrace `json:"posting_scan"`

	// ApproximateRanking captures candidates kept after approximate scoring.
	ApproximateRanking ApproximateRankingTrace `json:"approximate_ranking"`

	// LateInteraction captures the exact MaxSim rescoring stage (MUVERA only).
	LateInteraction LateInteractionTrace `json:"late_interaction,omitempty"`

	// FinalResults contains the returned result IDs and scores.
	FinalResults []ResultTrace `json:"final_results"`
}

// CentroidSearchTrace captures centroid selection details.
type CentroidSearchTrace struct {
	// CandidateCentroidNum is the number of centroids requested from HNSW.
	CandidateCentroidNum int `json:"candidate_centroid_num"`

	// CentroidsReturned is the number of centroids returned by HNSW search.
	CentroidsReturned int `json:"centroids_returned"`

	// SelectedPostings lists centroids that passed filtering and were selected.
	SelectedPostings []PostingSelectionTrace `json:"selected_postings"`

	// FilteredByDistance is the count of centroids skipped due to distance ratio.
	FilteredByDistance int `json:"filtered_by_distance"`

	// FilteredByEmptyPosting is the count of centroids skipped due to empty postings.
	FilteredByEmptyPosting int `json:"filtered_by_empty_posting"`
}

// PostingSelectionTrace captures details about a selected posting/centroid.
type PostingSelectionTrace struct {
	// CentroidID is the ID of the centroid/posting.
	CentroidID uint64 `json:"centroid_id"`

	// SelectionRank is the position in the centroid search results (0-indexed).
	SelectionRank int `json:"selection_rank"`

	// CentroidDistance is the distance from query to this centroid.
	CentroidDistance float32 `json:"centroid_distance"`

	// PostingSize is the number of vectors in this posting.
	PostingSize int `json:"posting_size"`
}

// PostingScanTrace captures the posting enumeration stage.
type PostingScanTrace struct {
	// TotalVectorsScanned is the total number of vectors examined across all postings.
	TotalVectorsScanned int `json:"total_vectors_scanned"`

	// SkippedDeleted is the count of vectors skipped because they were deleted.
	SkippedDeleted int `json:"skipped_deleted"`

	// SkippedDuplicate is the count of vectors skipped as duplicates.
	SkippedDuplicate int `json:"skipped_duplicate"`

	// SkippedAllowList is the count of vectors skipped due to allow list filtering.
	SkippedAllowList int `json:"skipped_allow_list"`

	// CandidatesEnumerated lists all unique candidates with their approximate scores.
	// This may be large; external tools can filter as needed.
	CandidatesEnumerated []CandidateTrace `json:"candidates_enumerated,omitempty"`
}

// CandidateTrace captures information about a candidate during search.
type CandidateTrace struct {
	// DocID is the document/vector ID.
	DocID uint64 `json:"doc_id"`

	// CentroidID is the centroid where this candidate was found.
	// May be 0 if candidate appeared in multiple postings.
	CentroidID uint64 `json:"centroid_id,omitempty"`

	// ApproxScore is the approximate (RQ-compressed) distance score.
	ApproxScore float32 `json:"approx_score"`

	// ApproxRank is the position in approximate ranking (0-indexed, -1 if not ranked).
	ApproxRank int `json:"approx_rank"`

	// KeptForRescore indicates whether this candidate was kept for exact rescoring.
	KeptForRescore bool `json:"kept_for_rescore"`
}

// ApproximateRankingTrace captures the top candidates after approximate scoring.
type ApproximateRankingTrace struct {
	// TotalCandidates is the number of unique candidates after posting scan.
	TotalCandidates int `json:"total_candidates"`

	// KeptForRescore is the number of candidates kept for exact rescoring.
	KeptForRescore int `json:"kept_for_rescore"`

	// TopCandidates lists candidates in approximate score order.
	// Limited to rescoreLimit candidates.
	TopCandidates []CandidateTrace `json:"top_candidates,omitempty"`
}

// LateInteractionTrace captures the exact MaxSim rescoring stage.
type LateInteractionTrace struct {
	// CandidatesRescored is the number of candidates that underwent MaxSim scoring.
	CandidatesRescored int `json:"candidates_rescored"`

	// RescoredCandidates lists candidates with their exact MaxSim scores.
	RescoredCandidates []RescoredCandidateTrace `json:"rescored_candidates,omitempty"`
}

// RescoredCandidateTrace captures a candidate after exact rescoring.
type RescoredCandidateTrace struct {
	// DocID is the document/vector ID.
	DocID uint64 `json:"doc_id"`

	// ApproxScore is the approximate score from FDE search.
	ApproxScore float32 `json:"approx_score"`

	// ExactScore is the exact MaxSim score.
	ExactScore float32 `json:"exact_score"`

	// ExactRank is the position in exact ranking (0-indexed).
	ExactRank int `json:"exact_rank"`

	// Returned indicates whether this candidate was in the final top-k.
	Returned bool `json:"returned"`
}

// ResultTrace captures a final result.
type ResultTrace struct {
	// DocID is the document/vector ID.
	DocID uint64 `json:"doc_id"`

	// Score is the final score (exact for MUVERA, approximate otherwise).
	Score float32 `json:"score"`

	// Rank is the position in final results (0-indexed).
	Rank int `json:"rank"`
}

// SearchTraceCollector accumulates trace data during search execution.
// It is safe for use within a single search but should not be shared
// across concurrent searches.
type SearchTraceCollector struct {
	mu    sync.Mutex
	trace SearchTrace

	// Internal state for building traces
	candidateMap map[uint64]*CandidateTrace
}

// NewSearchTraceCollector creates a new collector with an optional query ID.
func NewSearchTraceCollector(queryID string) *SearchTraceCollector {
	return &SearchTraceCollector{
		trace: SearchTrace{
			QueryID: queryID,
		},
		candidateMap: make(map[uint64]*CandidateTrace),
	}
}

// Trace returns the collected trace. Call this after search completes.
func (c *SearchTraceCollector) Trace() SearchTrace {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trace
}

// SetSearchParams records search parameters.
func (c *SearchTraceCollector) SetSearchParams(k, searchProbe, rescoreLimit int, isMuvera bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.K = k
	c.trace.SearchProbe = searchProbe
	c.trace.RescoreLimit = rescoreLimit
	c.trace.IsMuvera = isMuvera
}

// RecordCentroidSearch records centroid selection results.
func (c *SearchTraceCollector) RecordCentroidSearch(
	candidateCentroidNum int,
	centroidsReturned int,
	filteredByDistance int,
	filteredByEmptyPosting int,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.CentroidSearch.CandidateCentroidNum = candidateCentroidNum
	c.trace.CentroidSearch.CentroidsReturned = centroidsReturned
	c.trace.CentroidSearch.FilteredByDistance = filteredByDistance
	c.trace.CentroidSearch.FilteredByEmptyPosting = filteredByEmptyPosting
}

// RecordSelectedPosting records a posting that was selected for scanning.
func (c *SearchTraceCollector) RecordSelectedPosting(centroidID uint64, rank int, distance float32, postingSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.CentroidSearch.SelectedPostings = append(c.trace.CentroidSearch.SelectedPostings, PostingSelectionTrace{
		CentroidID:       centroidID,
		SelectionRank:    rank,
		CentroidDistance: distance,
		PostingSize:      postingSize,
	})
}

// RecordPostingScanStats records aggregate posting scan statistics.
func (c *SearchTraceCollector) RecordPostingScanStats(
	totalScanned int,
	skippedDeleted int,
	skippedDuplicate int,
	skippedAllowList int,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.PostingScan.TotalVectorsScanned = totalScanned
	c.trace.PostingScan.SkippedDeleted = skippedDeleted
	c.trace.PostingScan.SkippedDuplicate = skippedDuplicate
	c.trace.PostingScan.SkippedAllowList = skippedAllowList
}

// RecordCandidate records a candidate enumerated during posting scan.
func (c *SearchTraceCollector) RecordCandidate(docID uint64, centroidID uint64, approxScore float32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Track first occurrence
	if _, exists := c.candidateMap[docID]; !exists {
		candidate := &CandidateTrace{
			DocID:      docID,
			CentroidID: centroidID,
			ApproxScore: approxScore,
			ApproxRank: -1, // Not yet ranked
		}
		c.candidateMap[docID] = candidate
		c.trace.PostingScan.CandidatesEnumerated = append(
			c.trace.PostingScan.CandidatesEnumerated,
			*candidate,
		)
	}
}

// RecordApproximateRanking records the candidates kept after approximate scoring.
func (c *SearchTraceCollector) RecordApproximateRanking(topCandidates []Result) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.trace.ApproximateRanking.TotalCandidates = len(c.candidateMap)
	c.trace.ApproximateRanking.KeptForRescore = len(topCandidates)

	for rank, r := range topCandidates {
		candidate := CandidateTrace{
			DocID:          r.ID,
			ApproxScore:    r.Distance,
			ApproxRank:     rank,
			KeptForRescore: true,
		}
		c.trace.ApproximateRanking.TopCandidates = append(
			c.trace.ApproximateRanking.TopCandidates,
			candidate,
		)

		// Update the candidate map entry
		if existing, ok := c.candidateMap[r.ID]; ok {
			existing.ApproxRank = rank
			existing.KeptForRescore = true
		}
	}
}

// RecordLateInteractionCandidate records a candidate's exact MaxSim score.
func (c *SearchTraceCollector) RecordLateInteractionCandidate(docID uint64, approxScore, exactScore float32, exactRank int, returned bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.trace.LateInteraction.RescoredCandidates = append(
		c.trace.LateInteraction.RescoredCandidates,
		RescoredCandidateTrace{
			DocID:       docID,
			ApproxScore: approxScore,
			ExactScore:  exactScore,
			ExactRank:   exactRank,
			Returned:    returned,
		},
	)
}

// RecordLateInteractionStats records aggregate late interaction statistics.
func (c *SearchTraceCollector) RecordLateInteractionStats(candidatesRescored int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.LateInteraction.CandidatesRescored = candidatesRescored
}

// RecordFinalResults records the final returned results.
func (c *SearchTraceCollector) RecordFinalResults(ids []uint64, scores []float32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.trace.FinalResults = make([]ResultTrace, len(ids))
	for i, id := range ids {
		c.trace.FinalResults[i] = ResultTrace{
			DocID: id,
			Score: scores[i],
			Rank:  i,
		}
	}
}

// Context key for trace collector
type traceCollectorKey struct{}

// ContextWithTraceCollector returns a context with the trace collector attached.
func ContextWithTraceCollector(ctx context.Context, collector *SearchTraceCollector) context.Context {
	return context.WithValue(ctx, traceCollectorKey{}, collector)
}

// TraceCollectorFromContext retrieves the trace collector from context, or nil if not set.
func TraceCollectorFromContext(ctx context.Context) *SearchTraceCollector {
	if v := ctx.Value(traceCollectorKey{}); v != nil {
		return v.(*SearchTraceCollector)
	}
	return nil
}
