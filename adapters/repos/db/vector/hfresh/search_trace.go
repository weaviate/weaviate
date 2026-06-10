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
// offline diagnostic analysis. This is a minimal membership-based trace
// designed to classify recall loss into three categories:
//
//   - Routing failure: ground-truth doc not in any selected posting
//   - Approximate-ranking failure: doc in posting but not in approx top
//   - Exact-ranking failure: doc in approx top but not in final results
//
// Weaviate does not evaluate recall — it only records document membership
// at each stage. An external benchmark tool compares traces against
// ground-truth to classify where recall was lost.
//
// Usage:
//
//	collector := hfresh.NewSearchTraceCollector("query-123")
//	ctx := hfresh.ContextWithTraceCollector(ctx, collector)
//	ids, dists, err := index.SearchByMultiVector(ctx, vectors, k, nil)
//	trace := collector.Trace()
type SearchTrace struct {
	// QueryID is an optional caller-provided identifier for correlation.
	QueryID string `json:"query_id,omitempty"`

	// IsMuvera indicates whether this was a MUVERA multi-vector search.
	IsMuvera bool `json:"is_muvera"`

	// Search parameters
	K            int `json:"k"`
	SearchProbe  int `json:"search_probe"`
	RescoreLimit int `json:"rescore_limit"`

	// SelectedCentroids contains the centroid IDs of postings that were
	// selected for scanning. External benchmark uses these IDs to check
	// posting membership for routing failure attribution.
	SelectedCentroids []uint64 `json:"selected_centroids"`

	// ScanStats contains aggregate statistics from the posting scan phase.
	ScanStats ScanStats `json:"scan_stats"`

	// ApproxTopIDs contains document IDs that made the approximate ranking
	// (top rescoreLimit candidates). Used to detect approximate-ranking failure.
	ApproxTopIDs []uint64 `json:"approx_top_ids"`

	// ReturnedIDs contains document IDs in the final results.
	// Used to detect exact-ranking failure.
	ReturnedIDs []uint64 `json:"returned_ids"`
}

// ScanStats contains aggregate statistics from posting scan.
type ScanStats struct {
	// TotalScanned is the total number of vectors examined across all postings.
	TotalScanned int `json:"total_scanned"`

	// UniqueEnumerated is the number of unique candidates after filtering.
	UniqueEnumerated int `json:"unique_enumerated"`

	// SkippedDeleted is the count of vectors skipped because they were deleted.
	SkippedDeleted int `json:"skipped_deleted"`

	// SkippedDuplicate is the count of vectors skipped as duplicates.
	SkippedDuplicate int `json:"skipped_duplicate"`

	// SkippedAllowList is the count of vectors skipped due to allow list filtering.
	SkippedAllowList int `json:"skipped_allow_list"`
}

// SearchTraceCollector accumulates trace data during search execution.
// It is safe for use within a single search but should not be shared
// across concurrent searches.
type SearchTraceCollector struct {
	mu    sync.Mutex
	trace SearchTrace
}

// NewSearchTraceCollector creates a new collector with an optional query ID.
func NewSearchTraceCollector(queryID string) *SearchTraceCollector {
	return &SearchTraceCollector{
		trace: SearchTrace{
			QueryID: queryID,
		},
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

// RecordSelectedCentroids records the centroid IDs of selected postings.
func (c *SearchTraceCollector) RecordSelectedCentroids(centroidIDs []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.SelectedCentroids = make([]uint64, len(centroidIDs))
	copy(c.trace.SelectedCentroids, centroidIDs)
}

// RecordScanStats records aggregate posting scan statistics.
func (c *SearchTraceCollector) RecordScanStats(totalScanned, uniqueEnumerated, skippedDeleted, skippedDuplicate, skippedAllowList int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.ScanStats = ScanStats{
		TotalScanned:     totalScanned,
		UniqueEnumerated: uniqueEnumerated,
		SkippedDeleted:   skippedDeleted,
		SkippedDuplicate: skippedDuplicate,
		SkippedAllowList: skippedAllowList,
	}
}

// RecordApproxTopIDs records the document IDs that made approximate ranking.
func (c *SearchTraceCollector) RecordApproxTopIDs(ids []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.ApproxTopIDs = make([]uint64, len(ids))
	copy(c.trace.ApproxTopIDs, ids)
}

// RecordReturnedIDs records the document IDs in final results.
func (c *SearchTraceCollector) RecordReturnedIDs(ids []uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trace.ReturnedIDs = make([]uint64, len(ids))
	copy(c.trace.ReturnedIDs, ids)
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
