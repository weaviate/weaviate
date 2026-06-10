# HFresh + MUVERA Search Trace Instrumentation

This document describes the minimal diagnostic trace instrumentation for HFresh+MUVERA search, designed to support offline recall attribution by external benchmarking tools.

## Overview

The trace instrumentation captures minimal membership data at each stage of the HFresh+MUVERA pipeline without modifying search behavior. Weaviate itself does not evaluate recall — it only records document IDs at each stage. An external benchmark tool can then compare these traces against ground-truth results to classify where recall was lost.

**Design Principle**: All three recall failure categories (routing, approximate-ranking, exact-ranking) are membership tests. Scores are not required for attribution, so the trace captures only document IDs.

## Enabling Diagnostics

Diagnostics are disabled by default and have zero overhead when not enabled.

To enable tracing, attach a `SearchTraceCollector` to the context:

```go
import "github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"

// Create a collector with an optional query ID for correlation
collector := hfresh.NewSearchTraceCollector("query-123")

// Attach to context
ctx := hfresh.ContextWithTraceCollector(ctx, collector)

// Run search as normal
ids, dists, err := index.SearchByMultiVector(ctx, queryVectors, k, allowList)

// Retrieve trace after search completes
trace := collector.Trace()
```

## Trace Data Structure

The trace captures minimal membership data (~3.4KB per query vs ~37KB in the original design):

```go
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
```

## What Data is Captured

The trace records:

- **Search parameters**: k, searchProbe, rescoreLimit
- **Selected centroids**: IDs of postings that were probed
- **Scan statistics**: Aggregate counts of vectors scanned and filtered
- **Approx top IDs**: Document IDs that made the approximate ranking
- **Returned IDs**: Document IDs in the final results

## What Data is NOT Captured

The trace does not include:

- **Scores**: Neither approximate nor exact scores are recorded
- **Per-candidate details**: No individual candidate information
- **Ground-truth results**: Only the external benchmark has this
- **Recall metrics**: Cannot be computed without ground truth

## Recall Attribution Workflow

An external benchmark classifies recall loss by comparing the trace against ground truth:

### Step 1: External Benchmark Preparation

The benchmark must pre-compute:
1. **Ground-truth results** for each query (e.g., via brute-force MaxSim)
2. **Posting membership** for each document: which centroid postings contain each doc

### Step 2: Membership Classification

For each ground-truth document ID, check:

1. **Routing failure**: Is the document in any selected posting?
   - Check if doc's posting IDs intersect with `SelectedCentroids`
   - If no intersection → routing failure

2. **Approximate-ranking failure**: Is the document in `ApproxTopIDs`?
   - If doc was in a selected posting but NOT in `ApproxTopIDs` → approx failure

3. **Exact-ranking failure**: Is the document in `ReturnedIDs`?
   - If doc was in `ApproxTopIDs` but NOT in `ReturnedIDs` → exact failure

4. **Success**: Document is in `ReturnedIDs`

### Example: Python Attribution Code

```python
import json

def classify_recall(trace_json, ground_truth_ids, posting_membership):
    """
    Classify recall failures for a single query.

    Args:
        trace_json: JSON string of SearchTrace
        ground_truth_ids: list of correct document IDs for this query
        posting_membership: dict mapping doc_id -> list of posting/centroid IDs

    Returns:
        dict with classification counts and ID lists
    """
    trace = json.loads(trace_json)

    # Build sets for fast lookup
    selected_centroids = set(trace['selected_centroids'])
    approx_top = set(trace['approx_top_ids'])
    returned = set(trace['returned_ids'])

    # Classify each ground-truth ID
    routing_failures = []
    approx_failures = []
    exact_failures = []
    successes = []

    for gt_id in ground_truth_ids:
        # Check if doc is in any selected posting
        doc_postings = set(posting_membership.get(gt_id, []))
        in_selected_posting = bool(doc_postings & selected_centroids)

        if not in_selected_posting:
            routing_failures.append(gt_id)
        elif gt_id not in approx_top:
            approx_failures.append(gt_id)
        elif gt_id not in returned:
            exact_failures.append(gt_id)
        else:
            successes.append(gt_id)

    return {
        'routing_failures': routing_failures,
        'approx_failures': approx_failures,
        'exact_failures': exact_failures,
        'successes': successes,
        'recall': len(successes) / len(ground_truth_ids) if ground_truth_ids else 1.0
    }
```

### Building Posting Membership

The external benchmark must build the posting membership map by either:

1. **Query the index directly**: Use HFresh APIs to get posting contents per centroid
2. **Pre-compute during indexing**: Track which posting each document was assigned to
3. **Re-encode at benchmark time**: Run FDE encoding and centroid assignment offline

## Memory Footprint

Estimated per-query memory usage:

| Field | Size | Typical Values |
|-------|------|----------------|
| SelectedCentroids | 8 bytes × searchProbe | 64 × 8 = 512 bytes |
| ApproxTopIDs | 8 bytes × rescoreLimit | 100 × 8 = 800 bytes |
| ReturnedIDs | 8 bytes × k | 10 × 8 = 80 bytes |
| ScanStats | 5 × 8 bytes | 40 bytes |
| Fixed fields | ~50 bytes | 50 bytes |
| **Total** | | **~1.5KB typical, ~3.4KB max** |

This is 90% smaller than the original design (~37KB per query) which recorded per-candidate scores and all enumerated candidates.

## Concurrency Safety

Each `SearchTraceCollector` is independent and safe for use within a single search operation. Multiple concurrent searches should each create their own collector:

```go
// Safe: each goroutine has its own collector
go func() {
    collector := hfresh.NewSearchTraceCollector("query-A")
    ctx := hfresh.ContextWithTraceCollector(ctx, collector)
    // ... search ...
}()

go func() {
    collector := hfresh.NewSearchTraceCollector("query-B")
    ctx := hfresh.ContextWithTraceCollector(ctx, collector)
    // ... search ...
}()
```

## Performance Considerations

- When no collector is in context, there is zero overhead
- When tracing is enabled, minimal allocations occur (only ID slices)
- No per-candidate recording in the hot posting-scan loop
- Designed for diagnostic use, but low enough overhead for sampling in production

## Files

- `adapters/repos/db/vector/hfresh/search_trace.go` - Trace data structures and collector
- `adapters/repos/db/vector/hfresh/search.go` - Instrumented search code
- `adapters/repos/db/vector/hfresh/search_trace_test.go` - Tests including recall attribution workflow
