# HFresh + MUVERA Search Trace Instrumentation

This document describes the diagnostic trace instrumentation for HFresh+MUVERA search, designed to support offline recall analysis by external benchmarking tools.

## Overview

The trace instrumentation captures internal search state at each stage of the HFresh+MUVERA pipeline without modifying search behavior. Weaviate itself does not evaluate recall — it only records what happened during search. An external benchmark tool can then compare these traces against ground-truth results to classify where recall was lost.

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

The `SearchTrace` struct contains:

### Search Parameters

```go
type SearchTrace struct {
    QueryID      string  // Caller-provided identifier
    IsMuvera     bool    // Whether this was a MUVERA multi-vector search
    K            int     // Requested result count
    SearchProbe  int     // Number of centroids to probe
    RescoreLimit int     // Maximum candidates for exact rescoring
    // ...
}
```

### Centroid/Posting Selection

```go
type CentroidSearchTrace struct {
    CandidateCentroidNum   int  // Centroids requested from HNSW
    CentroidsReturned      int  // Centroids returned by HNSW
    FilteredByDistance     int  // Skipped due to distance ratio
    FilteredByEmptyPosting int  // Skipped due to empty postings
    SelectedPostings       []PostingSelectionTrace
}

type PostingSelectionTrace struct {
    CentroidID       uint64   // Centroid/posting ID
    SelectionRank    int      // Position in centroid search results
    CentroidDistance float32  // Distance from query to centroid
    PostingSize      int      // Number of vectors in posting
}
```

### Posting Scan

```go
type PostingScanTrace struct {
    TotalVectorsScanned  int  // Total vectors examined
    SkippedDeleted       int  // Skipped as deleted
    SkippedDuplicate     int  // Skipped as duplicates
    SkippedAllowList     int  // Skipped by allow list filter
    CandidatesEnumerated []CandidateTrace
}

type CandidateTrace struct {
    DocID          uint64   // Document ID
    CentroidID     uint64   // Centroid where found (first occurrence)
    ApproxScore    float32  // RQ-compressed distance score
    ApproxRank     int      // Position in approximate ranking (-1 if not ranked)
    KeptForRescore bool     // Whether kept for exact rescoring
}
```

### Approximate Ranking

```go
type ApproximateRankingTrace struct {
    TotalCandidates int  // Unique candidates after posting scan
    KeptForRescore  int  // Candidates kept for exact rescoring
    TopCandidates   []CandidateTrace
}
```

### Late Interaction (MUVERA only)

```go
type LateInteractionTrace struct {
    CandidatesRescored int  // Number rescored with MaxSim
    RescoredCandidates []RescoredCandidateTrace
}

type RescoredCandidateTrace struct {
    DocID       uint64   // Document ID
    ApproxScore float32  // Approximate score from FDE search
    ExactScore  float32  // Exact MaxSim score
    ExactRank   int      // Position in exact ranking
    Returned    bool     // In final top-k
}
```

### Final Results

```go
type ResultTrace struct {
    DocID uint64   // Document ID
    Score float32  // Final score
    Rank  int      // Position in results (0-indexed)
}
```

## What Data is Available

The trace captures:

- **Search parameters**: k, searchProbe, rescoreLimit
- **Centroid selection**: which centroids were searched, filtered, selected
- **Posting details**: sizes, centroid distances
- **All enumerated candidates**: document IDs, approximate scores, centroid associations
- **Approximate ranking**: which candidates were kept for rescoring
- **Exact rescoring** (MUVERA): approximate vs exact scores for each rescored candidate
- **Final results**: returned IDs and scores

## What Data is NOT Available

Weaviate does not know:

- **Ground-truth results**: The correct answers for this query
- **Whether any ID is a true/false positive**: Only the benchmark has this information
- **Recall metrics**: Cannot be computed without ground truth
- **Why a specific ID was missed**: Only that it wasn't present at each stage

## How External Benchmarks Should Use Traces

An external benchmark tool should:

1. **Generate ground-truth results** (e.g., via brute-force MaxSim computation)

2. **Run traced searches** against HFresh+MUVERA

3. **Join traces with ground truth** to answer:

   - **Was ground-truth ID X in any selected posting?**
     Check if X appears in any `PostingSelectionTrace.CentroidID` that was scanned.
     If not, this is **centroid routing failure**.

   - **Was X enumerated as a candidate?**
     Check `PostingScanTrace.CandidatesEnumerated` for X.
     If X was in a selected posting but not enumerated, it was likely **deleted, duplicate, or filtered**.

   - **Was X kept for exact rescoring?**
     Check if X appears in `ApproximateRankingTrace.TopCandidates` with `KeptForRescore=true`.
     If X was enumerated but not kept, this is **approximate scoring failure**.

   - **Was X rescored?**
     Check `LateInteractionTrace.RescoredCandidates` for X.
     Compare `ApproxScore` vs `ExactScore` to measure **scoring correlation**.

   - **Was X in final results?**
     Check if X appears in `FinalResults` or if `RescoredCandidateTrace.Returned=true`.
     If X was rescored but not returned, it was **outranked** by other candidates.

4. **Classify recall loss** by counting ground-truth IDs at each stage:

   ```
   total_ground_truth = len(ground_truth_ids)
   in_selected_postings = count(gt_id in selected_posting_vectors)
   enumerated = count(gt_id in candidates_enumerated)
   kept_for_rescore = count(gt_id in top_candidates where kept=true)
   rescored = count(gt_id in rescored_candidates)
   returned = count(gt_id in final_results)

   routing_loss = total_ground_truth - in_selected_postings
   enumeration_loss = in_selected_postings - enumerated
   approx_ranking_loss = enumerated - kept_for_rescore
   exact_ranking_loss = kept_for_rescore - returned
   ```

## Important Limitation

**Weaviate traces explain the search path, but only the external benchmark can classify recall loss because only the benchmark has the reference answer set.**

The trace provides complete visibility into what Weaviate did, but cannot answer "was this the right thing to do" without external ground truth.

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
- When tracing is enabled, additional allocations occur to record candidates
- For large result sets, `CandidatesEnumerated` may contain thousands of entries
- The trace is designed for diagnostic use, not production workloads

## Example: Recall Analysis Script

```python
import json

def analyze_recall(trace_json, ground_truth_ids):
    trace = json.loads(trace_json)
    gt_set = set(ground_truth_ids)

    # Build sets from trace
    selected_posting_ids = set()
    for posting in trace['centroid_search']['selected_postings']:
        # Would need posting contents - trace only shows posting metadata
        # This requires additional instrumentation or separate lookup
        pass

    enumerated_ids = set(
        c['doc_id'] for c in trace['posting_scan']['candidates_enumerated']
    )

    kept_ids = set(
        c['doc_id'] for c in trace['approximate_ranking']['top_candidates']
        if c['kept_for_rescore']
    )

    rescored_ids = set(
        c['doc_id'] for c in trace['late_interaction']['rescored_candidates']
    )

    returned_ids = set(r['doc_id'] for r in trace['final_results'])

    # Compute stage-wise recall
    enumerated_recall = len(gt_set & enumerated_ids) / len(gt_set)
    kept_recall = len(gt_set & kept_ids) / len(gt_set)
    returned_recall = len(gt_set & returned_ids) / len(gt_set)

    return {
        'enumerated_recall': enumerated_recall,
        'kept_recall': kept_recall,
        'returned_recall': returned_recall,
        'lost_at_enumeration': gt_set - enumerated_ids,
        'lost_at_approx_ranking': (gt_set & enumerated_ids) - kept_ids,
        'lost_at_final_ranking': (gt_set & kept_ids) - returned_ids,
    }
```

## Files

- `adapters/repos/db/vector/hfresh/search_trace.go` - Trace data structures and collector
- `adapters/repos/db/vector/hfresh/search.go` - Instrumented search code
- `adapters/repos/db/vector/hfresh/search_trace_test.go` - Tests
