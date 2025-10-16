# Add Prometheus Metric for HNSW Memory Allocation Rejections

This PR is a follow-up to [#9092](https://github.com/weaviate/weaviate/pull/9092), which introduced memory estimation and allocation checking to prevent out-of-memory situations during HNSW operations. That PR added protective logic to reject operations when insufficient memory is available using `CheckAlloc()` admission control. However, we had no visibility into how often operations were being rejected due to memory pressure.

This PR adds observability for the OOM prevention mechanism by tracking when batch operations are rejected, enabling alerts and monitoring for memory pressure situations that may require vertical scaling.

## Metric

This PR adds `vector_index_memory_allocation_rejected_total`, a **counter** tracking batch operations rejected due to insufficient memory. This metric is incremented whenever `CheckAlloc()` prevents a batch operation from proceeding due to memory constraints.

The metric includes `class_name` and `shard_name` labels for granular analysis, allowing operators to identify which specific indexes are experiencing memory pressure.

## Use Cases

This metric enables several operational scenarios:

1. **Alerting**: Alert when rejection rate exceeds a threshold for sustained periods (e.g., >5 minutes), indicating the need for vertical scaling
2. **Capacity Planning**: Track rejection trends to understand when indexes are approaching memory limits
3. **Auto-scaling Triggers**: Use rejection rate as a signal for automated vertical scaling in managed environments
4. **Validation**: Confirm that OOM prevention is working correctly in production

## Implementation

The implementation is straightforward and zero-overhead when metrics are disabled:

```go
estimatedMemory := estimateBatchMemory(vectors)
if err := h.allocChecker.CheckAlloc(estimatedMemory); err != nil {
    h.metrics.MemoryAllocationRejected()  // Track rejection
    return fmt.Errorf("add batch of %d vectors: %w", len(vectors), err)
}
```

The counter is incremented only when admission control rejects an operation, providing a direct signal of memory pressure without any sampling or continuous monitoring overhead.

## Design Decision: Simple and Focused

An earlier iteration of this PR included a `vector_index_batch_memory_estimation_ratio` gauge that attempted to measure the accuracy of memory estimation by comparing estimated vs actual memory usage. However, this metric was removed because:

1. **Concurrent operations corrupt measurements**: Go's `runtime.ReadMemStats()` measures global heap allocation, not per-operation usage. Multiple concurrent batch operations made it impossible to accurately attribute memory changes to specific operations.

2. **Not needed for OOM prevention**: The goal of memory estimation is admission control ("am I too close to my limit?"), not perfect accuracy. Over-estimation is safer than under-estimation, and estimation errors are self-correcting (if we over-allocate slightly, the next operation gets rejected).

3. **Performance overhead**: `ReadMemStats()` causes stop-the-world GC pauses, even with sampling.

The rejection counter provides the essential observability needed without the complexity and overhead of attempting to measure per-operation memory usage. Future improvements to estimation accuracy can be added based on production data from this metric.

## Testing

Unit tests verify that the counter increments correctly when `CheckAlloc()` rejects operations due to insufficient memory.
