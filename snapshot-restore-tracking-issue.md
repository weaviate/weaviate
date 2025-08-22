# Panic during HNSW snapshot restoration in quantized vector cache

## Summary
Weaviate panics with "index out of range" error during HNSW snapshot restoration when using binary quantization (BQ). The panic occurs in the vector cache system during vector search operations after snapshot loading.

## Environment
- **Weaviate Version**: 1.33.0-dev
- **Build Commit**: 8895ab6
- **Go Version**: go1.24.6
- **Vector Index**: HNSW with Binary Quantization (BQ)
- **Test**: `test_hnsw_snapshots_recovery_scenarios[hnsw_bq]`

## Error Details
```
panic: runtime error: index out of range [1207] with length 1000
```

## Stack Trace
```
goroutine 2841 [running]:
github.com/weaviate/weaviate/adapters/repos/db/vector/cache.(*shardedLockCache[...]).Get(0x450d260, {0x44de3e0, 0x782ce00}, 0x4b7)
	/go/src/github.com/weaviate/weaviate/adapters/repos/db/vector/cache/sharded_lock_cache.go:138 +0xc9
github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers.(*quantizedCompressorDistancer[...]).DistanceToNode(0xc000dbea50, 0x4b7?)
	/go/src/github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers/compression.go:777 +0x3d
github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw.(*hnsw).distToNode(0xc0015ec008?, {0x44ccb88?, 0xc001c2cba0?}, 0x100?, {0xc001682600?, 0x10000000000?, 0xc001523e80?})
	/go/src/github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/index.go:616 +0x3f
github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw.(*hnsw).knnSearchByVector(0xc0015ec008, {0x44de618, 0xc0023dccf0}, {0xc001682600, 0x180, 0x180}, 0xa, 0x200, {0x0, 0x0})
	/go/src/github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/search.go:714 +0x1e5
github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw.(*hnsw).SearchByVector(0xc0015ec008, {0x44de618, 0xc0023dccf0}, {0xc001682600?, 0x1?, 0x1?}, 0xa, {0x0, 0x0})
	/go/src/github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/search.go:91 +0x38d
github.com/weaviate/weaviate/adapters/repos/db.(*Shard).ObjectVectorSearch.func2()
	/go/src/github.com/weaviate/weaviate/adapters/repos/db/shard_read.go:460 +0x390
github.com/weaviate/weaviate/adapters/repos/db.(*Shard).ObjectVectorSearch.(*ErrorGroupWrapper).Go.func3()
	/go/src/github.com/weaviate/weaviate/entities/errors/error_group_wrapper.go:102 +0x8b
golang.org/x/sync/errgroup.(*Group).Go.func1()
	/go/pkg/mod/golang.org/x/sync@v0.16.0/errgroup/errgroup.go:93 +0x50
created by golang.org/x/sync/errgroup.(*Group).Go in goroutine 2809
	/go/pkg/mod/golang.org/x/sync@v0.16.0/errgroup/errgroup.go:78 +0x93
```

## Context
This panic occurred during E2E testing of HNSW snapshot recovery scenarios. The sequence of events:

1. ✅ Snapshot successfully loaded: `snapshot loaded, took: 718.339µs`
2. ✅ Data restored from disk: `restored data from disk, duration: 768.865µs`
3. ✅ Shard loading completed: `Completed loading shard testhnswsnapshotsrecoveryscenarioshnswbqhnsw_bq1_T0W30xRvxyFd in 1.425737ms`
4. ❌ **PANIC** during vector search operation

## Analysis
The panic occurs in `sharded_lock_cache.go:138` when trying to access index `[1207]` in a collection with length `1000`. This suggests:

- **Cache size mismatch**: The vector cache was initialized with 1000 entries but the system is trying to access index 1207
- **Index bounds issue**: During snapshot restoration, the cache bounds may not be properly synchronized with the actual data
- **BQ-specific issue**: This may be related to how binary quantization handles cache indexing after snapshot restoration

## Reproduction
- Occurs specifically with `hnsw_bq` (HNSW + Binary Quantization)
- Happens during snapshot restoration scenarios
- Not consistently reproducible (race condition characteristics)

## Priority
**High** - This is a crash/panic that affects snapshot recovery functionality with binary quantization.

## Analysis and Suggested Fixes

### Root Cause Analysis

**Missing Bounds Check in Cache Access**
The panic occurs in `shardedLockCache.Get()` method which accesses the cache without bounds checking:

```go
func (s *shardedLockCache[T]) Get(ctx context.Context, id uint64) ([]T, error) {
	s.shardedLocks.RLock(id)
	vec := s.cache[id]  // ❌ NO BOUNDS CHECK - PANIC HERE
	s.shardedLocks.RUnlock(id)

	if vec != nil {
		return vec, nil
	}

	return s.handleCacheMiss(ctx, id)
}
```

**Inconsistent Bounds Checking**
Other methods like `Delete` properly implement bounds checking:
```go
if int(id) >= len(s.cache) || s.cache[id] == nil {
    return
}
```

**Potential Concurrency Issues**
1. **Entry Point ID Race**: The `entryPointID` is read with a lock, but there might be concurrent writes happening without proper synchronization
   ```go
    func (h *hnsw) knnSearchByVector(ctx context.Context, searchVec []float32, k int,
        ...
        h.RLock()
        entryPointID := h.entryPointID
        maxLayer := h.currentMaximumLayer
        h.RUnlock()
    
        var compressorDistancer compressionhelpers.CompressorDistancer
        if h.compressed.Load() {
            var returnFn compressionhelpers.ReturnDistancerFn
            compressorDistancer, returnFn = h.compressor.NewDistancer(searchVec)
            defer returnFn()
        }
        entryPointDistance, err := h.distToNode(compressorDistancer, entryPointID, searchVec) // ❌ Invalid entryPointID = 1207
        ...
    ```
2. **Cache Resizing Without Locks**: Methods like `SetSizeAndGrowNoLock()` and `PreloadNoLock()` modify cache without holding locks:
   ```go
   func (s *shardedLockCache[T]) SetSizeAndGrowNoLock(size uint64) {
       newCache := make([][]T, newSize)
       copy(newCache, s.cache)
       s.cache = newCache  // ❌ Race condition potential
   }
   ```

**Startup Timing Issue**
Suspected race condition in vector index initialization (`shard_init_vector.go`):
```go
    func (s *Shard) initVectorIndex(
	...
    defer vectorIndex.PostStartup()
    return vectorIndex, nil
```
The vector index might be used before `PostStartup()` completes, leading to inconsistent state between cache size and actual data.

### Suggested Fixes

1. **Add Bounds Check to Get Method**
   ```go
   func (s *shardedLockCache[T]) Get(ctx context.Context, id uint64) ([]T, error) {
       s.shardedLocks.RLock(id)
       defer s.shardedLocks.RUnlock(id)
       
       if int(id) >= len(s.cache) {
           return s.handleCacheMiss(ctx, id)
       }
       
       vec := s.cache[id]
       if vec != nil {
           return vec, nil
       }
       
       return s.handleCacheMiss(ctx, id)
   }
   ```

2. **Review Locking in NoLock Methods**
   Investigate if `SetSizeAndGrowNoLock` and `PreloadNoLock` need proper locking during snapshot restoration.

3. **Fix Startup Sequence**
   Ensure `PostStartup()` completes before the vector index is available for use, rather than using `defer`.

4. **Validate Entry Point ID**
   Add validation that `entryPointID` is within valid bounds before using it for cache access.

## Related Files
- `/adapters/repos/db/vector/cache/sharded_lock_cache.go:138`
- `/adapters/repos/db/vector/compressionhelpers/compression.go:777`
- `/adapters/repos/db/vector/hnsw/search.go:714`

## Relevant PRs
- https://github.com/weaviate/weaviate/pull/8841: in this PR we replace `CopyShardingState` with `Read` to access the `sharding.State` directly instead of copying it.
- https://github.com/weaviate/weaviate/pull/8870: in this PR we update a test and the nodes api to correctly report the vector index compression status.