# Memory validation for HNSW compact

## What this directory ships for memory regression prevention

`SnapshotWriter` previously held a `[]*nodeState` slice indexed by node ID
(`ensureNodesCapacity`). For shards with sparse IDs the slice was 99 %+
nil pointers and peak heap during snapshot creation grew with `maxNodeID`
rather than with the live-set size — 100 k live nodes scattered across a
1×10⁸ ID space peaked at ~1.8 GB. The fix switches to
`nodes map[uint64]*nodeState` with a once-per-Flush sorted-keys walk; same
on-disk format, peak drops to ~30 MB.

This directory now contains the artifacts that prove the fix and guard
against regression.

## Files

| File | Purpose |
|---|---|
| `snapshot_writer.go` | The fixed writer. `nodes` is a sparse map; `writeBody` materializes sorted keys at Flush time and walks them with a cursor. |
| `memory_bench_test.go` | Peak-heap benchmarks for the sparse-ID, dense, high-fanout-merge, raw-WAL-conversion, and tombstone-churn scenarios. Each `Bench*` reports `PeakMB` / `HeapDeltaMB` / `NumGC` as custom metrics for `benchstat`. |
| `memory_integration_test.go` | Hard-ceiling regression gate under `//go:build integrationTest`. Currently passes at ~30 MB; fails if peak ever exceeds 100 MB. |

## Running

```bash
# Bench all memory-peak scenarios.
go test -run=^$ -bench=BenchmarkMemory -benchtime=1x -benchmem \
  ./adapters/repos/db/vector/hnsw/compact/

# Regression gate.
go test -tags=integrationTest -count=1 -v \
  -run=TestSparseSnapshotPeakRegression \
  ./adapters/repos/db/vector/hnsw/compact/
# Expect: PASS at ~30 MB peak

# Correctness gate after any future writer change.
go test -count=1 -race ./adapters/repos/db/vector/hnsw/compact/

# A/B against a tentative further optimization with benchstat.
go install golang.org/x/perf/cmd/benchstat@latest
git stash
go test -run=^$ -benchtime=5x -benchmem \
  -bench=BenchmarkMemorySparseSnapshot \
  ./adapters/repos/db/vector/hnsw/compact/ | tee /tmp/before.txt
git stash pop
go test -run=^$ -benchtime=5x -benchmem \
  -bench=BenchmarkMemorySparseSnapshot \
  ./adapters/repos/db/vector/hnsw/compact/ | tee /tmp/after.txt
benchstat /tmp/before.txt /tmp/after.txt
```

## What the fix does NOT solve

The writer-side fix is necessary but not sufficient for full O(liveNodes)
end-to-end memory:

1. **On-disk snapshot file size is still O(maxNodeID).** The V3 format
   emits one existence byte per slot from 0 to maxNodeID. For a
   maxID=1×10⁹ shard the snapshot file is ~1 GB even with the fix. A full
   solution would need a format-version bump with sparse-tuple encoding.
2. **`SnapshotReader.Read` materializes `Graph.Nodes []*Vertex` indexed
   by node ID.** Same symmetric problem on the read/startup path. Affects
   `Loader.Load` peak heap; not exercised by this benchmark.

Those axes warrant a separate change.
