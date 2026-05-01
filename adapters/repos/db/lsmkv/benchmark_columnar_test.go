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

package lsmkv

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/columnar"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// TestColumnarBucket_BasicRoundTrip verifies writes, flush, and reads.
func TestColumnarBucket_BasicRoundTrip(t *testing.T) {
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	schema := &columnar.Schema{
		Columns: []columnar.Column{
			{Name: "popularity", Type: columnar.ColumnTypeFloat32},
			{Name: "created_at", Type: columnar.ColumnTypeInt64},
		},
	}

	b, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil, // metrics
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
		WithUseBloomFilter(false),
		WithCalcCountNetAdditions(false),
	)
	require.NoError(t, err)
	defer b.Shutdown(context.Background())

	// Write some data
	for i := uint64(0); i < 100; i++ {
		require.NoError(t, b.ColumnarPutFloat32(i, 0, float32(i)*0.1))
		require.NoError(t, b.ColumnarPutInt64(i, 1, int64(i)*1000))
	}

	// Read from memtable
	val, ok := b.ColumnarLookupFloat32(42, 0)
	assert.True(t, ok)
	assert.InDelta(t, float32(4.2), val, 0.01)

	ts, ok := b.ColumnarLookupInt64(42, 1)
	assert.True(t, ok)
	assert.Equal(t, int64(42000), ts)

	// Miss
	_, ok = b.ColumnarLookupFloat32(999, 0)
	assert.False(t, ok)

	// Force flush
	require.NoError(t, b.FlushMemtable())

	// Read from disk segment
	val, ok = b.ColumnarLookupFloat32(42, 0)
	assert.True(t, ok)
	assert.InDelta(t, float32(4.2), val, 0.01)

	ts, ok = b.ColumnarLookupInt64(42, 1)
	assert.True(t, ok)
	assert.Equal(t, int64(42000), ts)

	// Write more data (into new memtable), overriding docID 42
	require.NoError(t, b.ColumnarPutFloat32(42, 0, 99.9))

	val, ok = b.ColumnarLookupFloat32(42, 0)
	assert.True(t, ok)
	assert.InDelta(t, float32(99.9), val, 0.1)

	// Original docID 10 still readable from segment
	val, ok = b.ColumnarLookupFloat32(10, 0)
	assert.True(t, ok)
	assert.InDelta(t, float32(1.0), val, 0.01)
}

// TestColumnarBucket_Compaction verifies that two flushed segments
// compact correctly.
func TestColumnarBucket_Compaction(t *testing.T) {
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	schema := &columnar.Schema{
		Columns: []columnar.Column{
			{Name: "score", Type: columnar.ColumnTypeFloat32},
		},
	}

	b, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
		WithUseBloomFilter(false),
		WithCalcCountNetAdditions(false),
	)
	require.NoError(t, err)
	defer b.Shutdown(context.Background())

	// Segment 1: docIDs 0..49
	for i := uint64(0); i < 50; i++ {
		require.NoError(t, b.ColumnarPutFloat32(i, 0, float32(i)))
	}
	require.NoError(t, b.FlushMemtable())

	// Segment 2: docIDs 25..74 (overlaps with segment 1 on 25..49)
	for i := uint64(25); i < 75; i++ {
		require.NoError(t, b.ColumnarPutFloat32(i, 0, float32(i)+1000))
	}
	require.NoError(t, b.FlushMemtable())

	// Compact
	compacted, err := b.disk.compactOnce()
	require.NoError(t, err)
	require.True(t, compacted)

	// Verify: docIDs 0..24 have original values, 25..74 have newer values
	for i := uint64(0); i < 25; i++ {
		val, ok := b.ColumnarLookupFloat32(i, 0)
		assert.True(t, ok, "docID %d", i)
		assert.InDelta(t, float32(i), val, 0.01, "docID %d", i)
	}
	for i := uint64(25); i < 75; i++ {
		val, ok := b.ColumnarLookupFloat32(i, 0)
		assert.True(t, ok, "docID %d", i)
		assert.InDelta(t, float32(i)+1000, val, 0.01, "docID %d", i)
	}
}

// TestColumnarBucket_Tombstones verifies deletion across memtable and segment.
func TestColumnarBucket_Tombstones(t *testing.T) {
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()

	schema := &columnar.Schema{
		Columns: []columnar.Column{
			{Name: "val", Type: columnar.ColumnTypeFloat32},
		},
	}

	b, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
		WithUseBloomFilter(false),
		WithCalcCountNetAdditions(false),
	)
	require.NoError(t, err)
	defer b.Shutdown(context.Background())

	// Write and flush
	require.NoError(t, b.ColumnarPutFloat32(1, 0, 42.0))
	require.NoError(t, b.FlushMemtable())

	// Delete in new memtable
	require.NoError(t, b.ColumnarDelete(1))

	// Should be gone
	_, ok := b.ColumnarLookupFloat32(1, 0)
	assert.False(t, ok)
}

// ----- Benchmarks -----

const (
	benchNumDocs    = 50_000
	benchNumReads   = 1_000
	benchNumWriters = 2
)

func setupColumnarBucket(b *testing.B) *Bucket {
	dir, err := os.MkdirTemp("", "bench-columnar-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	logger, _ := test.NewNullLogger()
	schema := &columnar.Schema{
		Columns: []columnar.Column{
			{Name: "popularity", Type: columnar.ColumnTypeFloat32},
			{Name: "created_at", Type: columnar.ColumnTypeInt64},
		},
	}

	bucket, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
		WithUseBloomFilter(false),
		WithCalcCountNetAdditions(false),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { bucket.Shutdown(context.Background()) })

	// Populate and flush to create a disk segment
	for i := uint64(0); i < benchNumDocs; i++ {
		if err := bucket.ColumnarPutFloat32(i, 0, float32(i)*0.001); err != nil {
			b.Fatal(err)
		}
		if err := bucket.ColumnarPutInt64(i, 1, int64(i)*1_000_000); err != nil {
			b.Fatal(err)
		}
	}
	if err := bucket.FlushMemtable(); err != nil {
		b.Fatal(err)
	}

	// Purge filesystem cache as much as possible (best-effort).
	// On macOS there's no easy way to purge per-file cache from Go;
	// on Linux we'd use posix_fadvise DONTNEED.
	// The mmap'd segment will fault pages back in on access, so
	// the first iteration of reads will reflect cold-cache latency.

	return bucket
}

func setupReplaceBucket(b *testing.B) *Bucket {
	dir, err := os.MkdirTemp("", "bench-replace-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { bucket.Shutdown(context.Background()) })

	// Store the same data as 8-byte key → 12-byte value (float32 + int64)
	for i := uint64(0); i < benchNumDocs; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, i)

		val := make([]byte, 12)
		binary.LittleEndian.PutUint32(val[0:], math.Float32bits(float32(i)*0.001))
		binary.LittleEndian.PutUint64(val[4:], uint64(i)*1_000_000)

		if err := bucket.Put(key, val); err != nil {
			b.Fatal(err)
		}
	}
	if err := bucket.FlushMemtable(); err != nil {
		b.Fatal(err)
	}

	return bucket
}

// BenchmarkColumnar_RandomRead measures random reads from a flushed
// columnar segment.
func BenchmarkColumnar_RandomRead(b *testing.B) {
	bucket := setupColumnarBucket(b)
	rng := rand.New(rand.NewSource(42))

	// Pre-generate random docIDs
	docIDs := make([]uint64, benchNumReads)
	for i := range docIDs {
		docIDs[i] = uint64(rng.Intn(benchNumDocs))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docID := docIDs[i%benchNumReads]
		val, ok := bucket.ColumnarLookupFloat32(docID, 0)
		if !ok {
			b.Fatalf("docID %d not found", docID)
		}
		_ = val
	}
}

// BenchmarkReplace_RandomRead measures random reads from a flushed
// replace-strategy segment for comparison.
func BenchmarkReplace_RandomRead(b *testing.B) {
	bucket := setupReplaceBucket(b)
	rng := rand.New(rand.NewSource(42))

	docIDs := make([]uint64, benchNumReads)
	for i := range docIDs {
		docIDs[i] = uint64(rng.Intn(benchNumDocs))
	}

	key := make([]byte, 8)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docID := docIDs[i%benchNumReads]
		binary.BigEndian.PutUint64(key, docID)
		val, err := bucket.Get(key)
		if err != nil {
			b.Fatalf("docID %d: %v", docID, err)
		}
		_ = val
	}
}

// BenchmarkColumnar_RandomReadUnderWrites measures random reads while
// concurrent writers are adding new data to the memtable.
func BenchmarkColumnar_RandomReadUnderWrites(b *testing.B) {
	bucket := setupColumnarBucket(b)
	rng := rand.New(rand.NewSource(42))

	docIDs := make([]uint64, benchNumReads)
	for i := range docIDs {
		docIDs[i] = uint64(rng.Intn(benchNumDocs))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start concurrent writers
	var writeCount atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < benchNumWriters; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-ctx.Done():
					return
				default:
					docID := uint64(benchNumDocs + r.Intn(100_000))
					_ = bucket.ColumnarPutFloat32(docID, 0, r.Float32())
					writeCount.Add(1)
				}
			}
		}(int64(w))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docID := docIDs[i%benchNumReads]
		val, ok := bucket.ColumnarLookupFloat32(docID, 0)
		if !ok {
			b.Fatalf("docID %d not found", docID)
		}
		_ = val
	}

	b.StopTimer()
	cancel()
	wg.Wait()

	b.ReportMetric(float64(writeCount.Load()), "concurrent_writes")
}

// BenchmarkReplace_RandomReadUnderWrites measures the same for replace strategy.
func BenchmarkReplace_RandomReadUnderWrites(b *testing.B) {
	bucket := setupReplaceBucket(b)
	rng := rand.New(rand.NewSource(42))

	docIDs := make([]uint64, benchNumReads)
	for i := range docIDs {
		docIDs[i] = uint64(rng.Intn(benchNumDocs))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var writeCount atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < benchNumWriters; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			r := rand.New(rand.NewSource(seed))
			key := make([]byte, 8)
			val := make([]byte, 12)
			for {
				select {
				case <-ctx.Done():
					return
				default:
					docID := uint64(benchNumDocs + r.Intn(100_000))
					binary.BigEndian.PutUint64(key, docID)
					binary.LittleEndian.PutUint32(val[0:], math.Float32bits(r.Float32()))
					binary.LittleEndian.PutUint64(val[4:], docID)
					_ = bucket.Put(key, val)
					writeCount.Add(1)
				}
			}
		}(int64(w))
	}

	key := make([]byte, 8)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docID := docIDs[i%benchNumReads]
		binary.BigEndian.PutUint64(key, docID)
		val, err := bucket.Get(key)
		if err != nil {
			b.Fatalf("docID %d: %v", docID, err)
		}
		_ = val
	}

	b.StopTimer()
	cancel()
	wg.Wait()

	b.ReportMetric(float64(writeCount.Load()), "concurrent_writes")
}

// BenchmarkColumnar_ScoringLoop simulates a realistic scoring loop where
// the search engine retrieves float32 and int64 values for candidate
// documents, as would happen in a reranking or boosting phase.
func BenchmarkColumnar_ScoringLoop(b *testing.B) {
	bucket := setupColumnarBucket(b)
	rng := rand.New(rand.NewSource(99))

	// Simulate a result set of 100 candidates (typical for reranking)
	candidates := make([]uint64, 100)
	for i := range candidates {
		candidates[i] = uint64(rng.Intn(benchNumDocs))
	}

	now := time.Now().UnixNano()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		totalScore := float64(0)
		for _, docID := range candidates {
			score := float64(1.0)

			if pop, ok := bucket.ColumnarLookupFloat32(docID, 0); ok {
				score *= float64(1.0 + pop)
			}
			if ts, ok := bucket.ColumnarLookupInt64(docID, 1); ok {
				age := float64(now - ts)
				if age > 0 {
					score *= 1.0 / (1.0 + age*1e-15)
				}
			}
			totalScore += score
		}
		_ = totalScore
	}
}

// BenchmarkReplace_ScoringLoop is the equivalent scoring loop using
// replace-strategy bucket for comparison.
func BenchmarkReplace_ScoringLoop(b *testing.B) {
	bucket := setupReplaceBucket(b)
	rng := rand.New(rand.NewSource(99))

	candidates := make([]uint64, 100)
	for i := range candidates {
		candidates[i] = uint64(rng.Intn(benchNumDocs))
	}

	now := time.Now().UnixNano()
	key := make([]byte, 8)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		totalScore := float64(0)
		for _, docID := range candidates {
			score := float64(1.0)

			binary.BigEndian.PutUint64(key, docID)
			val, err := bucket.Get(key)
			if err == nil && len(val) >= 12 {
				pop := math.Float32frombits(binary.LittleEndian.Uint32(val[0:]))
				score *= float64(1.0 + pop)

				ts := int64(binary.LittleEndian.Uint64(val[4:]))
				age := float64(now - ts)
				if age > 0 {
					score *= 1.0 / (1.0 + age*1e-15)
				}
			}
			totalScore += score
		}
		_ = totalScore
	}
}

// BenchmarkColumnar_LargeSegmentRead tests read performance with a larger
// dataset that may exceed L1/L2 cache. This helps surface the difference
// between columnar (cache-friendly sequential column access) and replace
// (random B-tree traversal) strategies.
func BenchmarkColumnar_LargeSegmentRead(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-col-large-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	logger, _ := test.NewNullLogger()
	numDocs := 200_000
	schema := &columnar.Schema{
		Columns: []columnar.Column{
			{Name: "score", Type: columnar.ColumnTypeFloat32},
		},
	}

	bucket, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
		WithUseBloomFilter(false),
		WithCalcCountNetAdditions(false),
	)
	require.NoError(b, err)
	b.Cleanup(func() { bucket.Shutdown(context.Background()) })

	for i := 0; i < numDocs; i++ {
		require.NoError(b, bucket.ColumnarPutFloat32(uint64(i), 0, float32(i)))
	}
	require.NoError(b, bucket.FlushMemtable())

	rng := rand.New(rand.NewSource(7))
	docIDs := make([]uint64, 10_000)
	for i := range docIDs {
		docIDs[i] = uint64(rng.Intn(numDocs))
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(4) // reading 4 bytes (float32) per op

	for i := 0; i < b.N; i++ {
		docID := docIDs[i%len(docIDs)]
		v, ok := bucket.ColumnarLookupFloat32(docID, 0)
		if !ok {
			b.Fatal("not found")
		}
		_ = v
	}
}

func BenchmarkReplace_LargeSegmentRead(b *testing.B) {
	dir, err := os.MkdirTemp("", "bench-rep-large-*")
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { os.RemoveAll(dir) })

	logger, _ := test.NewNullLogger()
	numDocs := 200_000

	bucket, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil,
		cyclemanager.NewCallbackGroupNoop(),
		cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
	)
	require.NoError(b, err)
	b.Cleanup(func() { bucket.Shutdown(context.Background()) })

	for i := 0; i < numDocs; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		val := make([]byte, 4)
		binary.LittleEndian.PutUint32(val, math.Float32bits(float32(i)))
		require.NoError(b, bucket.Put(key, val))
	}
	require.NoError(b, bucket.FlushMemtable())

	rng := rand.New(rand.NewSource(7))
	docIDs := make([]uint64, 10_000)
	for i := range docIDs {
		docIDs[i] = uint64(rng.Intn(numDocs))
	}

	key := make([]byte, 8)
	b.ResetTimer()
	b.ReportAllocs()
	b.SetBytes(4)

	for i := 0; i < b.N; i++ {
		docID := docIDs[i%len(docIDs)]
		binary.BigEndian.PutUint64(key, docID)
		v, err := bucket.Get(key)
		if err != nil {
			b.Fatal(err)
		}
		_ = v
	}
}

// BenchmarkColumnar_Write measures write throughput into the memtable.
func BenchmarkColumnar_Write(b *testing.B) {
	dir := b.TempDir()
	logger, _ := test.NewNullLogger()
	schema := &columnar.Schema{
		Columns: []columnar.Column{
			{Name: "score", Type: columnar.ColumnTypeFloat32},
			{Name: "ts", Type: columnar.ColumnTypeInt64},
		},
	}

	bucket, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
		WithUseBloomFilter(false),
		WithCalcCountNetAdditions(false),
	)
	require.NoError(b, err)
	b.Cleanup(func() { bucket.Shutdown(context.Background()) })

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docID := uint64(i)
		_ = bucket.ColumnarPutFloat32(docID, 0, float32(i)*0.01)
		_ = bucket.ColumnarPutInt64(docID, 1, int64(i)*1000)
	}
}

// BenchmarkColumnar_WriteRow measures write throughput using the batch PutRow API
// (single lock + single WAL entry per doc).
func BenchmarkColumnar_WriteRow(b *testing.B) {
	dir := b.TempDir()
	logger, _ := test.NewNullLogger()
	schema := &columnar.Schema{
		Columns: []columnar.Column{
			{Name: "score", Type: columnar.ColumnTypeFloat32},
			{Name: "ts", Type: columnar.ColumnTypeInt64},
		},
	}

	bucket, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyColumnar),
		WithColumnarSchema(schema),
		WithUseBloomFilter(false),
		WithCalcCountNetAdditions(false),
	)
	require.NoError(b, err)
	b.Cleanup(func() { bucket.Shutdown(context.Background()) })

	row := make([]byte, schema.RowWidth()) // 12 bytes: float32 + int64

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		docID := uint64(i)
		binary.LittleEndian.PutUint32(row[0:], math.Float32bits(float32(i)*0.01))
		binary.LittleEndian.PutUint64(row[4:], uint64(i)*1000)
		_ = bucket.ColumnarPutRow(docID, row)
	}
}

// BenchmarkReplace_Write measures equivalent write throughput for replace.
func BenchmarkReplace_Write(b *testing.B) {
	dir := b.TempDir()
	logger, _ := test.NewNullLogger()

	bucket, err := NewBucketCreator().NewBucket(context.Background(), dir, dir, logger,
		nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyReplace),
	)
	require.NoError(b, err)
	b.Cleanup(func() { bucket.Shutdown(context.Background()) })

	key := make([]byte, 8)
	val := make([]byte, 12)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.LittleEndian.PutUint32(val[0:], math.Float32bits(float32(i)*0.01))
		binary.LittleEndian.PutUint64(val[4:], uint64(i)*1000)
		_ = bucket.Put(key, val)
	}
}
