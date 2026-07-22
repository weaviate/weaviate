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

package hnsw

import (
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

// prefillBenchConfig shapes a dataset that reproduces the slow prefill case: named
// target vectors (decode must chain-skip className/props/meta/vectorWeights and a
// msgpack offsets map per object), a properties payload dominating value size (the
// read-amplification source), and multiple flushed segments.
type prefillBenchConfig struct {
	n            int
	dims         int
	payloadBytes int
	segments     int
}

const prefillBenchTarget = "custom"

func benchVector(i, dims, salt int) []float32 {
	vec := make([]float32, dims)
	for j := range vec {
		vec[j] = float32((i+salt)%977) + float32(j)*0.25
	}
	return vec
}

// buildPrefillBenchStore writes cfg.n objects with two named vectors and a filler
// property across cfg.segments flushed segments, mirroring the real write path
// (16-byte primary key, little-endian docID secondary key at index 0).
func buildPrefillBenchStore(tb testing.TB, cfg prefillBenchConfig, opts ...lsmkv.BucketOption) *lsmkv.Store {
	tb.Helper()
	dir := tb.TempDir()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroup("objects", logger, 1),
		cyclemanager.NewCallbackGroup("nonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(tb, err)
	tb.Cleanup(func() { store.Shutdown(context.Background()) })

	require.NoError(tb, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		append([]lsmkv.BucketOption{
			lsmkv.WithStrategy(lsmkv.StrategyReplace),
			lsmkv.WithSecondaryIndices(1),
		}, opts...)...))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	payload := strings.Repeat("x", cfg.payloadBytes)
	perSegment := (cfg.n + cfg.segments - 1) / cfg.segments
	for i := 0; i < cfg.n; i++ {
		docID := uint64(i)
		obj := storobj.New(docID)
		obj.Object = models.Object{
			ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012x", docID)),
			Class:      "Bench",
			Properties: map[string]interface{}{"filler": payload},
		}
		obj.Vectors = map[string][]float32{
			prefillBenchTarget: benchVector(i, cfg.dims, 0),
			"sibling":          benchVector(i, cfg.dims, 13),
		}
		data, err := obj.MarshalBinary()
		require.NoError(tb, err)

		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		require.NoError(tb, bucket.Put(keyForDocID(docID), data,
			lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))

		if (i+1)%perSegment == 0 {
			require.NoError(tb, bucket.FlushAndSwitch())
		}
	}
	require.NoError(tb, bucket.FlushAndSwitch())
	return store
}

func newPrefillBenchIndex(store *lsmkv.Store, c cache.Cache[float32], n int) *hnsw {
	logger, _ := test.NewNullLogger()
	nodes := make([]*vertex, n)
	for i := range nodes {
		nodes[i] = &vertex{level: 0}
	}
	return &hnsw{
		store:               store,
		cache:               c,
		nodes:               nodes,
		id:                  helpers.VectorsBucketLSM + "_" + prefillBenchTarget,
		logger:              logger,
		distancerProvider:   distancer.NewDotProductProvider(),
		shardedNodeLocks:    common.NewDefaultShardedRWLocks(),
		tombstoneLock:       &sync.RWMutex{},
		tombstones:          map[uint64]struct{}{},
		currentMaximumLayer: 0,
	}
}

// runSerialPrefill measures the by-id path: per node one secondary-index lookup of
// the full object plus a named-vector decode — the pre-#11838 behavior every path
// outside the parallel gates still uses.
func runSerialPrefill(tb testing.TB, store *lsmkv.Store, cfg prefillBenchConfig) time.Duration {
	tb.Helper()
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	logger, _ := test.NewNullLogger()

	byID := func(ctx context.Context, id uint64) ([]float32, error) {
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, id)
		v, err := bucket.GetBySecondary(ctx, helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)
		if err != nil {
			return nil, err
		}
		if len(v) == 0 {
			return nil, fmt.Errorf("doc id %d not found", id)
		}
		return storobj.VectorFromBinary(v, nil, prefillBenchTarget)
	}
	c := cache.NewShardedFloat32LockCache(byID, nil, 1_000_000_000, 1, logger, false, 0, nil)
	c.Grow(uint64(cfg.n))
	h := newPrefillBenchIndex(store, c, cfg.n)

	start := time.Now()
	err := newVectorCachePrefiller(c, h, logger).Prefill(context.Background(), int(c.CopyMaxSize()))
	took := time.Since(start)

	require.NoError(tb, err)
	require.Equal(tb, int64(cfg.n), c.CountVectors())
	return took
}

func runParallelPrefill(tb testing.TB, store *lsmkv.Store, cfg prefillBenchConfig) time.Duration {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000_000, 1, logger, false, 0, nil)
	c.Grow(uint64(cfg.n))
	h := newPrefillBenchIndex(store, c, cfg.n)

	start := time.Now()
	err := h.prefillCacheParallel(context.Background())
	took := time.Since(start)

	require.NoError(tb, err)
	require.Equal(tb, int64(cfg.n), c.CountVectors())
	return took
}

// BenchmarkPrefillNamedVectorsMultiSegment compares full-cache prefill time, serial
// by-id vs parallel scan, on multi-segment named-vector data. Page-cache hot, so it
// measures lookup/decode cost and parallelism, not disk latency — the on-disk win is
// larger (see #11837).
func BenchmarkPrefillNamedVectorsMultiSegment(b *testing.B) {
	cfg := prefillBenchConfig{n: 20_000, dims: 128, payloadBytes: 1024, segments: 4}
	store := buildPrefillBenchStore(b, cfg)

	b.Run("serial-by-id", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runSerialPrefill(b, store, cfg)
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
	b.Run("parallel-scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runParallelPrefill(b, store, cfg)
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
}

// TestPrefillParallelOutperformsSerialNamedVectorsMultiSegment validates the win the
// parallel scan exists for, on the named-vector multi-segment shape. Guarded and
// wide-margin because relative-performance assertions are inherently noisy.
func TestPrefillParallelOutperformsSerialNamedVectorsMultiSegment(t *testing.T) {
	if testing.Short() {
		t.Skip("performance comparison skipped with -short")
	}
	if runtime.GOMAXPROCS(0) < 4 {
		t.Skip("performance comparison needs >= 4 CPUs")
	}

	cfg := prefillBenchConfig{n: 15_000, dims: 64, payloadBytes: 1024, segments: 4}
	store := buildPrefillBenchStore(t, cfg)

	// best-of-two damps scheduler and page-cache warm-up noise
	serial := min(runSerialPrefill(t, store, cfg), runSerialPrefill(t, store, cfg))
	parallel := min(runParallelPrefill(t, store, cfg), runParallelPrefill(t, store, cfg))

	speedup := float64(serial) / float64(parallel)
	t.Logf("serial=%v parallel=%v speedup=%.1fx (n=%d dims=%d payload=%dB segments=%d)",
		serial, parallel, speedup, cfg.n, cfg.dims, cfg.payloadBytes, cfg.segments)
	require.Greaterf(t, speedup, 1.5,
		"parallel scan prefill should clearly beat serial by-id (serial=%v parallel=%v)", serial, parallel)
}

func runTargetedPrefill(tb testing.TB, store *lsmkv.Store, cfg prefillBenchConfig) time.Duration {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000_000, 1, logger, false, 0, nil)
	c.Grow(uint64(cfg.n))
	h := newPrefillBenchIndex(store, c, cfg.n)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	start := time.Now()
	err := h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
		return h.scanObjectVectorsTargeted(ctx, bucket, prefillBenchTarget, onVector)
	})
	took := time.Since(start)

	require.NoError(tb, err)
	require.Equal(tb, int64(cfg.n), c.CountVectors())
	return took
}

func runTargetedAllVersionsPrefill(tb testing.TB, store *lsmkv.Store, cfg prefillBenchConfig) time.Duration {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000_000, 1, logger, false, 0, nil)
	c.Grow(uint64(cfg.n))
	h := newPrefillBenchIndex(store, c, cfg.n)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	start := time.Now()
	err := h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
		return h.scanObjectVectorsTargetedAllVersions(ctx, bucket, prefillBenchTarget, onVector)
	})
	took := time.Since(start)

	require.NoError(tb, err)
	require.Equal(tb, int64(cfg.n), c.CountVectors())
	return took
}

// BenchmarkPrefillNamedVectorsLargeProps is the read-amplification case the targeted
// scan exists for: the properties schema dominates the value, so peek+jump reads a
// small fraction of the bucket while both full-scan variants read all of it.
func BenchmarkPrefillNamedVectorsLargeProps(b *testing.B) {
	cfg := prefillBenchConfig{n: 5_000, dims: 128, payloadBytes: 16 << 10, segments: 4}
	store := buildPrefillBenchStore(b, cfg)

	b.Run("serial-by-id", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runSerialPrefill(b, store, cfg)
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
	b.Run("parallel-scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runParallelPrefill(b, store, cfg)
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
	b.Run("targeted-scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runTargetedPrefill(b, store, cfg)
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})

	preadStore := buildPrefillBenchStore(b, cfg, lsmkv.WithPread(true), lsmkv.WithMinMMapSize(0))
	b.Run("pread/parallel-scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runParallelPrefill(b, preadStore, cfg)
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
	b.Run("pread/targeted-scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runTargetedPrefill(b, preadStore, cfg)
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
	b.Run("pread/targeted-all-versions", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runTargetedAllVersionsPrefill(b, preadStore, cfg)
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
}

// BenchmarkPrefillNamedVectorsChurned models a pread bucket where 40% of the keys
// were rewritten (new doc ids) after the base segments flushed, leaving stale rows
// in older segments under live keys. The liveness-filter scan pays a peek read per
// stale row to learn its dead doc id; the newest-wins scan hides stale rows via
// in-memory key probes before any read; the merged cursor copies their full
// values into the merge before discarding them.
func BenchmarkPrefillNamedVectorsChurned(b *testing.B) {
	cfg := prefillBenchConfig{n: 5_000, dims: 128, payloadBytes: 16 << 10, segments: 3}
	const rewritten = 2_000

	store := buildPrefillBenchStore(b, cfg, lsmkv.WithPread(true), lsmkv.WithMinMMapSize(0))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	// rewrite keys 0..rewritten-1 under fresh doc ids into a fourth segment
	payload := strings.Repeat("x", cfg.payloadBytes)
	for i := 0; i < rewritten; i++ {
		docID := uint64(cfg.n + i)
		obj := storobj.New(docID)
		obj.Object = models.Object{
			ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012x", uint64(i))),
			Class:      "Bench",
			Properties: map[string]interface{}{"filler": payload},
		}
		obj.Vectors = map[string][]float32{
			prefillBenchTarget: benchVector(int(docID), cfg.dims, 0),
			"sibling":          benchVector(int(docID), cfg.dims, 13),
		}
		data, err := obj.MarshalBinary()
		require.NoError(b, err)
		docIDBytes := make([]byte, 8)
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		require.NoError(b, bucket.Put(keyForDocID(uint64(i)), data,
			lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
	}
	require.NoError(b, bucket.FlushAndSwitch())

	// live nodes: unmodified originals plus the rewrites; doc ids 0..rewritten-1
	// are dead
	total := cfg.n + rewritten
	newChurnedIndex := func(c cache.Cache[float32]) *hnsw {
		h := newPrefillBenchIndex(store, c, total)
		for i := 0; i < rewritten; i++ {
			h.nodes[i] = nil
		}
		return h
	}
	run := func(b *testing.B, scan func(h *hnsw, bucket *lsmkv.Bucket) error) {
		logger, _ := test.NewNullLogger()
		mustHit := func(_ context.Context, id uint64) ([]float32, error) {
			return nil, fmt.Errorf("unexpected cache miss for id %d", id)
		}
		c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000_000, 1, logger, false, 0, nil)
		c.Grow(uint64(total))
		h := newChurnedIndex(c)
		require.NoError(b, scan(h, bucket))
		require.Equal(b, int64(cfg.n), c.CountVectors())
	}

	b.Run("pread/parallel-scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			run(b, func(h *hnsw, bucket *lsmkv.Bucket) error {
				return h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
					return scanBucketVectorsParallel(ctx, bucket, objectsRowDecoder(prefillBenchTarget, h.logger), onVector, h.logger)
				})
			})
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
	b.Run("pread/targeted-scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			run(b, func(h *hnsw, bucket *lsmkv.Bucket) error {
				return h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
					return h.scanObjectVectorsTargeted(ctx, bucket, prefillBenchTarget, onVector)
				})
			})
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
	b.Run("pread/targeted-all-versions", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			run(b, func(h *hnsw, bucket *lsmkv.Bucket) error {
				return h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
					return h.scanObjectVectorsTargetedAllVersions(ctx, bucket, prefillBenchTarget, onVector)
				})
			})
		}
		b.ReportMetric(float64(cfg.n)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
	})
}
