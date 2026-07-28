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
	"strings"
	"sync"
	"testing"

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

// prefillBenchConfig shapes the slow-prefill dataset: named target vectors, a
// properties payload dominating value size, multiple flushed segments.
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

// putBenchObject mirrors the real objects-bucket write path (16-byte primary key,
// little-endian docID secondary key at index 0). keyID and docID differ only for
// rewrites, which model an update landing in a newer segment.
func putBenchObject(tb testing.TB, bucket *lsmkv.Bucket, keyID, docID uint64, dims int, payload string) {
	tb.Helper()
	obj := storobj.New(docID)
	obj.Object = models.Object{
		ID:         strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012x", keyID)),
		Class:      "Bench",
		Properties: map[string]interface{}{"filler": payload},
	}
	obj.Vectors = map[string][]float32{
		prefillBenchTarget: benchVector(int(docID), dims, 0),
		"sibling":          benchVector(int(docID), dims, 13),
	}
	data, err := obj.MarshalBinary()
	require.NoError(tb, err)

	docIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(docIDBytes, docID)
	require.NoError(tb, bucket.Put(keyForDocID(keyID), data,
		lsmkv.WithSecondaryKey(helpers.ObjectsBucketLSMDocIDSecondaryIndex, docIDBytes)))
}

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
		putBenchObject(tb, bucket, uint64(i), uint64(i), cfg.dims, payload)
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
// the full object plus a named-vector decode.
func runSerialPrefill(tb testing.TB, store *lsmkv.Store, cfg prefillBenchConfig) {
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

	require.NoError(tb, newVectorCachePrefiller(c, h, logger).Prefill(context.Background(), int(c.CopyMaxSize())))
	require.Equal(tb, int64(cfg.n), c.CountVectors())
}

func runParallelPrefill(tb testing.TB, store *lsmkv.Store, cfg prefillBenchConfig) {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000_000, 1, logger, false, 0, nil)
	c.Grow(uint64(cfg.n))
	h := newPrefillBenchIndex(store, c, cfg.n)

	require.NoError(tb, h.prefillCacheParallel(context.Background()))
	require.Equal(tb, int64(cfg.n), c.CountVectors())
}

func runTargetedPrefill(tb testing.TB, store *lsmkv.Store, cfg prefillBenchConfig) {
	tb.Helper()
	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000_000, 1, logger, false, 0, nil)
	c.Grow(uint64(cfg.n))
	h := newPrefillBenchIndex(store, c, cfg.n)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	require.NoError(tb, h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
		return h.scanObjectVectorsTargeted(ctx, bucket, prefillBenchTarget, onVector)
	}))
	require.Equal(tb, int64(cfg.n), c.CountVectors())
}

func benchRuns(b *testing.B, vectors int, run func(tb testing.TB)) {
	for i := 0; i < b.N; i++ {
		run(b)
	}
	b.ReportMetric(float64(vectors)*float64(b.N)/b.Elapsed().Seconds(), "vectors/s")
}

// BenchmarkPrefillNamedVectorsLargeProps is the read-amplification case the targeted
// scan exists for: the properties schema dominates the value, so peek+jump reads a
// small fraction of the bucket while both full-scan variants read all of it.
// Page-cache hot, so it measures syscalls/copies and decode, not disk latency.
func BenchmarkPrefillNamedVectorsLargeProps(b *testing.B) {
	cfg := prefillBenchConfig{n: 5_000, dims: 128, payloadBytes: 16 << 10, segments: 4}
	store := buildPrefillBenchStore(b, cfg)

	b.Run("serial-by-id", func(b *testing.B) {
		benchRuns(b, cfg.n, func(tb testing.TB) { runSerialPrefill(tb, store, cfg) })
	})
	b.Run("parallel-scan", func(b *testing.B) {
		benchRuns(b, cfg.n, func(tb testing.TB) { runParallelPrefill(tb, store, cfg) })
	})
	b.Run("targeted-scan", func(b *testing.B) {
		benchRuns(b, cfg.n, func(tb testing.TB) { runTargetedPrefill(tb, store, cfg) })
	})

	preadStore := buildPrefillBenchStore(b, cfg, lsmkv.WithPread(true), lsmkv.WithMinMMapSize(0))
	b.Run("pread/parallel-scan", func(b *testing.B) {
		benchRuns(b, cfg.n, func(tb testing.TB) { runParallelPrefill(tb, preadStore, cfg) })
	})
	b.Run("pread/targeted-scan", func(b *testing.B) {
		benchRuns(b, cfg.n, func(tb testing.TB) { runTargetedPrefill(tb, preadStore, cfg) })
	})
}

// BenchmarkPrefillNamedVectorsChurned: 40% of keys rewritten under fresh doc ids
// after the base segments flushed, leaving stale rows in older segments. The
// newest-wins scan hides them via in-memory key probes before any value read; the
// merged cursor copies their full values into the merge before discarding them.
func BenchmarkPrefillNamedVectorsChurned(b *testing.B) {
	cfg := prefillBenchConfig{n: 5_000, dims: 128, payloadBytes: 16 << 10, segments: 3}
	const rewritten = 2_000

	store := buildPrefillBenchStore(b, cfg, lsmkv.WithPread(true), lsmkv.WithMinMMapSize(0))
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	payload := strings.Repeat("x", cfg.payloadBytes)
	for i := 0; i < rewritten; i++ {
		putBenchObject(b, bucket, uint64(i), uint64(cfg.n+i), cfg.dims, payload)
	}
	require.NoError(b, bucket.FlushAndSwitch())

	// live: unmodified originals plus the rewrites; doc ids 0..rewritten-1 are dead
	total := cfg.n + rewritten
	run := func(tb testing.TB, scan func(h *hnsw) error) {
		logger, _ := test.NewNullLogger()
		c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000_000, 1, logger, false, 0, nil)
		c.Grow(uint64(total))
		h := newPrefillBenchIndex(store, c, total)
		for i := 0; i < rewritten; i++ {
			h.nodes[i] = nil
		}
		require.NoError(tb, scan(h))
		require.Equal(tb, int64(cfg.n), c.CountVectors())
	}

	b.Run("pread/parallel-scan", func(b *testing.B) {
		benchRuns(b, cfg.n, func(tb testing.TB) {
			run(tb, func(h *hnsw) error {
				return h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
					return scanBucketVectorsParallel(ctx, bucket, objectsRowDecoder(prefillBenchTarget, h.logger), onVector, h.logger)
				})
			})
		})
	})
	b.Run("pread/targeted-scan", func(b *testing.B) {
		benchRuns(b, cfg.n, func(tb testing.TB) {
			run(tb, func(h *hnsw) error {
				return h.prefillFromScan(context.Background(), func(ctx context.Context, onVector prefillOnVector) error {
					return h.scanObjectVectorsTargeted(ctx, bucket, prefillBenchTarget, onVector)
				})
			})
		})
	})
}
