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
	"sync"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
)

func newTestObjectsStore(t *testing.T) *lsmkv.Store {
	t.Helper()
	dir := t.TempDir()
	logger, _ := test.NewNullLogger()
	store, err := lsmkv.New(dir, dir, logger, nil, nil,
		cyclemanager.NewCallbackGroup("objects", logger, 1),
		cyclemanager.NewCallbackGroup("nonObjects", logger, 1),
		cyclemanager.NewCallbackGroupNoop())
	require.NoError(t, err)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	require.NoError(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace)))
	return store
}

func newTestObjectsBucket(t *testing.T) *lsmkv.Bucket {
	t.Helper()
	return newTestObjectsStore(t).Bucket(helpers.ObjectsBucketLSM)
}

// putTestObject stores an object marshalled exactly as the write path does, so the
// scan reads real on-disk data rather than a hand-rolled encoding.
func putTestObject(t *testing.T, bucket *lsmkv.Bucket, docID uint64, legacyVec []float32, named map[string][]float32) {
	t.Helper()
	id := strfmt.UUID(fmt.Sprintf("00000000-0000-4000-8000-%012x", docID))
	obj := storobj.New(docID)
	obj.Object = models.Object{ID: id, Class: "Test"}
	obj.Vector = legacyVec
	if named != nil {
		obj.Vectors = named
	}
	data, err := obj.MarshalBinary()
	require.NoError(t, err)

	require.NoError(t, bucket.Put(keyForDocID(docID), data))
}

// keyForDocID builds a unique, sortable bucket key. The scan reads docID + vector from
// the value, not the key, so a 16-byte big-endian docID stands in for the real UUID key.
func keyForDocID(docID uint64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[8:], docID)
	return key
}

func collectScan(t *testing.T, bucket *lsmkv.Bucket, target string) map[uint64][]float32 {
	t.Helper()
	logger, _ := test.NewNullLogger()
	var mu sync.Mutex
	got := map[uint64][]float32{}
	err := scanObjectVectorsParallel(context.Background(), bucket, target,
		func(id uint64, vec []float32) {
			mu.Lock()
			defer mu.Unlock()
			_, exists := got[id]
			require.Falsef(t, exists, "doc id %d emitted more than once", id)
			got[id] = vec
		}, logger)
	require.NoError(t, err)
	return got
}

func assertVectorsEqual(t *testing.T, exp, got map[uint64][]float32) {
	t.Helper()
	require.Equal(t, len(exp), len(got), "vector count mismatch")
	for id, ev := range exp {
		gv, ok := got[id]
		require.Truef(t, ok, "missing doc id %d", id)
		require.Equalf(t, ev, gv, "vector mismatch for doc id %d", id)
	}
}

func TestScanObjectVectorsParallel(t *testing.T) {
	t.Run("legacy single vector, memtable only", func(t *testing.T) {
		bucket := newTestObjectsBucket(t)
		exp := map[uint64][]float32{}
		for i := uint64(0); i < 50; i++ {
			vec := []float32{float32(i), float32(i) + 0.5, float32(i) * 2}
			putTestObject(t, bucket, i, vec, nil)
			exp[i] = vec
		}
		assertVectorsEqual(t, exp, collectScan(t, bucket, ""))
	})

	t.Run("legacy, flushed to segment (exercises parallel ranges)", func(t *testing.T) {
		bucket := newTestObjectsBucket(t)
		exp := map[uint64][]float32{}
		for i := uint64(0); i < 3000; i++ {
			vec := []float32{float32(i), float32(-int64(i))}
			putTestObject(t, bucket, i, vec, nil)
			exp[i] = vec
		}
		require.NoError(t, bucket.FlushAndSwitch())
		assertVectorsEqual(t, exp, collectScan(t, bucket, ""))
	})

	t.Run("named target vector", func(t *testing.T) {
		bucket := newTestObjectsBucket(t)
		exp := map[uint64][]float32{}
		for i := uint64(0); i < 60; i++ {
			vec := []float32{float32(i) + 0.25, float32(i) - 0.25}
			putTestObject(t, bucket, i, nil, map[string][]float32{"custom": vec})
			exp[i] = vec
		}
		assertVectorsEqual(t, exp, collectScan(t, bucket, "custom"))
	})

	t.Run("objects without the target vector are skipped", func(t *testing.T) {
		bucket := newTestObjectsBucket(t)
		exp := map[uint64][]float32{}
		for i := uint64(0); i < 40; i++ {
			if i%2 == 0 {
				vec := []float32{float32(i)}
				putTestObject(t, bucket, i, nil, map[string][]float32{"custom": vec})
				exp[i] = vec
			} else {
				putTestObject(t, bucket, i, nil, map[string][]float32{"other": {1, 2, 3}})
			}
		}
		assertVectorsEqual(t, exp, collectScan(t, bucket, "custom"))
	})

	t.Run("empty bucket", func(t *testing.T) {
		bucket := newTestObjectsBucket(t)
		assert.Empty(t, collectScan(t, bucket, ""))
	})

	t.Run("context cancelled before scan returns error", func(t *testing.T) {
		bucket := newTestObjectsBucket(t)
		for i := uint64(0); i < 3000; i++ {
			putTestObject(t, bucket, i, []float32{float32(i)}, nil)
		}
		require.NoError(t, bucket.FlushAndSwitch())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		logger, _ := test.NewNullLogger()
		err := scanObjectVectorsParallel(ctx, bucket, "", func(uint64, []float32) {}, logger)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestParallelPrefillEligible(t *testing.T) {
	base := parallelPrefillInputs{
		waitForPrefill: true,
		multivector:    false,
		muvera:         false,
		cacheMaxSize:   1e12,
		nodeCount:      1000,
	}

	tests := []struct {
		name string
		mod  func(*parallelPrefillInputs)
		want bool
	}{
		{"sync + unbounded + single-vector", func(*parallelPrefillInputs) {}, true},
		{"async prefill keeps serial path", func(in *parallelPrefillInputs) { in.waitForPrefill = false }, false},
		{"true multivector keeps serial path", func(in *parallelPrefillInputs) { in.multivector = true; in.muvera = false }, false},
		// muvera's float32 cache is sourced from the _muvera_vectors bucket, not the
		// objects bucket the parallel scan reads, so it must stay on the serial path —
		// regardless of whether the multivector flag happens to be set alongside it.
		{"muvera keeps serial path", func(in *parallelPrefillInputs) { in.multivector = true; in.muvera = true }, false},
		{"muvera without multivector flag keeps serial path", func(in *parallelPrefillInputs) { in.multivector = false; in.muvera = true }, false},
		{"bounded cache (max < nodes) keeps serial path", func(in *parallelPrefillInputs) { in.cacheMaxSize = 500; in.nodeCount = 1000 }, false},
		{"cache exactly fits nodes is eligible", func(in *parallelPrefillInputs) { in.cacheMaxSize = 1000; in.nodeCount = 1000 }, true},
		{"empty index (0 nodes) is eligible", func(in *parallelPrefillInputs) { in.nodeCount = 0 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := base
			tt.mod(&in)
			assert.Equal(t, tt.want, parallelPrefillEligible(in))
		})
	}
}

// TestPrefillCacheParallelEndToEnd runs the full prefill against a real bucket and
// cache. VectorForID errors, so any vector the prefill missed surfaces as a Get error.
func TestPrefillCacheParallelEndToEnd(t *testing.T) {
	const n = 500

	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	exp := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		vec := []float32{float32(i), float32(i) * 0.5, float32(i) + 7}
		putTestObject(t, bucket, i, vec, nil)
		exp[i] = vec
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d: prefill should have loaded it", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(uint64(n)) // mimic the restore-time pre-grow

	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             make([]*vertex, n),
		id:                "main", // no "vectors_" prefix => legacy default target vector
		logger:            logger,
		distancerProvider: distancer.NewDotProductProvider(), // non-cosine: no normalization
	}

	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(n), c.CountVectors())

	for i := uint64(0); i < n; i++ {
		got, err := h.cache.Get(context.Background(), i)
		require.NoErrorf(t, err, "doc id %d should be a cache hit", i)
		require.Equalf(t, exp[i], got, "vector mismatch for doc id %d", i)
	}
}

// TestPrefillCacheParallelGrowsBeyondPreGrown exercises the defensive Grow path: with
// preGrown == 0 every id triggers cache.Grow from concurrent scanners, swapping the
// cache slice under load. Run with -race to catch a missing lock on the grow.
func TestPrefillCacheParallelGrowsBeyondPreGrown(t *testing.T) {
	const n = 2000

	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	exp := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		vec := []float32{float32(i), float32(i) + 1}
		putTestObject(t, bucket, i, vec, nil)
		exp[i] = vec
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)

	// Not pre-grown: preGrown == 0, so every id takes the defensive Grow path.
	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             nil,
		id:                "main",
		logger:            logger,
		distancerProvider: distancer.NewDotProductProvider(),
	}

	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(n), c.CountVectors())
	for i := uint64(0); i < n; i++ {
		got, err := h.cache.Get(context.Background(), i)
		require.NoErrorf(t, err, "doc id %d", i)
		require.Equalf(t, exp[i], got, "doc id %d", i)
	}
}

// TestScanObjectVectorsParallelLatestWinsAcrossSegments verifies that when the same
// key is written in two segments, the cursor yields the latest value exactly once.
func TestScanObjectVectorsParallelLatestWinsAcrossSegments(t *testing.T) {
	bucket := newTestObjectsBucket(t)

	putTestObject(t, bucket, 7, []float32{1, 1}, nil)
	require.NoError(t, bucket.FlushAndSwitch())
	putTestObject(t, bucket, 7, []float32{2, 2}, nil) // same key, newer segment
	require.NoError(t, bucket.FlushAndSwitch())

	got := collectScan(t, bucket, "")
	require.Len(t, got, 1)
	require.Equal(t, []float32{2, 2}, got[7])
}

// TestScanObjectVectorsParallelSkipsDeleted verifies tombstoned objects are skipped,
// matching the serial path (which never sees a deleted doc id).
func TestScanObjectVectorsParallelSkipsDeleted(t *testing.T) {
	bucket := newTestObjectsBucket(t)

	putTestObject(t, bucket, 1, []float32{1}, nil)
	putTestObject(t, bucket, 2, []float32{2}, nil)
	putTestObject(t, bucket, 3, []float32{3}, nil)
	require.NoError(t, bucket.FlushAndSwitch())

	require.NoError(t, bucket.Delete(keyForDocID(2)))
	require.NoError(t, bucket.FlushAndSwitch())

	assertVectorsEqual(t, map[uint64][]float32{
		1: {1},
		3: {3},
	}, collectScan(t, bucket, ""))
}

// TestScanObjectVectorsParallelNamedVectorIsolation puts objects carrying several
// named vectors (plus a legacy vector) in one bucket and scans each target
// separately. Each named index shares the objects bucket with its siblings, so a
// scan for one target must yield only that target's vectors and skip objects that
// lack it — never bleed a sibling's vector or a legacy vector into the wrong cache.
func TestScanObjectVectorsParallelNamedVectorIsolation(t *testing.T) {
	bucket := newTestObjectsBucket(t)

	// Deliberately sparse: not every object has every target.
	putTestObject(t, bucket, 0, []float32{0, 0}, map[string][]float32{"title": {1, 0}, "body": {2, 0}})
	putTestObject(t, bucket, 1, nil, map[string][]float32{"title": {1, 1}})
	putTestObject(t, bucket, 2, nil, map[string][]float32{"body": {2, 2}})
	putTestObject(t, bucket, 3, []float32{3, 3}, nil)
	putTestObject(t, bucket, 4, nil, map[string][]float32{"title": {1, 4}, "body": {2, 4}})

	// legacy target: only objects with a legacy vector.
	assertVectorsEqual(t, map[uint64][]float32{
		0: {0, 0},
		3: {3, 3},
	}, collectScan(t, bucket, ""))

	assertVectorsEqual(t, map[uint64][]float32{
		0: {1, 0},
		1: {1, 1},
		4: {1, 4},
	}, collectScan(t, bucket, "title"))

	assertVectorsEqual(t, map[uint64][]float32{
		0: {2, 0},
		2: {2, 2},
		4: {2, 4},
	}, collectScan(t, bucket, "body"))
}

// TestPrefillCacheParallelNormalizesForCosine guards the normalization invariant:
// for a cosine-dot index the cache must hold normalized vectors (so the dot product
// equals cosine similarity). The objects bucket stores raw vectors, and the serial
// prefiller normalizes them via the cache's normalizeOnRead wrapper — the parallel
// path must match, or cosine search silently returns wrong distances.
func TestPrefillCacheParallelNormalizesForCosine(t *testing.T) {
	const n = 50
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)

	raw := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		vec := []float32{float32(i) + 1, float32(i) + 2, float32(i) + 3} // non-unit on purpose
		putTestObject(t, bucket, i, vec, nil)
		raw[i] = vec
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	// normalizeOnRead=true mirrors how index.New builds the cache for cosine-dot.
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, true, 0, nil)
	c.Grow(uint64(n))

	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             make([]*vertex, n),
		id:                "main",
		logger:            logger,
		distancerProvider: distancer.NewCosineDistanceProvider(),
	}

	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(n), c.CountVectors())
	for i := uint64(0); i < n; i++ {
		got, err := c.Get(context.Background(), i)
		require.NoError(t, err)
		require.Equalf(t, distancer.Normalize(raw[i]), got,
			"cosine-dot cache must hold normalized vectors for doc %d", i)
	}
}
