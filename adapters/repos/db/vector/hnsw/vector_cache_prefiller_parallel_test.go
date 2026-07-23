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
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
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
		lsmkv.WithStrategy(lsmkv.StrategyReplace),
		lsmkv.WithCalcCountNetAdditions(true))) // like the real objects bucket
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

// scanObjectVectorsParallel is the objects-bucket specialization of the generic scan,
// kept as a helper so the scan tests read at the domain level.
func scanObjectVectorsParallel(ctx context.Context, bucket *lsmkv.Bucket, targetVector string,
	onVector prefillOnVector, logger logrus.FieldLogger,
) error {
	return scanBucketVectorsParallel(ctx, bucket, objectsRowDecoder(targetVector, logger), onVector, logger)
}

func collectScan(t *testing.T, bucket *lsmkv.Bucket, target string) map[uint64][]float32 {
	t.Helper()
	logger, _ := test.NewNullLogger()
	var mu sync.Mutex
	got := map[uint64][]float32{}
	err := scanObjectVectorsParallel(context.Background(), bucket, target,
		func(id uint64, vec []float32) error {
			mu.Lock()
			defer mu.Unlock()
			_, exists := got[id]
			require.Falsef(t, exists, "doc id %d emitted more than once", id)
			got[id] = vec
			return nil
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
		err := scanObjectVectorsParallel(ctx, bucket, "", func(uint64, []float32) error { return nil }, logger)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestParallelPrefillEligible(t *testing.T) {
	base := parallelPrefillInputs{
		multivector:  false,
		muvera:       false,
		cacheMaxSize: 1e12,
		nodeCount:    1000,
	}

	tests := []struct {
		name string
		mod  func(*parallelPrefillInputs)
		want bool
	}{
		{"unbounded single-vector is eligible", func(*parallelPrefillInputs) {}, true},
		{"true multivector keeps serial path", func(in *parallelPrefillInputs) { in.multivector = true; in.muvera = false }, false},
		// muvera's float32 cache is sourced from the _muvera_vectors bucket, not the
		// objects bucket this scan reads — it takes the muvera scan instead.
		{"muvera does not take the objects scan", func(in *parallelPrefillInputs) { in.multivector = true; in.muvera = true }, false},
		{"muvera without multivector flag does not take the objects scan", func(in *parallelPrefillInputs) { in.multivector = false; in.muvera = true }, false},
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

func TestMuveraParallelPrefillEligible(t *testing.T) {
	base := parallelPrefillInputs{
		multivector:  true,
		muvera:       true,
		cacheMaxSize: 1e12,
		nodeCount:    1000,
	}

	tests := []struct {
		name string
		mod  func(*parallelPrefillInputs)
		want bool
	}{
		{"unbounded muvera is eligible", func(*parallelPrefillInputs) {}, true},
		{"non-muvera is not", func(in *parallelPrefillInputs) { in.muvera = false }, false},
		{"single-vector is not", func(in *parallelPrefillInputs) { in.multivector = false; in.muvera = false }, false},
		{"bounded cache keeps serial path", func(in *parallelPrefillInputs) { in.cacheMaxSize = 500; in.nodeCount = 1000 }, false},
		{"cache exactly fits nodes is eligible", func(in *parallelPrefillInputs) { in.cacheMaxSize = 1000; in.nodeCount = 1000 }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := base
			tt.mod(&in)
			assert.Equal(t, tt.want, muveraParallelPrefillEligible(in))
		})
	}
}

// prefillParallelIntoCache fills an objects bucket with vecs (a legacy vector per id),
// then runs prefillCacheParallel against a real cache wired so any miss errors. h.nodes
// is sized to preGrown (and the cache grown to match when preGrown>0); preGrown==0
// forces the defensive in-scan Grow path. Returns the populated cache.
func prefillParallelIntoCache(t *testing.T, vecs map[uint64][]float32, preGrown int,
	dp distancer.Provider, normalizeOnRead bool,
) cache.Cache[float32] {
	t.Helper()
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for id, v := range vecs {
		putTestObject(t, bucket, id, v, nil)
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d: prefill should have loaded it", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, normalizeOnRead, 0, nil)
	if preGrown > 0 {
		c.Grow(uint64(preGrown)) // mimic the restore-time pre-grow
	}
	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             make([]*vertex, preGrown),
		id:                "main", // no "vectors_" prefix => legacy default target vector
		logger:            logger,
		distancerProvider: dp,
	}
	require.NoError(t, h.prefillCacheParallel(context.Background()))
	return c
}

func requireCacheContains(t *testing.T, c cache.Cache[float32], want map[uint64][]float32) {
	t.Helper()
	require.Equal(t, int64(len(want)), c.CountVectors())
	for id, w := range want {
		got, err := c.Get(context.Background(), id)
		require.NoErrorf(t, err, "doc id %d should be a cache hit", id)
		require.Equalf(t, w, got, "vector mismatch for doc id %d", id)
	}
}

// TestPrefillCacheParallelEndToEnd runs the full prefill against a real bucket and
// cache. VectorForID errors, so any vector the prefill missed surfaces as a Get error.
func TestPrefillCacheParallelEndToEnd(t *testing.T) {
	const n = 500
	exp := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		exp[i] = []float32{float32(i), float32(i) * 0.5, float32(i) + 7}
	}
	c := prefillParallelIntoCache(t, exp, n, distancer.NewDotProductProvider(), false)
	requireCacheContains(t, c, exp)
}

// TestPrefillCacheParallelSkipsBeyondNodeRange: ids at or beyond the restored node
// range are not preloaded — live inserts self-preload, and a corrupt key must not
// size the cache.
func TestPrefillCacheParallelSkipsBeyondNodeRange(t *testing.T) {
	const n = 100
	vecs := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		vecs[i] = []float32{float32(i), float32(i) + 1}
	}
	exp := map[uint64][]float32{}
	for i := uint64(0); i < 40; i++ {
		exp[i] = vecs[i]
	}
	c := prefillParallelIntoCache(t, vecs, 40, distancer.NewDotProductProvider(), false)
	requireCacheContains(t, c, exp)
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

// TestPrefillCacheParallelDoesNotOverwriteNewerVectors is the PreloadIfAbsent
// invariant end-to-end: ids already in the cache (e.g. written by an insert racing an
// async prefill) keep their value, and count is not double-incremented for them.
func TestPrefillCacheParallelDoesNotOverwriteNewerVectors(t *testing.T) {
	const n = 100
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	exp := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		vec := []float32{float32(i), float32(i)}
		putTestObject(t, bucket, i, vec, nil)
		exp[i] = vec
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)

	inserted := map[uint64][]float32{3: {42, 42}, 7: {43, 43}}
	for id, vec := range inserted {
		c.Preload(id, vec)
		exp[id] = vec
	}

	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             make([]*vertex, n),
		id:                "main",
		logger:            logger,
		distancerProvider: distancer.NewDotProductProvider(),
	}
	require.NoError(t, h.prefillCacheParallel(context.Background()))

	requireCacheContains(t, c, exp)
}

type failingAllocChecker struct{}

func (failingAllocChecker) CheckAlloc(int64) error {
	return fmt.Errorf("out of memory")
}
func (failingAllocChecker) CheckMappingAndReserve(int64, int) error { return nil }
func (failingAllocChecker) Refresh(bool)                            {}

// TestPrefillCacheParallelAbortsUnderMemoryPressure: with a failing allocChecker the
// scan stops at the first probe and prefill degrades gracefully (nil error, partial
// cache); the memtable-only bucket forces a single scan range, making the abort point
// deterministic.
func TestPrefillCacheParallelAbortsUnderMemoryPressure(t *testing.T) {
	const n = prefillAllocCheckEvery + 500
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTestObject(t, bucket, i, []float32{float32(i)}, nil)
	}

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)

	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             make([]*vertex, n),
		id:                "main",
		logger:            logger,
		distancerProvider: distancer.NewDotProductProvider(),
		allocChecker:      failingAllocChecker{},
	}
	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(prefillAllocCheckEvery), c.CountVectors())
}

// TestPrefillCacheParallelStopsWhenCompressionActivates: once h.compressed flips, the
// uncompressed cache is no longer the live cache and the scan must stop loading into it.
func TestPrefillCacheParallelStopsWhenCompressionActivates(t *testing.T) {
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < 50; i++ {
		putTestObject(t, bucket, i, []float32{float32(i)}, nil)
	}

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(50)

	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             make([]*vertex, 50),
		id:                "main",
		logger:            logger,
		distancerProvider: distancer.NewDotProductProvider(),
	}
	h.compressed.Store(true)

	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(0), c.CountVectors())
}

func putMuveraVector(t *testing.T, bucket *lsmkv.Bucket, id uint64, vec []float32) {
	t.Helper()
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, id)
	require.NoError(t, bucket.Put(key, multivector.MuveraBytesFromFloat32(vec)))
}

// TestPrefillMuveraCacheParallelEndToEnd scans a real muvera vectors bucket (key =
// big-endian docID = node id, value = raw float32s) into the cache, across flushed
// segments and with a deleted entry that must not resurface.
func TestPrefillMuveraCacheParallelEndToEnd(t *testing.T) {
	const n = 300
	store := newTestObjectsStore(t)
	bucketName := "m_muvera_vectors"
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), bucketName,
		lsmkv.WithStrategy(lsmkv.StrategyReplace)))
	bucket := store.Bucket(bucketName)

	exp := make(map[uint64][]float32, n)
	for i := uint64(0); i < n; i++ {
		vec := []float32{float32(i) + 0.5, float32(i) - 0.5, float32(i)}
		putMuveraVector(t, bucket, i, vec)
		exp[i] = vec
	}
	require.NoError(t, bucket.FlushAndSwitch())

	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 42)
	require.NoError(t, bucket.Delete(key))
	require.NoError(t, bucket.FlushAndSwitch())
	delete(exp, 42)

	logger, _ := test.NewNullLogger()
	mustHit := func(_ context.Context, id uint64) ([]float32, error) {
		return nil, fmt.Errorf("unexpected cache miss for id %d", id)
	}
	c := cache.NewShardedFloat32LockCache(mustHit, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)

	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             make([]*vertex, n),
		id:                "m",
		logger:            logger,
		distancerProvider: distancer.NewDotProductProvider(),
	}
	require.NoError(t, h.prefillMuveraCacheParallel(context.Background()))
	requireCacheContains(t, c, exp)
}

// TestPrefillCacheParallelStopsWhenCacheFull: the scan must stop once count reaches
// maxSize instead of feeding replaceIfFull's full-cache wipe; memtable-only data
// forces a single scan range so the stop point is deterministic.
func TestPrefillCacheParallelStopsWhenCacheFull(t *testing.T) {
	const n = 50
	const maxSize = 10
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTestObject(t, bucket, i, []float32{float32(i)}, nil)
	}

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(nil, nil, maxSize, 1, logger, false, 0, nil)
	c.Grow(n)

	h := &hnsw{
		store:             store,
		cache:             c,
		nodes:             make([]*vertex, n),
		id:                "main",
		logger:            logger,
		distancerProvider: distancer.NewDotProductProvider(),
	}
	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(maxSize), c.CountVectors())
}

// TestUseParallelPrefillExcludesHFresh: the hfresh centroid index shares the shard's
// store (objects bucket present) but its cache holds centroid vectors, so the
// objects scan must never run for it.
func TestUseParallelPrefillExcludesHFresh(t *testing.T) {
	store := newTestObjectsStore(t)
	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)

	h := &hnsw{
		store:             store,
		cache:             c,
		id:                "main_centroids",
		logger:            logger,
		hfreshMode:        true,
		distancerProvider: distancer.NewDotProductProvider(),
	}
	require.False(t, h.useParallelPrefill())

	h.hfreshMode = false
	require.True(t, h.useParallelPrefill())
}
