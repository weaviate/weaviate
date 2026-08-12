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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
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

// newPrefillTestIndex builds the index a prefill scan needs: the sharded node locks
// and tombstone map prefillEligible consults on every row, and a live vertex per id.
// A nil vertex means "not indexed", which the scan deliberately skips — a fixture
// that leaves h.nodes zero-valued prefills nothing.
func newPrefillTestIndex(id string, store *lsmkv.Store, c cache.Cache[float32],
	nodeCount int, dp distancer.Provider, logger logrus.FieldLogger,
) *hnsw {
	nodes := make([]*vertex, nodeCount)
	for i := range nodes {
		nodes[i] = &vertex{level: 0}
	}
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	return &hnsw{
		store:             store,
		cache:             c,
		nodes:             nodes,
		id:                id,
		logger:            logger,
		distancerProvider: dp,
		shardedNodeLocks:  common.NewDefaultShardedRWLocks(),
		tombstoneLock:     &sync.RWMutex{},
		tombstones:        map[uint64]struct{}{},
		shutdownCtx:       shutdownCtx,
		shutdownCtxCancel: shutdownCancel,
	}
}

// mustIfAbsentPreloader: PreloadIfAbsent is not on Cache[T], so a wrapper standing in
// for a real cache has to carry the preloader separately to delegate to it.
func mustIfAbsentPreloader(t testing.TB, c cache.Cache[float32]) cache.IfAbsentPreloader[float32] {
	t.Helper()
	p, ok := c.(cache.IfAbsentPreloader[float32])
	require.True(t, ok, "test cache must implement IfAbsentPreloader")
	return p
}

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

// keyForDocID builds the bucket key the shard would write: the object's uuid in
// binary form, matching the uuid the marshalled row carries.
func keyForDocID(docID uint64) []byte {
	id, err := uuid.Parse(testObjectUUID(docID))
	if err != nil {
		panic(err)
	}
	key, err := id.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return key
}

// testObjectUUID is the uuid every test object helper assigns for a given id.
func testObjectUUID(id uint64) string {
	return fmt.Sprintf("00000000-0000-4000-8000-%012x", id)
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
		ifAbsent:     true,
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
		// the scan stops one short of maxSize, so an exact fit cannot reach the count
		// replaceIfFull wipes on; the serial prefiller it would otherwise fall to
		// caches nothing at all
		{"cache exactly fits nodes is eligible", func(in *parallelPrefillInputs) { in.cacheMaxSize = 1000; in.nodeCount = 1000 }, true},
		{"cache one slot larger than nodes is eligible", func(in *parallelPrefillInputs) { in.cacheMaxSize = 1001; in.nodeCount = 1000 }, true},
		{"empty index (0 nodes) is eligible", func(in *parallelPrefillInputs) { in.nodeCount = 0 }, true},
		// the scan runs alongside live writes, so an overwriting Preload is not enough:
		// without if-absent semantics it could clobber a newer inserted vector
		{"cache without PreloadIfAbsent keeps serial path", func(in *parallelPrefillInputs) { in.ifAbsent = false }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := base
			tt.mod(&in)
			assert.Equal(t, tt.want, parallelPrefillEligible(in))
		})
	}
}

// errOnCacheMiss is the VectorForID for prefill tests: every post-prefill Get must
// be a cache hit, so any miss fails the test.
func errOnCacheMiss(_ context.Context, id uint64) ([]float32, error) {
	return nil, fmt.Errorf("unexpected cache miss for id %d", id)
}

// prefillParallelIntoCache fills an objects bucket with vecs (a legacy vector per id),
// then runs prefillCacheParallel against a real cache wired so any miss errors. h.nodes
// and the cache are sized to preGrown. Returns the populated cache.
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
	// id has no "vectors_" prefix => legacy default target vector
	h := newPrefillTestIndex("main", store, c, preGrown, dp, logger)
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

// TestScanObjectVectorsParallelNamedVectorIsolation: named indexes share one objects
// bucket, so a scan must yield only its own target and skip objects lacking it —
// never bleed a sibling's or the legacy vector into the wrong cache.
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

// TestPrefillCacheParallelNormalizesForCosine: a cosine-dot cache must hold normalized
// vectors. The bucket stores them raw and the serial path normalizes via the cache's
// normalizeOnRead wrapper, which preloading bypasses — mismatch means wrong distances.
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

	h := newPrefillTestIndex("main", store, c, n, distancer.NewCosineDistanceProvider(), logger)

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
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(n)

	inserted := map[uint64][]float32{3: {42, 42}, 7: {43, 43}}
	for id, vec := range inserted {
		c.Preload(id, vec)
		exp[id] = vec
	}

	h := newPrefillTestIndex("main", store, c, n, distancer.NewDotProductProvider(), logger)
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

	h := newPrefillTestIndex("main", store, c, n, distancer.NewDotProductProvider(), logger)
	h.allocChecker = failingAllocChecker{}
	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(prefillAllocCheckEvery), c.CountVectors())
}

// TestPrefillCacheParallelSkipsWhenAlreadyCompressed: an index that is already
// compressed when the scan starts must not load a single vector into the
// uncompressed cache.
func TestPrefillCacheParallelSkipsWhenAlreadyCompressed(t *testing.T) {
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < 50; i++ {
		putTestObject(t, bucket, i, []float32{float32(i)}, nil)
	}

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)
	c.Grow(50)

	h := newPrefillTestIndex("main", store, c, 50, distancer.NewDotProductProvider(), logger)
	h.compressed.Store(true)

	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(0), c.CountVectors())
}

// compressionFlippingCache flips the index's compressed flag from inside the cache
// write, after a fixed number of stores. Compression activating mid-scan is
// otherwise a timing race the test cannot place.
type compressionFlippingCache struct {
	cache.Cache[float32]
	inner     cache.IfAbsentPreloader[float32]
	h         *hnsw
	flipAfter int64
	stored    atomic.Int64
}

func (c *compressionFlippingCache) PreloadIfAbsent(id uint64, vec []float32) bool {
	stored := c.inner.PreloadIfAbsent(id, vec)
	if stored && c.stored.Add(1) == c.flipAfter {
		c.h.compressed.Store(true)
	}
	return stored
}

// TestPrefillCacheParallelStopsWhenCompressionActivatesMidScan: the compression guard
// exists for a flag that flips while a long scan is running, not just for an index
// already compressed at entry. The scan must abandon the rest of the bucket, leaving a
// partial cache, and still report success — the vectors it skipped load on demand.
func TestPrefillCacheParallelStopsWhenCompressionActivatesMidScan(t *testing.T) {
	const n = 50
	const flipAfter = 10

	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTestObject(t, bucket, i, []float32{float32(i)}, nil)
	}
	// no FlushAndSwitch: memtable-only data yields no quantile seeds, so the scan
	// runs as a single sequential range and the flip lands at an exact store count

	logger, _ := test.NewNullLogger()
	inner := cache.NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)
	inner.Grow(n)

	h := newPrefillTestIndex("main", store, nil, n, distancer.NewDotProductProvider(), logger)
	h.cache = &compressionFlippingCache{
		Cache: inner, inner: mustIfAbsentPreloader(t, inner), h: h, flipAfter: flipAfter,
	}

	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.True(t, h.compressed.Load(), "the wrapper never reached the flip point")
	require.Equal(t, int64(flipAfter), inner.CountVectors())
}

// TestPrefillCacheParallelStopsBelowCacheFull: the scan must stop short of maxSize,
// not on it — replaceIfFull wipes at count == maxSize, discarding the whole scan
// within one deletion interval. Memtable-only data forces a single scan range, so the
// stop point is deterministic.
func TestPrefillCacheParallelStopsBelowCacheFull(t *testing.T) {
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

	h := newPrefillTestIndex("main", store, c, n, distancer.NewDotProductProvider(), logger)
	require.NoError(t, h.prefillCacheParallel(context.Background()))
	require.Equal(t, int64(maxSize-1), c.CountVectors())
	require.Less(t, c.CountVectors(), c.CopyMaxSize(),
		"a completed scan must leave the cache below the wipe threshold")
}

// TestUseParallelPrefillExcludesHFresh: the hfresh centroid index shares the shard's
// store (objects bucket present) but its cache holds centroid vectors, so the
// objects scan must never run for it.
func TestUseParallelPrefillExcludesHFresh(t *testing.T) {
	store := newTestObjectsStore(t)
	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(nil, nil, 1_000_000, 1, logger, false, 0, nil)

	h := newPrefillTestIndex("main_centroids", store, c, 0, distancer.NewDotProductProvider(), logger)
	h.hfreshMode = true
	require.False(t, h.useParallelPrefill())

	h.hfreshMode = false
	require.True(t, h.useParallelPrefill())
}

// countingAllocChecker fails every probe once armed, and records how many vectors
// were preloaded after the first failure.
type countingAllocChecker struct {
	aborted *atomic.Bool
}

func (c *countingAllocChecker) CheckAlloc(size int64) error {
	c.aborted.Store(true)
	return fmt.Errorf("simulated memory pressure")
}

func (c *countingAllocChecker) CheckMappingAndReserve(int64, int) error { return nil }
func (c *countingAllocChecker) Refresh(bool)                            {}

// countingPreloadCache counts stores that land after the alloc probe has failed.
// Counting at the store itself keeps the measurement exact — sampling a shared
// counter from inside the probe would race the workers still writing.
type countingPreloadCache struct {
	cache.Cache[float32]
	inner      cache.IfAbsentPreloader[float32]
	aborted    atomic.Bool
	afterAbort atomic.Int64
}

func (c *countingPreloadCache) PreloadIfAbsent(id uint64, vec []float32) bool {
	ok := c.inner.PreloadIfAbsent(id, vec)
	if ok && c.aborted.Load() {
		c.afterAbort.Add(1)
	}
	return ok
}

// TestPrefillCacheParallelStopsAllWorkersOnMemoryPressure: the alloc probe fires on
// one worker, but every worker must stop at its next row. Returning an error only
// ends the worker that raised it — its siblings poll the cancelled context once per
// 1024 rows, so without shared abort state they keep filling the cache long after
// the probe said to stop, which on large vectors is the memory the probe was
// defending. Multiple flushed segments give the scan several concurrent ranges.
func TestPrefillCacheParallelStopsAllWorkersOnMemoryPressure(t *testing.T) {
	const n = 40_000
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	for i := uint64(0); i < n; i++ {
		putTestObject(t, bucket, i, []float32{float32(i), float32(i) + 1}, nil)
		if i%5_000 == 4_999 {
			require.NoError(t, bucket.FlushAndSwitch())
		}
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	inner := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, 10_000_000, 1, logger, false, 0, nil)
	inner.Grow(n)
	counting := &countingPreloadCache{Cache: inner, inner: mustIfAbsentPreloader(t, inner)}

	h := newPrefillTestIndex("main", store, counting, n, distancer.NewDotProductProvider(), logger)
	h.allocChecker = &countingAllocChecker{aborted: &counting.aborted}

	require.NoError(t, h.prefillCacheParallel(context.Background()))

	// a worker already past the abort check may still finish its row, so allow one
	// per worker; what must not happen is the ~1023 rows each would otherwise run
	// before its next context poll
	after := counting.afterAbort.Load()
	// Bounded by one worker's poll interval, not by workers x that interval: before
	// the shared abort, every sibling ran to its next context poll. Observed is a
	// row or two per worker; the margin absorbs scheduling.
	require.LessOrEqual(t, after, int64(prefillCtxCheckEvery),
		"%d vectors were cached after the alloc probe failed", after)
}

// TestParallelPrefillExactFitTakesTheScan: an exact fit must not be pushed onto the
// serial prefiller. onVector already stops one short of maxSize, so the scan cannot
// reach the count replaceIfFull wipes on, and the serial path would cache nothing.
func TestParallelPrefillExactFitTakesTheScan(t *testing.T) {
	const n = 200
	store := newTestObjectsStore(t)
	bucket := store.Bucket(helpers.ObjectsBucketLSM)
	exp := map[uint64][]float32{}
	for i := uint64(0); i < n; i++ {
		vec := []float32{float32(i), float32(i) + 1}
		putTestObject(t, bucket, i, vec, nil)
		exp[i] = vec
	}
	require.NoError(t, bucket.FlushAndSwitch())

	logger, _ := test.NewNullLogger()
	c := cache.NewShardedFloat32LockCache(errOnCacheMiss, nil, n, 1, logger, false, 0, nil)
	c.Grow(n)
	h := newPrefillTestIndex("main", store, c, n, distancer.NewDotProductProvider(), logger)

	require.True(t, h.useParallelPrefill(), "an exact fit must still take the scan")
	require.NoError(t, h.prefillCacheParallel(context.Background()))

	// the guard stops one short of maxSize, so all but the last vector are resident
	require.Equal(t, int64(n-1), c.CountVectors())
	require.Less(t, c.CountVectors(), c.CopyMaxSize())
}

// TestSerialPrefillerMeasuresOccupancyNotAllocation: the limit must be compared
// against how many vectors are cached, not against how far the cache is allocated.
// Restore grows the cache to cover the node range and growTargetFor rounds up beyond
// it, so measuring Len() means an index whose cache already covers its nodes — every
// index, after restore — prefills nothing at all.
func TestSerialPrefillerMeasuresOccupancyNotAllocation(t *testing.T) {
	cases := []struct {
		name    string
		nodes   int
		maxSize int
		want    int64
	}{
		{"cache exactly fits the nodes", 10, 10, 10},
		{"cache larger than the nodes", 10, 11, 10},
		{"bounded cache stops at the limit", 10, 5, 5},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			vecFor := func(ctx context.Context, id uint64) ([]float32, error) {
				return []float32{float32(id), 1}, nil
			}
			c := cache.NewShardedFloat32LockCache(vecFor, nil, tc.maxSize, 1, logger, false, 0, nil)
			c.Grow(uint64(tc.nodes)) // restore covers the node range before any prefill
			require.Greater(t, c.Len(), int32(tc.maxSize),
				"the allocated span must exceed the limit, or this test proves nothing")

			h := newPrefillTestIndex("main", nil, c, tc.nodes, distancer.NewDotProductProvider(), logger)
			require.NoError(t, newVectorCachePrefiller(c, h, logger).
				Prefill(context.Background(), int(c.CopyMaxSize())))

			require.Equal(t, tc.want, c.CountVectors())
		})
	}
}
