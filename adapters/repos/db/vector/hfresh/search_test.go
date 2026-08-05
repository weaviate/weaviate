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

package hfresh

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/storobj"
)

func TestSearchTreatsPartiallyInitializedDimensionsAsEmptyIndex(t *testing.T) {
	tf := createHFreshIndex(t)
	vector := createTestVectors(32, 1)[0]

	atomic.StoreUint32(&tf.Index.dims, uint32(len(vector)))

	ids, dists, err := tf.Index.SearchByVector(t.Context(), vector, 10, nil)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)
}

func TestFlatSearchTreatsPartiallyInitializedIndexAsEmptyIndex(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 32)

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if indexID == 0 {
			return vectors[0], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	atomic.StoreUint32(&index.dims, uint32(len(vectors[0])))

	// a small allow list routes the search through flatSearch, which must
	// also treat the partially initialized index as empty
	allowList := helpers.NewAllowList(0)
	ids, dists, err := index.SearchByVector(t.Context(), vectors[0], 10, allowList)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)
}

func TestSearchWithEmptyIndex(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	vectors, _ := testinghelpers.RandomVecs(1, 0, 32)

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if indexID == 0 {
			return vectors[0], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	// search on empty index returns 0 results and no error
	ids, dists, err := index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)

	err = index.Add(t.Context(), 0, vectors[0])
	require.NoError(t, err)

	ids, dists, err = index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Len(t, dists, 1)
	require.Equal(t, uint64(0), ids[0])

	err = index.Delete(0)
	require.NoError(t, err)

	ids, dists, err = index.SearchByVector(t.Context(), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Empty(t, ids)
	require.Empty(t, dists)
}

// TestSearchCosineDistanceRescore verifies that HFresh correctly computes and
// reports distances when using cosine distance. The VectorForIDThunk returns
// raw (unnormalized) vectors to simulate the real object store, where vectors
// are stored as provided by the user.
//
// The bug: HFresh normalizes the query vector internally for quantized search
// but the rescore step fetches raw vectors from vectorForId. The cosine-dot
// distance function computes 1-dot(a,b) and clamps negative values to 0.
// Since dot(normalized_query, unnormalized_stored) > 1 for vectors with
// magnitude > 1, all rescored distances get clamped to 0.
func TestSearchCosineDistanceRescore(t *testing.T) {
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	// Use cosine distance for both the main index and centroids,
	// matching how shard_init_vector.go configures HFresh in production.
	cfg.DistanceProvider = distancer.NewCosineDistanceProvider()
	cfg.Centroids.HNSWConfig.DistanceProvider = distancer.NewCosineDistanceProvider()

	vectorsSize := 500
	dimensions := 32
	k := 10

	vectors, _ := testinghelpers.RandomVecsFixedSeed(vectorsSize, 0, dimensions)

	// Verify that our test vectors have magnitude > 1, which is
	// required to trigger the bug.
	var norm float32
	for _, v := range vectors[0] {
		norm += v * v
	}
	norm = float32(math.Sqrt(float64(norm)))
	require.Greater(t, norm, float32(1.0),
		"test vectors should have magnitude > 1 to exercise the bug")

	// VectorForIDThunk returns RAW (unnormalized) vectors, simulating
	// the real object store. In production, the shard stores the user's
	// original vector, not the normalized version that HFresh uses internally.
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if int(indexID) < len(vectors) {
			return vectors[indexID], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	for i := 0; i < vectorsSize; i++ {
		err := index.Add(t.Context(), uint64(i), vectors[i])
		require.NoError(t, err)
	}

	// Wait for background tasks (splits, merges) to complete.
	for index.taskQueue.Size() > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	// Search for a vector that was indexed. The self-match must be the
	// top result with distance ≈ 0, and the remaining results must have
	// non-zero distances in ascending order.
	queryID := uint64(42)
	ids, dists, err := index.SearchByVector(t.Context(), vectors[queryID], k, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ids)

	// Self-match must be rank 1.
	assert.Equal(t, queryID, ids[0],
		"self-match should be the first result")

	// Self-match distance must be approximately 0.
	assert.InDelta(t, 0, dists[0], 0.01,
		"self-match distance should be near zero")

	// Not all distances should be zero — if they are, the rescore is broken.
	allZero := true
	for _, d := range dists {
		if d > 0 {
			allZero = false
			break
		}
	}
	assert.False(t, allZero,
		"all distances are zero: rescore is not producing correct distances")

	// Distances must be monotonically non-decreasing (correctly ordered).
	for i := 1; i < len(dists); i++ {
		assert.LessOrEqual(t, dists[i-1], dists[i],
			"distances should be non-decreasing: dists[%d]=%f > dists[%d]=%f",
			i-1, dists[i-1], i, dists[i])
	}

	// Verify returned distances match independently computed cosine distances.
	cosine := distancer.NewCosineDistanceProvider()
	for i, id := range ids {
		normalizedQuery := distancer.Normalize(vectors[queryID])
		normalizedStored := distancer.Normalize(vectors[id])
		expected, err := cosine.SingleDist(normalizedQuery, normalizedStored)
		require.NoError(t, err)
		assert.InDelta(t, expected, dists[i], 0.01,
			"result %d (id=%d): returned distance %f != expected cosine distance %f",
			i, id, dists[i], expected)
	}
}

// searchMarker tags contexts issued by the tests below so the instrumented
// VectorForIDThunk only counts fetches belonging to our search calls, not
// fetches from background split/reassign workers.
type searchMarker struct{}

// newSearchTestIndex builds an HFresh index serving the given vectors through
// VectorForIDThunk. A non-nil intercept runs first on every fetch; a non-nil
// error from it fails that fetch.
func newSearchTestIndex(t *testing.T, vectors [][]float32, intercept func(ctx context.Context, id uint64) error) *HFresh {
	t.Helper()
	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector,
		func(ctx context.Context, id uint64, targetVector string) ([]float32, error) {
			if intercept != nil {
				if err := intercept(ctx, id); err != nil {
					return nil, err
				}
			}
			if int(id) >= len(vectors) {
				return nil, storobj.NewErrNotFoundf(id, "out of range")
			}
			return vectors[id], nil
		})

	index := makeHFreshWithConfig(t, store, cfg, uc)
	for i, vec := range vectors {
		require.NoError(t, index.Add(t.Context(), uint64(i), vec))
	}
	return index
}

func TestRescoreConcurrencyRespectsBudget(t *testing.T) {
	vectors, queries := testinghelpers.RandomVecs(300, 1, 32)

	var cur, maxSeen atomic.Int64
	index := newSearchTestIndex(t, vectors, func(ctx context.Context, id uint64) error {
		if ctx.Value(searchMarker{}) == nil {
			return nil
		}
		c := cur.Add(1)
		for {
			m := maxSeen.Load()
			if c <= m || maxSeen.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(500 * time.Microsecond) // force worker overlap
		cur.Add(-1)
		return nil
	})

	searchCtx := context.WithValue(t.Context(), searchMarker{}, true)
	query := queries[0]

	// no budget in ctx: fan-out up to 2*GOMAXPROCS allowed
	cur.Store(0)
	maxSeen.Store(0)
	idsFree, distsFree, err := index.SearchByVector(searchCtx, query, 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, idsFree)
	t.Logf("no budget: max concurrent rescore fetches = %d", maxSeen.Load())
	require.Greater(t, maxSeen.Load(), int64(1),
		"rescore fetches must overlap when no budget caps them")

	// budget = 1: rescore must be serial
	cur.Store(0)
	maxSeen.Store(0)
	budgetCtx := concurrency.CtxWithBudget(searchCtx, 1)
	idsBudget, distsBudget, err := index.SearchByVector(budgetCtx, query, 10, nil)
	require.NoError(t, err)
	t.Logf("budget=1: max concurrent rescore fetches = %d", maxSeen.Load())
	require.Equal(t, int64(1), maxSeen.Load(),
		"a budget of 1 must serialize the rescore")

	// identical inputs must produce identical results either way
	require.Equal(t, idsFree, idsBudget)
	require.Equal(t, distsFree, distsBudget)
}

func TestRescoreSkipsCandidatesDeletedMidQuery(t *testing.T) {
	vectors, _ := testinghelpers.RandomVecs(100, 1, 32)
	const deletedID = uint64(5)

	// simulate a deletion the index has not processed yet: the full vector
	// is gone from the object store, but only for our search (background
	// workers still see it, keeping the index intact)
	index := newSearchTestIndex(t, vectors, func(ctx context.Context, id uint64) error {
		if id == deletedID && ctx.Value(searchMarker{}) != nil {
			return storobj.NewErrNotFoundf(id, "deleted mid-query")
		}
		return nil
	})

	// querying with the deleted vector itself guarantees it is a rescore
	// candidate; the search must skip it without failing
	searchCtx := context.WithValue(t.Context(), searchMarker{}, true)
	ids, _, err := index.SearchByVector(searchCtx, vectors[deletedID], 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, ids)
	require.NotContains(t, ids, deletedID,
		"a candidate whose vector vanished mid-query must be dropped, not returned")
}

func TestRescoreTieBreakingIsDeterministic(t *testing.T) {
	// 30 exact duplicates of the query vector (ids 0-29) followed by 70
	// distinct vectors: the top-k boundary falls inside a 30-way distance
	// tie, so which ids are returned depends entirely on tie handling.
	const dupes = 30
	vectors, _ := testinghelpers.RandomVecs(100, 1, 32)
	for i := 1; i < dupes; i++ {
		vectors[i] = vectors[0]
	}

	index := newSearchTestIndex(t, vectors, nil)

	// tie handling must not depend on worker scheduling or the budget:
	// every run over the same index must return the same ids
	ctx := t.Context()
	first, _, err := index.SearchByVector(ctx, vectors[0], 10, nil)
	require.NoError(t, err)
	require.NotEmpty(t, first)

	for range 5 {
		again, _, err := index.SearchByVector(ctx, vectors[0], 10, nil)
		require.NoError(t, err)
		require.Equal(t, first, again, "rescore tie-breaking drifted between runs")
	}

	serial, _, err := index.SearchByVector(concurrency.CtxWithBudget(ctx, 1), vectors[0], 10, nil)
	require.NoError(t, err)
	require.Equal(t, first, serial, "budget must not change tie handling")
}
