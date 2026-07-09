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
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hfresh"
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
// but the rescore step fetches raw vectors from vectorForID. The cosine-dot
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

// Regression tests: the number of returned results must be
// min(limit, matching docs). rescoreLimit (default 350) is a quality floor
// for the RQ1 candidate depth, not a cap — limits above it were silently
// truncated to rescoreLimit results.
//
// For limit == nDocs the assertion allows a small tolerance: centroid
// retrieval is approximate (HNSW) and may transiently miss a posting while
// background splits run, so demanding all 500 documents is a recall
// guarantee the index does not make. The pinned regression — results capped
// at rescoreLimit — is still caught, as it would return only 350.
func TestSearchLimitNotCappedByRescoreLimit(t *testing.T) {
	const (
		nDocs        = 500
		dim          = 32
		fullScanFlex = 10 // tolerated misses when limit == nDocs
	)
	limits := []int{10, 350, 400, 500}

	assertLen := func(t *testing.T, limit int, ids []uint64) {
		t.Helper()
		if limit >= nDocs {
			require.GreaterOrEqual(t, len(ids), nDocs-fullScanFlex, "limit=%d", limit)
			require.LessOrEqual(t, len(ids), nDocs, "limit=%d", limit)
			return
		}
		require.Len(t, ids, limit, "limit=%d", limit)
	}

	t.Run("single vector path", func(t *testing.T) {
		tf := createHFreshIndex(t)
		// the non-muvera path rescores against the original vectors, which
		// the shard normally provides via VectorForIDThunk
		stored := make(map[uint64][]float32, nDocs)
		tf.Index.vectorForID = func(_ context.Context, id uint64) ([]float32, error) {
			return stored[id], nil
		}
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < nDocs; i++ {
			vec := make([]float32, dim)
			for j := range vec {
				vec[j] = rng.Float32()
			}
			stored[uint64(i)] = vec
			addVectorToIndex(t, &tf, uint64(i), vec)
		}

		probe := make([]float32, dim)
		for j := range probe {
			probe[j] = rng.Float32()
		}
		for _, limit := range limits {
			ids, dists, err := tf.Index.SearchByVector(t.Context(), probe, limit, nil)
			require.NoError(t, err)
			assertLen(t, limit, ids)
			require.Len(t, dists, len(ids), "limit=%d", limit)
		}
	})

	t.Run("multi vector path", func(t *testing.T) {
		tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
		rng := rand.New(rand.NewSource(42))
		for i := 0; i < nDocs; i++ {
			addMultiVectorToIndex(t, &tf, uint64(i), randomMultiVector(rng, 2, dim))
		}

		probe := randomMultiVector(rng, 2, dim)
		for _, limit := range limits {
			ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), probe, limit, nil)
			require.NoError(t, err)
			assertLen(t, limit, ids)
			require.Len(t, dists, len(ids), "limit=%d", limit)
		}
	})
}

// Regression test (single-node manifestation): a multi-vector
// search on a collection that never indexed any multi-vector data must
// return a clean error. Before the guard, EncodeQuery ran on the
// uninitialized muvera encoder (nil projection matrices) and panicked with
// "index out of range [0] with length 0".
func TestSearchByMultiVectorOnEmptyCollection(t *testing.T) {
	tf := createMuveraHFreshIndex(t)
	probe := [][]float32{{0.1, 0.2, 0.3, 0.4}}

	t.Run("search errors cleanly before first insert", func(t *testing.T) {
		_, _, err := tf.Index.SearchByMultiVector(t.Context(), probe, 3, nil)
		require.ErrorIs(t, err, ErrMuveraNotInitialized)
	})

	t.Run("distance search errors cleanly before first insert", func(t *testing.T) {
		_, _, err := tf.Index.SearchByMultiVectorDistance(t.Context(), probe, 0.5, 100, nil)
		require.ErrorIs(t, err, ErrMuveraNotInitialized)
	})

	t.Run("search works after first insert", func(t *testing.T) {
		addMultiVectorToIndex(t, &tf, 0, [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}})
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), probe, 3, nil)
		require.NoError(t, err)
		require.Equal(t, []uint64{0}, ids)
	})
}

// Regression tests: multi-vector search with cosine distance
// must normalize tokens on both the insert and the query/rescore paths. The
// cosine-dot provider computes 1-dot, which only equals the cosine distance
// on unit vectors, and callers are not required to send normalized tokens.

func randomMultiVector(rng *rand.Rand, tokens, dim int) [][]float32 {
	mv := make([][]float32, tokens)
	for i := range mv {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		mv[i] = v
	}
	return mv
}

// TestSearchByMultiVectorMaxSimOrderingCosine mirrors e2e TC-013[cosine]:
// with A=[e0,e0], B=[e0,e1], C=[e1,e1] and query [e0], A and B tie on exact
// MaxSim (both contain a token identical to the query token), so the
// deterministic (distance, id) tie-break must order A before B, and C, the
// only doc without an e0 token, must come last.
func TestSearchByMultiVectorMaxSimOrderingCosine(t *testing.T) {
	const dim = 32
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))

	onehot := func(idx int) []float32 {
		v := make([]float32, dim)
		v[idx] = 1.0
		return v
	}
	e0, e1 := onehot(0), onehot(1)

	addMultiVectorToIndex(t, &tf, 0, [][]float32{e0, e0}) // A
	addMultiVectorToIndex(t, &tf, 1, [][]float32{e0, e1}) // B
	addMultiVectorToIndex(t, &tf, 2, [][]float32{e1, e1}) // C

	ids, dists, err := tf.Index.SearchByMultiVector(t.Context(), [][]float32{e0}, 3, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 1, 2}, ids, "expected order A, B, C")

	require.Len(t, dists, 3)
	assert.Equal(t, float32(0), dists[0], "A contains the query token, distance must be 0")
	assert.Equal(t, float32(0), dists[1], "B contains the query token, distance must be 0")
	assert.Equal(t, float32(1), dists[2], "C is orthogonal to the query token")
}

// TestSearchByMultiVectorSelfRecallCosine mirrors e2e TC-006: querying with a
// document's own (unnormalized) multi-vector must return that document as
// top-1. All docs fit within the default rescoreLimit, so the exact MaxSim
// rescore sees every document and self-recall@1 must be perfect.
func TestSearchByMultiVectorSelfRecallCosine(t *testing.T) {
	const (
		nDocs  = 200
		tokens = 8
		dim    = 96
	)
	tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))

	rng := rand.New(rand.NewSource(7))
	docs := make([][][]float32, nDocs)
	for i := 0; i < nDocs; i++ {
		docs[i] = randomMultiVector(rng, tokens, dim)
		addMultiVectorToIndex(t, &tf, uint64(i), docs[i])
	}

	sampleRng := rand.New(rand.NewSource(13))
	const samples = 50
	for q := 0; q < samples; q++ {
		id := uint64(sampleRng.Intn(nDocs))
		ids, _, err := tf.Index.SearchByMultiVector(t.Context(), docs[id], 1, nil)
		require.NoError(t, err)
		require.NotEmpty(t, ids)
		require.Equal(t, id, ids[0],
			"self-query for doc %d returned doc %d: exact MaxSim rescore must rank the identical multi-vector first", id, ids[0])
	}
}

// TestResultSetTieBreak pins the deterministic (distance, id) ordering of
// ResultSet: equal distances must be returned in ascending id order
// regardless of insertion order, including evictions at the capacity
// boundary.
func TestResultSetTieBreak(t *testing.T) {
	type entry struct {
		id   uint64
		dist float32
	}

	tests := []struct {
		name    string
		k       int
		inserts []entry
		want    []uint64
	}{
		{
			name:    "all tied, inserted ascending",
			k:       3,
			inserts: []entry{{1, 0.5}, {2, 0.5}, {3, 0.5}},
			want:    []uint64{1, 2, 3},
		},
		{
			name:    "all tied, inserted descending",
			k:       3,
			inserts: []entry{{3, 0.5}, {2, 0.5}, {1, 0.5}},
			want:    []uint64{1, 2, 3},
		},
		{
			name:    "tie broken within mixed distances",
			k:       4,
			inserts: []entry{{7, 0.2}, {3, 0.1}, {5, 0.2}, {1, 0.3}},
			want:    []uint64{3, 5, 7, 1},
		},
		{
			name:    "eviction at boundary keeps lowest ids among ties",
			k:       2,
			inserts: []entry{{9, 0.5}, {4, 0.5}, {6, 0.5}, {2, 0.5}},
			want:    []uint64{2, 4},
		},
		{
			name:    "tied item does not evict smaller id",
			k:       1,
			inserts: []entry{{2, 0.5}, {5, 0.5}},
			want:    []uint64{2},
		},
		{
			name:    "smaller distance still wins over smaller id",
			k:       2,
			inserts: []entry{{1, 0.9}, {2, 0.9}, {8, 0.1}},
			want:    []uint64{8, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := NewResultSet(tt.k)
			for _, in := range tt.inserts {
				rs.Insert(in.id, in.dist)
			}

			got := make([]uint64, 0, rs.Len())
			for id := range rs.Iter() {
				got = append(got, id)
			}
			require.Equal(t, tt.want, got)

			// distances must remain sorted ascending
			var prev float32 = -1
			for _, dist := range rs.data {
				require.GreaterOrEqual(t, dist.Distance, prev)
				prev = dist.Distance
			}
		})
	}
}

// TestConfigUpdateDoesNotRaceSearch pins the memory-safety contract of the
// tunable search budgets: UpdateUserConfig stores searchProbe and
// rescoreLimit atomically while searches are in flight, so every read on the
// search paths must be atomic too. A plain read here is a data race — this
// test fails under -race without the atomic loads in SearchByVector and
// muveraSearchBudgets.
func TestConfigUpdateDoesNotRaceSearch(t *testing.T) {
	const (
		dim    = 16
		nDocs  = 50
		rounds = 200
	)
	rng := rand.New(rand.NewSource(3))
	vectors := make([][]float32, nDocs)
	for i := range vectors {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		vectors[i] = v
	}
	tf := createHFreshIndexWithVectorStore(t, vectors)
	for i, v := range vectors {
		addVectorToIndex(t, &tf, uint64(i), v)
	}

	probe := vectors[0]
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < rounds; i++ {
			uc := ent.NewDefaultUserConfig()
			uc.SearchProbe = uint32(8 + i%64)
			uc.RQ.RescoreLimit = 100 + i%300
			require.NoError(t, tf.Index.UpdateUserConfig(uc, func() {}))
		}
	}()
	for i := 0; i < rounds; i++ {
		_, _, err := tf.Index.SearchByVector(t.Context(), probe, 10, nil)
		require.NoError(t, err)
		_, _ = tf.Index.muveraSearchBudgets(10)
	}
	<-done
}
