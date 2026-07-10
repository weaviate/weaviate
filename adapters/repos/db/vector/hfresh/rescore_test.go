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
	"sync"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// buildRescoreTestIndex creates an index big enough that queries produce a
// full rescore candidate set (rescoreLimit), so the adaptive cutoff has room
// to trigger.
func buildRescoreTestIndex(t *testing.T, vectorsSize, dims int) (*HFresh, [][]float32) {
	t.Helper()

	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)

	vectors, _ := testinghelpers.RandomVecsFixedSeed(vectorsSize, 0, dims)

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		if int(indexID) < len(vectors) {
			return vectors[indexID], nil
		}
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})

	index := makeHFreshWithConfig(t, store, cfg, uc)

	for i := range vectorsSize {
		require.NoError(t, index.Add(t.Context(), uint64(i), vectors[i]))
	}
	for index.taskQueue.Size() > 0 {
		time.Sleep(50 * time.Millisecond)
	}

	return index, vectors
}

// Adaptive rescore must return exactly the same results as the full rescore:
// it may only skip candidates that provably cannot enter the top k.
func TestAdaptiveRescoreMatchesFullRescore(t *testing.T) {
	const (
		vectorsSize = 2000
		dims        = 64
		k           = 10
		queries     = 25
	)

	index, vectors := buildRescoreTestIndex(t, vectorsSize, dims)

	logger, _ := logrustest.NewNullLogger()
	index.profiler = newSearchProfiler(logger, 1_000_000)

	type result struct {
		ids   []uint64
		dists []float32
	}

	// full rescore baseline
	index.adaptiveRescore = false
	baseline := make([]result, queries)
	for i := range queries {
		ids, dists, err := index.SearchByVector(t.Context(), vectors[i*7], k, nil)
		require.NoError(t, err)
		require.NotEmpty(t, ids)
		baseline[i] = result{ids, dists}
	}
	require.EqualValues(t, 0, index.profiler.snapshot().RescoreSkipped.Sum(),
		"full rescore must not skip anything")

	// adaptive rescore must match exactly, query by query
	index.adaptiveRescore = true
	for i := range queries {
		ids, dists, err := index.SearchByVector(t.Context(), vectors[i*7], k, nil)
		require.NoError(t, err)
		assert.Equal(t, baseline[i].ids, ids, "query %d: ids diverged", i)
		assert.Equal(t, baseline[i].dists, dists, "query %d: dists diverged", i)
	}

	// and it must actually have skipped something across the query set,
	// otherwise this test is vacuous
	s := index.profiler.snapshot()
	assert.Positive(t, s.RescoreSkipped.Sum(),
		"adaptive rescore never skipped a candidate — cutoff not working")
}

// The full-rescore path (adaptive off) must be equivalent to the old
// semantics: rescore every candidate and return the k smallest by exact
// distance. This pins the streaming worker pool against dropping or
// duplicating candidates, at concurrency 1 and 16 alike.
func TestFullRescoreMatchesReference(t *testing.T) {
	const (
		vectorsSize = 2000
		dims        = 64
		k           = 10
		queries     = 15
	)

	index, vectors := buildRescoreTestIndex(t, vectorsSize, dims)
	index.adaptiveRescore = false

	// force the vectorForId path and record the exact candidate set the
	// rescore fetches, so the reference is built over the same candidates.
	index.getViewThunk = nil
	index.vectorForIDWithView = nil

	origThunk := index.vectorForId
	var mu sync.Mutex
	var seen []uint64
	index.vectorForId = func(ctx context.Context, id uint64) ([]float32, error) {
		mu.Lock()
		seen = append(seen, id)
		mu.Unlock()
		return origThunk(ctx, id)
	}

	for qi := range queries {
		query := vectors[qi*11]

		// run 1: sequential and deterministic, records the full candidate set
		mu.Lock()
		seen = seen[:0]
		mu.Unlock()
		index.rescoreConcurrency = 1
		ids1, dists1, err := index.SearchByVector(t.Context(), query, k, nil)
		require.NoError(t, err)
		require.NotEmpty(t, ids1)

		candidateIDs := append([]uint64(nil), seen...)

		// reference: rescore every recorded candidate and take the k best,
		// using the same normalization + distancer the index uses.
		normQuery := index.normalizeVec(append([]float32(nil), query...))
		ref := NewResultSet(k)
		for _, id := range candidateIDs {
			vec, err := origThunk(t.Context(), id)
			require.NoError(t, err)
			vec = index.normalizeVec(vec)
			dist, err := index.distancer.distancer.SingleDist(normQuery, vec)
			require.NoError(t, err)
			ref.Insert(id, dist)
		}
		var refIDs []uint64
		var refDists []float32
		for id, d := range ref.Iter() {
			refIDs = append(refIDs, id)
			refDists = append(refDists, d)
		}

		assert.Equal(t, refIDs, ids1, "query %d: concurrency=1 diverged from reference", qi)
		assert.Equal(t, refDists, dists1, "query %d: concurrency=1 dists diverged", qi)

		// run 2: the concurrent pool must return the same top-k
		index.rescoreConcurrency = 16
		ids16, dists16, err := index.SearchByVector(t.Context(), query, k, nil)
		require.NoError(t, err)
		assert.Equal(t, refIDs, ids16, "query %d: concurrency=16 diverged from reference", qi)
		assert.Equal(t, refDists, dists16, "query %d: concurrency=16 dists diverged", qi)
	}
}

// The margin factor and minimum-rescore knobs must parse from the environment
// into the corresponding HFresh fields at construction time.
func TestRescoreEnvKnobs(t *testing.T) {
	t.Setenv("HFRESH_RESCORE_MARGIN_FACTOR", "3.5")
	t.Setenv("HFRESH_RESCORE_MIN", "128")

	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})
	index := makeHFreshWithConfig(t, store, cfg, uc)

	assert.EqualValues(t, 3.5, index.rescoreMarginFactor)
	assert.Equal(t, 128, index.rescoreMin)
}

// The kill switch must force the full-rescore path.
func TestAdaptiveRescoreEnvDisable(t *testing.T) {
	t.Setenv("HFRESH_ADAPTIVE_RESCORE", "0")

	store := testinghelpers.NewDummyStore(t)
	cfg, uc := makeHFreshConfig(t)
	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		return nil, fmt.Errorf("vector not found for ID %d", indexID)
	})
	index := makeHFreshWithConfig(t, store, cfg, uc)

	assert.False(t, index.adaptiveRescore)
}
