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

//go:build integrationTest

package hnsw

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// storeWithObjectsBucket gives the index a real objects bucket so useParallelPrefill
// gets past its bucket-presence check and actually evaluates the eligibility gate —
// otherwise every index would fall back to serial for the wrong reason.
func storeWithObjectsBucket(t *testing.T) *lsmkv.Store {
	store := testinghelpers.NewDummyStore(t)
	require.NoError(t, store.CreateOrLoadBucket(context.Background(), helpers.ObjectsBucketLSM,
		lsmkv.WithStrategy(lsmkv.StrategyReplace)))
	return store
}

func newPrefillRoutingIndex(t *testing.T, id string, uc ent.UserConfig, store *lsmkv.Store) *hnsw {
	idx, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    id,
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewDotProductProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{0.1, 0.2, 0.3}, nil
		},
		MultiVectorForIDThunk: func(ctx context.Context, id uint64) ([][]float32, error) {
			return multiVectors[id], nil
		},
		MakeBucketOptions:   lsmkv.MakeNoopBucketOptions,
		AllocChecker:        memwatch.NewDummyMonitor(),
		GetViewThunk:        func() common.BucketView { return &multivectorNoopBucketView{} },
		WaitForCachePrefill: true,
	}, uc, cyclemanager.NewCallbackGroupNoop(), store)
	require.NoError(t, err)
	return idx
}

// TestUseParallelPrefillRoutingRealIndex builds real indexes (objects bucket present,
// sync prefill, unbounded cache) and confirms the config→atomic→gate wiring routes
// each index type to the right prefill path: only a plain single-vector index takes
// the parallel objects-bucket scan; multivector and muvera stay on the serial path
// because their caches are not sourced from the objects bucket.
// prefillRoutingUserConfig is the baseline single-vector config that satisfies the
// parallel-prefill gate (unbounded cache); tests layer Multivector/Muvera on top to
// flip the expected routing.
func prefillRoutingUserConfig() ent.UserConfig {
	return ent.UserConfig{VectorCacheMaxObjects: 1e12, MaxConnections: 8, EFConstruction: 64, EF: 64}
}

func muveraUserConfig() ent.UserConfig {
	uc := prefillRoutingUserConfig()
	uc.Multivector = ent.MultivectorConfig{
		Enabled:      true,
		MuveraConfig: ent.MuveraConfig{Enabled: true, KSim: 2, DProjections: 3, Repetitions: 5},
	}
	return uc
}

func TestUseParallelPrefillRoutingRealIndex(t *testing.T) {
	t.Run("single-vector uncompressed is eligible", func(t *testing.T) {
		idx := newPrefillRoutingIndex(t, "single", prefillRoutingUserConfig(), storeWithObjectsBucket(t))
		require.True(t, idx.useParallelPrefill())
	})

	t.Run("true multivector keeps serial path", func(t *testing.T) {
		uc := prefillRoutingUserConfig()
		uc.Multivector = ent.MultivectorConfig{Enabled: true}
		idx := newPrefillRoutingIndex(t, "multivector", uc, storeWithObjectsBucket(t))
		require.False(t, idx.useParallelPrefill())
	})

	t.Run("muvera keeps serial path", func(t *testing.T) {
		idx := newPrefillRoutingIndex(t, "muvera", muveraUserConfig(), storeWithObjectsBucket(t))
		require.False(t, idx.useParallelPrefill())
	})
}

// TestMuveraSerialPrefillPopulatesCacheRealIndex is the end-to-end guard for the bug
// the parallel path introduced: a muvera index's float32 cache is loaded from the
// dedicated _muvera_vectors bucket, not the objects bucket. After clearing the cache
// (cold-restart shape), the serial prefiller muvera routes to must repopulate it
// fully with the correct muvera-encoded vectors — the parallel path would have left
// it empty while marking it prefilled.
func TestMuveraSerialPrefillPopulatesCacheRealIndex(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()
	store := storeWithObjectsBucket(t)

	idx := newPrefillRoutingIndex(t, "muvera-prefill", muveraUserConfig(), store)

	for i, vec := range multiVectors {
		require.NoError(t, idx.AddMulti(ctx, uint64(i), vec))
	}

	require.False(t, idx.useParallelPrefill(),
		"muvera must route to the serial prefiller, not the objects-bucket scan")

	expected := make(map[uint64][]float32, len(multiVectors))
	for i := range multiVectors {
		v, err := idx.muveraEncoder.GetMuveraVectorForID(uint64(i), idx.id+"_muvera_vectors")
		require.NoError(t, err)
		expected[uint64(i)] = v
	}

	// Cold-restart shape: drop the cache, then run the serial prefiller muvera is
	// routed to and confirm full, correct repopulation.
	idx.cache.Drop()
	require.Equal(t, int64(0), idx.cache.CountVectors())

	require.NoError(t, newVectorCachePrefiller(idx.cache, idx, logger).Prefill(ctx, int(idx.cache.CopyMaxSize())))

	require.Equal(t, int64(len(multiVectors)), idx.cache.CountVectors())
	for id, want := range expected {
		got, err := idx.cache.Get(ctx, id)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}
