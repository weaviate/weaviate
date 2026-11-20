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

package dynamic

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// A dynamic index answers a search by distance with the index it holds, and
// holds its read lock while it does. While it was flat, the search returned
// its first page (100 results) and no more, and with no maximum it never
// returned and never let go of the lock. It is checked before and after the
// upgrade to HNSW against a search that asks for more than the index holds.
func TestDynamicSearchByVectorDistance(t *testing.T) {
	const (
		vectors = 1000
		dim     = 20
		page    = common.DefaultSearchByDistInitialLimit
		// the first page and the one after it, which holds every vector
		secondPage = page + page*common.DefaultSearchByDistLimitMultiplier
		// farther than any vector
		anyDistance = float32(1e9)
	)
	ctx := context.Background()

	meta, err := shardmeta.Open(t.TempDir(), time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	stored, probes := testinghelpers.RandomVecs(vectors, 5, dim)
	provider := distancer.NewL2SquaredProvider()
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	index, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              t.TempDir(),
		ID:                    "search-by-distance-test",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      provider,
		VectorForIDThunk: func(_ context.Context, id uint64) ([]float32, error) {
			return stored[id], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(stored),
		TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
		State:                        meta.Namespace(StateNamespace),
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true,
	}, ent.UserConfig{
		Threshold: vectors,
		Distance:  provider.Type(),
		HnswUC: hnswent.UserConfig{
			MaxConnections:        30,
			EFConstruction:        64,
			EF:                    32,
			VectorCacheMaxObjects: 1_000_000,
		},
		FlatUC: fuc,
	}, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	for i, vector := range stored {
		require.NoError(t, index.Add(ctx, uint64(i), vector))
	}

	check := func(t *testing.T) {
		for _, probe := range probes {
			want, wantDists, err := index.SearchByVector(ctx, probe, secondPage, nil)
			require.NoError(t, err)
			require.Greater(t, len(want), page, "the index has to answer with more than a page")

			for _, maxLimit := range []int64{10_000, -1} {
				var (
					ids   []uint64
					dists []float32
					err   error
				)
				done := make(chan struct{})
				enterrors.GoWrapper(func() {
					defer close(done)
					ids, dists, err = index.SearchByVectorDistance(ctx, probe, anyDistance, maxLimit, nil)
				}, logger)
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					t.Fatalf("the search by distance with the maximum %d did not return", maxLimit)
				}
				require.NoError(t, err)
				require.Equal(t, want, ids, "with the maximum %d", maxLimit)
				require.Equal(t, wantDists, dists, "with the maximum %d", maxLimit)
			}
		}
	}

	t.Run("flat", func(t *testing.T) {
		require.False(t, index.status.IsUpgraded())
		check(t)
	})

	t.Run("upgraded to hnsw", func(t *testing.T) {
		var upgraded sync.WaitGroup
		upgraded.Add(1)
		require.NoError(t, index.Upgrade(upgraded.Done))
		upgraded.Wait()
		require.True(t, index.status.IsUpgraded())
		check(t)
	})
}
