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
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// newDynamicAboveThreshold builds an async dynamic index seeded past its upgrade
// threshold, so a subsequent Upgrade() copies enough vectors to overlap a
// concurrent accessor.
func newDynamicAboveThreshold(t *testing.T) (*dynamic, ent.UserConfig, [][]float32) {
	t.Helper()
	ctx := context.Background()
	dimensions := 20
	vectorsSize := 1_000
	threshold := 100

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()

	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	hnswuc := hnswent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
	}
	hnswuc.SetDefaults()

	uc := ent.UserConfig{
		Threshold: uint64(threshold),
		Distance:  dist.Type(),
		HnswUC:    hnswuc,
		FlatUC:    fuc,
	}

	idx, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              t.TempDir(),
		ID:                    "race-uc-test",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      dist,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
		SharedDB:                     db,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true, // required: New() errors otherwise
	}, uc, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)

	compressionhelpers.Concurrently(logger, uint64(vectorsSize), func(i uint64) {
		require.NoError(t, idx.Add(ctx, i, vectors[i]))
	})
	return idx, uc, vectors
}

// UpdateUserConfig writes dynamic.uc while doUpgrade reads it; the write must be
// synchronized. A single continuous writer keeps the reported race unambiguous.
func TestUpdateUserConfig_RacesDoUpgrade(t *testing.T) {
	idx, uc, _ := newDynamicAboveThreshold(t)

	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ef := 0
		for {
			select {
			case <-done:
				return
			default:
				updated := uc
				updated.HnswUC.EF = ef // vary so each call is a real update
				ef++
				_ = idx.UpdateUserConfig(updated, func() {})
			}
		}
	}()
	wg.Wait()
}

// Iterate reads dynamic.index without a lock; it must be synchronized against
// the upgrade swap. Several readers so a read is in flight during the one-shot
// swap; fn returns true to keep each call cheap and the read frequent.
func TestIterate_RacesDoUpgrade(t *testing.T) {
	idx, _, _ := newDynamicAboveThreshold(t)

	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					idx.Iterate(func(id uint64) bool { return true })
				}
			}
		}()
	}
	wg.Wait()
}

// A config update must reach the active index whether it runs before or after
// the upgrade swaps flat->hnsw — not get routed into the wrong index type.
//
// Known gap (not fixed here): an update landing inside doUpgrade's read->swap
// window still applies to the discarded flat index, not the new hnsw one.
func TestUpdateUserConfig_RoutesToActiveIndexType(t *testing.T) {
	const newEF = int64(99)

	t.Run("after upgrade routes HnswUC to the hnsw index", func(t *testing.T) {
		idx, uc, _ := newDynamicAboveThreshold(t)
		require.NotEqual(t, newEF, int64(uc.HnswUC.EF))

		upgradeDone := make(chan struct{})
		require.NoError(t, idx.Upgrade(func() { close(upgradeDone) }))
		<-upgradeDone

		updated := uc
		updated.HnswUC.EF = int(newEF)
		require.NoError(t, idx.UpdateUserConfig(updated, func() {}))

		require.True(t, idx.status.IsUpgraded())
		hnswIdx, ok := idx.index.(interface{ EF() int64 })
		require.True(t, ok, "post-upgrade active index should be *hnsw")
		assert.Equal(t, newEF, hnswIdx.EF(),
			"a post-upgrade config update must be applied to the active HNSW index")
	})

	t.Run("before upgrade the update is carried into the hnsw index", func(t *testing.T) {
		idx, uc, _ := newDynamicAboveThreshold(t)
		require.NotEqual(t, newEF, int64(uc.HnswUC.EF))

		updated := uc
		updated.HnswUC.EF = int(newEF)
		require.NoError(t, idx.UpdateUserConfig(updated, func() {}))

		upgradeDone := make(chan struct{})
		require.NoError(t, idx.Upgrade(func() { close(upgradeDone) }))
		<-upgradeDone

		require.True(t, idx.status.IsUpgraded())
		hnswIdx, ok := idx.index.(interface{ EF() int64 })
		require.True(t, ok, "post-upgrade active index should be *hnsw")
		assert.Equal(t, newEF, hnswIdx.EF(),
			"a config update applied before the upgrade must be carried into the new HNSW index via uc")
	})
}
