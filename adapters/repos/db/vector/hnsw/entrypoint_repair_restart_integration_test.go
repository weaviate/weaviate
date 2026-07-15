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
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func (s *externalDeleteStore) deleteOne(id uint64) {
	s.Lock()
	defer s.Unlock()
	delete(s.objects, id)
}

// TestEntrypointRepair_DeletedEntrypoints covers entrypoint repair for
// uncompressed and RQ-compressed (8 bit) indexes when the entrypoint's object
// was deleted from the object store without the index being told:
//  1. delete only the entrypoint, restart, query → repaired to a live node
//  2. delete everything, restart, query → empty result, no error
//  3. delete the entrypoint and evict it from the caches without a restart,
//     query → repaired to a live node
func TestEntrypointRepair_DeletedEntrypoints(t *testing.T) {
	const (
		numVectors = 100
		dims       = 32
		k          = 10
	)

	logger, _ := test.NewNullLogger()
	vectors, queries := testinghelpers.RandomVecs(numVectors, 1, dims)
	query := queries[0]

	variants := []struct {
		name       string
		compressed bool
	}{
		{name: "uncompressed", compressed: false},
		{name: "rq8", compressed: true},
	}

	newIndex := func(t *testing.T, dirName, indexID string,
		store *externalDeleteStore, dummyStore *lsmkv.Store, compressed bool,
	) *hnsw {
		uc := ent.UserConfig{}
		uc.SetDefaults()
		if compressed {
			uc.RQ = ent.RQConfig{Enabled: true, Bits: 8}
		} else {
			uc.RQ.Enabled = false
			uc.SkipDefaultQuantization = true
		}

		idx, err := New(Config{
			AllocChecker: memwatch.NewDummyMonitor(),
			RootPath:     dirName,
			ID:           indexID,
			Logger:       logger,
			MakeCommitLoggerThunk: func() (CommitLogger, error) {
				return NewCommitLogger(dirName, indexID, logger,
					cyclemanager.NewCallbackGroupNoop())
			},
			DistanceProvider:  distancer.NewL2SquaredProvider(),
			VectorForIDThunk:  store.vectorForID,
			GetViewThunk:      func() common.BucketView { return &searchRepairNoopBucketView{} },
			MakeBucketOptions: lsmkv.MakeNoopBucketOptions,
		}, uc, cyclemanager.NewCallbackGroupNoop(), dummyStore)
		require.NoError(t, err)
		idx.PostStartup(context.Background())
		return idx
	}

	populate := func(t *testing.T, ctx context.Context, idx *hnsw,
		store *externalDeleteStore, compressed bool,
	) {
		for i := 0; i < numVectors; i++ {
			store.put(uint64(i), vectors[i])
			require.NoError(t, idx.Add(ctx, uint64(i), vectors[i]))
		}
		require.Equal(t, compressed, idx.Compressed())
	}

	// a hung repair loop should fail the test instead of blocking it forever
	search := func(t *testing.T, idx *hnsw) ([]uint64, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		ids, _, err := idx.SearchByVector(ctx, query, k, nil)
		require.NoError(t, ctx.Err(), "search did not terminate — repair loop is stuck")
		return ids, err
	}

	// removes the id from everything except the index itself: the raw object
	// store and, for compressed indexes, the compressed bucket and cache
	deleteExternally := func(ctx context.Context, idx *hnsw,
		store *externalDeleteStore, id uint64,
	) {
		store.deleteOne(id)
		if idx.Compressed() {
			idx.compressor.Delete(ctx, id)
		}
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			t.Run("delete entrypoint, restart, query repairs entrypoint", func(t *testing.T) {
				ctx := context.Background()
				dirName := t.TempDir()
				indexID := "entrypoint-repair-restart-" + variant.name
				dummyStore := testinghelpers.NewDummyStore(t)
				defer dummyStore.Shutdown(ctx)
				store := &externalDeleteStore{objects: map[uint64][]float32{}}

				idx := newIndex(t, dirName, indexID, store, dummyStore, variant.compressed)
				populate(t, ctx, idx, store, variant.compressed)

				oldEP := idx.entryPointID
				deleteExternally(ctx, idx, store, oldEP)

				require.NoError(t, idx.Flush())
				require.NoError(t, idx.Shutdown(ctx))
				idx = newIndex(t, dirName, indexID, store, dummyStore, variant.compressed)
				defer idx.Shutdown(ctx)
				require.Equal(t, variant.compressed, idx.Compressed())
				require.Equal(t, oldEP, idx.entryPointID,
					"restart should restore the stale entrypoint from the commit log")

				ids, err := search(t, idx)
				require.NoError(t, err, "first search after restart must repair the entrypoint")
				require.NotEmpty(t, ids)
				require.NotContains(t, ids, oldEP, "results must not contain the deleted node")

				newEP := idx.entryPointID
				require.NotEqual(t, oldEP, newEP, "a new entrypoint must have been selected")
				_, err = store.vectorForID(ctx, newEP)
				require.NoError(t, err, "new entrypoint must be a live node")
				require.True(t, idx.hasTombstone(oldEP),
					"deleted entrypoint must be flagged for cleanup")
			})

			t.Run("delete all vectors, restart, query returns empty without error", func(t *testing.T) {
				ctx := context.Background()
				dirName := t.TempDir()
				indexID := "entrypoint-repair-all-dead-" + variant.name
				dummyStore := testinghelpers.NewDummyStore(t)
				defer dummyStore.Shutdown(ctx)
				store := &externalDeleteStore{objects: map[uint64][]float32{}}

				idx := newIndex(t, dirName, indexID, store, dummyStore, variant.compressed)
				populate(t, ctx, idx, store, variant.compressed)

				for i := 0; i < numVectors; i++ {
					deleteExternally(ctx, idx, store, uint64(i))
				}

				require.NoError(t, idx.Flush())
				require.NoError(t, idx.Shutdown(ctx))
				idx = newIndex(t, dirName, indexID, store, dummyStore, variant.compressed)
				defer idx.Shutdown(ctx)

				// twice: covers the initial repair chain and the already-exhausted state
				for i := 0; i < 2; i++ {
					ids, err := search(t, idx)
					require.NoError(t, err, "search %d over a fully deleted graph must not error", i)
					require.Empty(t, ids)
				}
			})

			t.Run("immediate entrypoint deletion without restart repairs entrypoint", func(t *testing.T) {
				ctx := context.Background()
				dirName := t.TempDir()
				indexID := "entrypoint-repair-immediate-" + variant.name
				dummyStore := testinghelpers.NewDummyStore(t)
				defer dummyStore.Shutdown(ctx)
				store := &externalDeleteStore{objects: map[uint64][]float32{}}

				idx := newIndex(t, dirName, indexID, store, dummyStore, variant.compressed)
				defer idx.Shutdown(ctx)
				populate(t, ctx, idx, store, variant.compressed)

				oldEP := idx.entryPointID
				deleteExternally(ctx, idx, store, oldEP)
				if !variant.compressed {
					// the raw vector cache would otherwise mask the deletion
					idx.cache.Delete(ctx, oldEP)
				}

				ids, err := search(t, idx)
				require.NoError(t, err, "search must repair the entrypoint")
				require.NotEmpty(t, ids)
				require.NotContains(t, ids, oldEP, "results must not contain the deleted node")

				newEP := idx.entryPointID
				require.NotEqual(t, oldEP, newEP, "a new entrypoint must have been selected")
				_, err = store.vectorForID(ctx, newEP)
				require.NoError(t, err, "new entrypoint must be a live node")
				require.True(t, idx.hasTombstone(oldEP),
					"deleted entrypoint must be flagged for cleanup")
			})
		})
	}
}
