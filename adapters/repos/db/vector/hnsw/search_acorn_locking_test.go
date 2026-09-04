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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw/packedconn"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// https://github.com/weaviate/weaviate/issues/12935
//
// The ACORN branch expands the neighbors of a candidate's neighbors. It used to
// read those second-hop vertices without holding their mutex, so a write
// running at the same time could update a vertex's connection count and its
// encoded connection bytes between the two loads the reader performs. The
// decode then indexed past the end of the data. Run with -race.
//
// Both writers that mutate a vertex's connections are exercised: inserts append
// through InsertAtLayer, and tombstone cleanup reassigns through ReplaceLayer.
// The reported panic was "index out of range [0] with length 0", which is the
// nil-data state ReplaceLayer leaves behind when a layer is emptied.
func TestSearchConcurrentWithConnectionUpdates(t *testing.T) {
	tests := []struct {
		name           string
		filterStrategy string
	}{
		{name: "acorn", filterStrategy: ent.FilterStrategyAcorn},
		{name: "sweeping", filterStrategy: ent.FilterStrategySweeping},
	}

	const (
		seeded  = 400
		inserts = 400
		readers = 4
		writers = 4
		deletes = 40
	)

	ctx := context.Background()
	vectors, queries := testinghelpers.RandomVecs(seeded+inserts, 1, 8)

	// every tenth seeded id, so the allow list stays well below AcornFilterRatio
	// and the search picks ACORN over RRE
	allowed := make([]uint64, 0, seeded/10)
	for id := uint64(0); id < seeded; id += 10 {
		allowed = append(allowed, id)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := testinghelpers.NewDummyStore(t)
			t.Cleanup(func() { store.Shutdown(ctx) })

			index, err := New(Config{
				RootPath:              t.TempDir(),
				ID:                    "acorn-neighbor-locking",
				MakeCommitLoggerThunk: MakeNoopCommitLogger,
				DistanceProvider:      distancer.NewCosineDistanceProvider(),
				AllocChecker:          memwatch.NewDummyMonitor(),
				VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
					return vectors[int(id)], nil
				},
				GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
				TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
				AcornFilterRatio:             0.4,
			}, ent.UserConfig{
				MaxConnections:        16,
				EFConstruction:        32,
				EF:                    32,
				VectorCacheMaxObjects: 100000,
				FilterStrategy:        test.filterStrategy,
			}, cyclemanager.NewCallbackGroupNoop(), store)
			require.Nil(t, err)
			t.Cleanup(func() { index.Shutdown(ctx) })

			for id := uint64(0); id < seeded; id++ {
				require.Nil(t, index.Add(ctx, id, vectors[id]))
			}

			errs := make(chan error, readers+writers+1)
			stopReaders := make(chan struct{})

			var writersWg sync.WaitGroup
			for w := 0; w < writers; w++ {
				writersWg.Add(1)
				go func(w int) {
					defer writersWg.Done()
					for id := uint64(seeded + w); id < seeded+inserts; id += writers {
						if err := index.Add(ctx, id, vectors[id]); err != nil {
							errs <- err
							return
						}
					}
				}(w)
			}

			// Tombstone cleanup reassigns connections, which is what empties a
			// layer through ReplaceLayer. The deleted ids are outside the allow
			// list so the searches keep returning results.
			writersWg.Add(1)
			go func() {
				defer writersWg.Done()
				for i := 0; i < deletes; i++ {
					id := uint64(i*10 + 1)
					if err := index.Delete(id); err != nil {
						errs <- err
						return
					}
					if err := index.CleanUpTombstonedNodes(neverStop); err != nil {
						errs <- err
						return
					}
				}
			}()

			var readersWg sync.WaitGroup
			for r := 0; r < readers; r++ {
				readersWg.Add(1)
				go func() {
					defer readersWg.Done()
					for {
						select {
						case <-stopReaders:
							return
						default:
						}
						_, _, err := index.SearchByVector(ctx, queries[0], 10,
							helpers.NewAllowList(allowed...))
						if err != nil {
							errs <- err
							return
						}
					}
				}()
			}

			writersWg.Wait()
			close(stopReaders)
			readersWg.Wait()
			close(errs)

			for err := range errs {
				require.NoError(t, err)
			}
		})
	}
}

// https://github.com/weaviate/weaviate/issues/12935
//
// knnSearchByVector locks the entrypoint vertex to measure how many of its
// connections pass the filter. When the entrypoint has no layers there is
// nothing to count, and that branch used to return without unlocking. The
// search then blocked on the same vertex as soon as it became a candidate.
//
// A vertex restored from a commit log that holds no connections has zero
// layers, which is what NewWithData(nil) produces here.
func TestSearchReleasesEntrypointLockWhenEntrypointHasNoLayers(t *testing.T) {
	const size = 200

	ctx := context.Background()
	vectors, queries := testinghelpers.RandomVecs(size, 1, 8)

	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    "acorn-entrypoint-without-layers",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		AllocChecker:          memwatch.NewDummyMonitor(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
		},
		GetViewThunk:                 func() common.BucketView { return &noopBucketView{} },
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		AcornFilterRatio:             0.4,
	}, ent.UserConfig{
		MaxConnections:        16,
		EFConstruction:        16,
		VectorCacheMaxObjects: 100000,
		FilterStrategy:        ent.FilterStrategyAcorn,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	for id := uint64(0); id < size; id++ {
		require.Nil(t, index.Add(ctx, id, vectors[id]))
	}

	index.currentMaximumLayer = 0
	index.nodes[index.entryPointID].connections = packedconn.NewWithData(nil)

	// small enough against the filled vector cache that acorn stays enabled
	allowed := make([]uint64, 0, 10)
	for id := uint64(0); id < 10; id++ {
		allowed = append(allowed, id)
	}

	type searchResult struct {
		ids []uint64
		err error
	}
	done := make(chan searchResult, 1)
	go func() {
		ids, _, err := index.SearchByVector(ctx, queries[0], 10, helpers.NewAllowList(allowed...))
		done <- searchResult{ids: ids, err: err}
	}()

	select {
	case res := <-done:
		require.NoError(t, res.err)
		// the entrypoint has no connections, so everything found comes from the
		// allow list members ACORN seeds the search with
		require.NotEmpty(t, res.ids)
		for _, id := range res.ids {
			require.Contains(t, allowed, id)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("search did not return: the entrypoint vertex mutex was never unlocked")
	}
}
