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

// AcornFilterRatio is the server default, so an allow list well under 40% of
// the seeded vectors keeps these searches on the ACORN path.
func newSeededIndex(t *testing.T, id string, vectors [][]float32, seed int, filterStrategy string) *hnsw {
	t.Helper()

	ctx := context.Background()
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(ctx) })

	index, err := New(Config{
		RootPath:              t.TempDir(),
		ID:                    id,
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
		FilterStrategy:        filterStrategy,
	}, cyclemanager.NewCallbackGroupNoop(), store)
	require.Nil(t, err)
	t.Cleanup(func() { index.Shutdown(ctx) })

	for id := uint64(0); id < uint64(seed); id++ {
		require.Nil(t, index.Add(ctx, id, vectors[id]))
	}

	return index
}

// Pins the unsynchronized neighbor read of
// https://github.com/weaviate/weaviate/issues/12935. Needs -race: a torn read
// only sometimes panics on its own.
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

	// every tenth id, keeping the allow list under AcornFilterRatio so the
	// search picks ACORN over RRE
	allowed := make([]uint64, 0, seeded/10)
	for id := uint64(0); id < seeded; id += 10 {
		allowed = append(allowed, id)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := newSeededIndex(t, "acorn-neighbor-locking", vectors, seeded, test.filterStrategy)

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

			// Inserts alone only exercise InsertAtLayer. Tombstone cleanup
			// reassigns through ReplaceLayer, the writer that empties a layer
			// and produced the reported panic. Deleted ids stay outside the
			// allow list so the searches keep returning results.
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

// A search decodes connections while holding a vertex: the entrypoint, and each
// second-hop neighbor the ACORN expansion copies. That decode panics on a layer
// whose header claims more entries than its bytes hold — NewWithData accepts one,
// since it only checks that a layer's declared length fits inside the blob. The
// mutex has to survive the panic, or the vertex is locked for good.
func TestSearchUnlocksVertexWhenConnectionDecodePanics(t *testing.T) {
	const size = 200

	tests := []struct {
		name    string
		corrupt func(index *hnsw) (lockedVertex uint64)
	}{
		{
			name: "second-hop neighbor",
			corrupt: func(index *hnsw) uint64 {
				// a vertex on the allow list is taken without decoding, so the
				// corrupt one is kept off it and reached through the entrypoint
				neighbor := uint64(0)
				if index.entryPointID == neighbor {
					neighbor = 1
				}
				index.nodes[index.entryPointID].connections.ReplaceLayer(0, []uint64{neighbor})
				index.nodes[neighbor].connections = packedconn.NewWithData(layerClaimingMoreEntriesThanItStores())
				return neighbor
			},
		},
		{
			name: "entrypoint",
			corrupt: func(index *hnsw) uint64 {
				index.nodes[index.entryPointID].connections = packedconn.NewWithData(layerClaimingMoreEntriesThanItStores())
				return index.entryPointID
			},
		},
	}

	ctx := context.Background()
	vectors, queries := testinghelpers.RandomVecs(size, 1, 8)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := newSeededIndex(t, "acorn-decode-panic", vectors, size, ent.FilterStrategyAcorn)

			// stay on layer 0, so the ACORN-vs-RRE count is the first decode
			// of the entrypoint's connections
			index.currentMaximumLayer = 0
			locked := test.corrupt(index)

			require.Panics(t, func() {
				index.SearchByVector(ctx, queries[0], 10,
					helpers.NewAllowList(index.entryPointID))
			})

			require.True(t, index.nodes[locked].TryLock(),
				"the panic left the vertex locked")
			index.nodes[locked].Unlock()
		})
	}
}

// The blob layout NewWithData parses: a layer count, then per layer a packed
// scheme+count, a data length, and the data.
func layerClaimingMoreEntriesThanItStores() []byte {
	const (
		scheme = 1  // SCHEME_3BYTE
		count  = 10 // ten entries, three bytes each
	)
	packed := uint16(scheme) | uint16(count)<<4

	blob := []byte{1} // layer count
	blob = append(blob, byte(packed), byte(packed>>8))
	blob = append(blob, 3, 0, 0, 0) // data length: 3 bytes where the count claims 30
	blob = append(blob, 0xFF, 0xFF, 0xFF)
	return blob
}

// Pins the entrypoint mutex leak of
// https://github.com/weaviate/weaviate/issues/12935. A commit log holding no
// connections restores a vertex with zero layers, which NewWithData(nil)
// reproduces; the search then blocks on it as soon as it becomes a candidate.
func TestSearchReleasesEntrypointLockWhenEntrypointHasNoLayers(t *testing.T) {
	const size = 200

	ctx := context.Background()
	vectors, queries := testinghelpers.RandomVecs(size, 1, 8)

	index := newSeededIndex(t, "acorn-entrypoint-without-layers", vectors, size, ent.FilterStrategyAcorn)

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
		// with no entrypoint connections, hits can only be the allow list
		// members ACORN seeds the search with
		require.NotEmpty(t, res.ids)
		for _, id := range res.ids {
			require.Contains(t, allowed, id)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("search did not return: the entrypoint vertex mutex was never unlocked")
	}
}
