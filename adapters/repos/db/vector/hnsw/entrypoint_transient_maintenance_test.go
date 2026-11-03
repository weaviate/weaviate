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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// In a tiny graph every live node can be transiently unusable as an
// entrypoint: under maintenance by a concurrent insert or by tombstone
// cleanup, or with its vector gone between the object store delete and the
// index delete. The async queue discards a batch on a permanent error, so the
// insert must fail with a transient one and succeed when retried after the
// state clears.
func TestPickEntrypoint_TransientMaintenanceIsRetryable(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}, {0.7, 0.8, 0.9}, {0.5, 0.5, 0.5}}

	type fixture struct {
		index      *hnsw
		goneVector *atomic.Int64
	}

	newFixture := func(t *testing.T) fixture {
		goneVector := &atomic.Int64{}
		goneVector.Store(-1)
		store := testinghelpers.NewDummyStore(t)
		t.Cleanup(func() { store.Shutdown(ctx) })
		index, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "entrypoint-transient-maintenance",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				if int64(id) == goneVector.Load() {
					return nil, storobj.NewErrNotFoundf(id, "deleted from object store")
				}
				return vectors[id], nil
			},
			GetViewThunk:                 GetViewThunk,
			TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
			AllocChecker:                 memwatch.NewDummyMonitor(),
		}, ent.UserConfig{
			MaxConnections:        30,
			EFConstruction:        128,
			VectorCacheMaxObjects: 100000,
		}, cyclemanager.NewCallbackGroupNoop(), store)
		require.NoError(t, err)
		t.Cleanup(func() { index.Drop(ctx, false) })
		// every node on level 0 so the scenario does not depend on random levels
		index.randFunc = func() float64 { return 1 }
		for i := 0; i < 3; i++ {
			require.NoError(t, index.Add(ctx, uint64(i), vectors[i]))
		}
		return fixture{index: index, goneVector: goneVector}
	}

	// the retried insert must land the node in the graph, reachable by search
	requireRetrySucceeds := func(t *testing.T, index *hnsw) {
		require.NoError(t, index.Add(ctx, 3, vectors[3]))
		ids, _, err := index.SearchByVector(ctx, vectors[3], 4, nil)
		require.NoError(t, err)
		require.Contains(t, ids, uint64(3))
	}

	t.Run("all live nodes under maintenance", func(t *testing.T) {
		f := newFixture(t)
		for i := 0; i < 3; i++ {
			f.index.nodeByID(uint64(i)).markAsMaintenance()
		}

		err := f.index.Add(ctx, 3, vectors[3])
		require.ErrorIs(t, err, enterrors.ErrNoUsableEntrypoint)
		require.True(t, enterrors.IsTransient(err), "queue must retry, not discard: %v", err)

		for i := 0; i < 3; i++ {
			f.index.nodeByID(uint64(i)).unmarkAsMaintenance()
		}
		requireRetrySucceeds(t, f.index)
	})

	t.Run("exhausted local fallback", func(t *testing.T) {
		f := newFixture(t)
		ep := f.index.entryPointID
		var gone, other uint64
		for i := uint64(0); i < 3; i++ {
			if i != ep && i > gone {
				gone = i
			}
		}
		for i := uint64(0); i < 3; i++ {
			if i != ep && i != gone {
				other = i
			}
		}
		// the entrypoint is being rewired by tombstone cleanup
		f.index.nodeByID(ep).markAsMaintenance()
		// the node the global repair picks lost its object between the object
		// store delete and the vector index delete
		f.goneVector.Store(int64(gone))
		f.index.cache.Delete(ctx, gone)
		// the remaining live node is being rewired by a cleanup worker
		f.index.nodeByID(other).markAsMaintenance()

		err := f.index.Add(ctx, 3, vectors[3])
		require.ErrorIs(t, err, enterrors.ErrNoUsableEntrypoint)
		require.ErrorContains(t, err, "local fallback exhausted")
		require.True(t, enterrors.IsTransient(err), "queue must retry, not discard: %v", err)

		f.index.nodeByID(ep).unmarkAsMaintenance()
		f.index.nodeByID(other).unmarkAsMaintenance()
		requireRetrySucceeds(t, f.index)
	})
}
