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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

func newCleanForReuseTestIndex(t *testing.T, vectors [][]float32) *hnsw {
	t.Helper()
	store := testinghelpers.NewDummyStore(t)
	t.Cleanup(func() { store.Shutdown(context.Background()) })

	index, err := New(Config{
		RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
		ID:                    "clean-for-reuse-test",
		MakeCommitLoggerThunk: MakeNoopCommitLogger,
		DistanceProvider:      distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return vectors[int(id)], nil
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
	t.Cleanup(func() { index.Shutdown(context.Background()) })
	return index
}

// TestCleanForReuse_EntrypointGuard pins the stranded-entrypoint invariant:
// an id must never be reported clean while it is the index's current
// entrypoint in a non-empty graph — even when its node slot is nil and no
// tombstone remains. Historic cleanups could produce exactly that state
// (tombstone dropped, node removed, entrypoint never reassigned); the state
// is constructed directly here so the test does not depend on whether the
// buggy producer (fixed by #13031) is present in the tree.
func TestCleanForReuse_EntrypointGuard(t *testing.T) {
	vectors := [][]float32{{1, 0, 0}, {0, 1, 0}, {0, 0, 1}}

	t.Run("stranded entrypoint is rejected until it moves", func(t *testing.T) {
		index := newCleanForReuseTestIndex(t, vectors)
		for i, vec := range vectors {
			require.NoError(t, index.Add(context.Background(), uint64(i), vec))
		}
		require.Equal(t, uint64(0), index.Entrypoint(), "first insert becomes the entrypoint")

		// Construct the poisoned state directly: node 0's slot nil, no
		// tombstone, but the entrypoint still references it.
		index.Lock()
		index.shardedNodeLocks.Lock(0)
		index.nodes[0] = nil
		index.shardedNodeLocks.Unlock(0)
		index.Unlock()
		require.False(t, index.hasTombstone(0))

		require.False(t, index.CleanForReuse(0),
			"an id that is still the entrypoint of a non-empty graph must not be clean")
		// live nodes are unaffected by the guard
		require.False(t, index.CleanForReuse(1), "live node is not clean")
		require.True(t, index.CleanForReuse(50), "an id never used is clean")

		// Once the entrypoint moves, the same id becomes clean.
		index.Lock()
		index.entryPointID = 1
		index.Unlock()
		require.True(t, index.CleanForReuse(0),
			"after the entrypoint moved on, the cleaned id is reusable")
	})

	t.Run("docID 0 on an empty index is not vetoed by the default entrypoint", func(t *testing.T) {
		index := newCleanForReuseTestIndex(t, vectors)
		// fresh index: no nodes, entryPointID has its zero default
		require.Equal(t, uint64(0), index.Entrypoint())
		require.True(t, index.CleanForReuse(0),
			"an empty index's default entryPointID must not permanently veto docID 0")
	})

	t.Run("delete of the sole node leaves the id clean", func(t *testing.T) {
		index := newCleanForReuseTestIndex(t, vectors)
		require.NoError(t, index.Add(context.Background(), 0, vectors[0]))
		// deleting the only node resets the graph (resetIfOnlyNode): empty
		// again, id 0 clean despite being the (default) entrypoint value
		require.NoError(t, index.Delete(0))
		require.True(t, index.CleanForReuse(0))
	})
}
