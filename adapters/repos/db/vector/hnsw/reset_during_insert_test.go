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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/cache"
)

// TestInsert_CleanupEmptiesGraphDuringInsert: after a crash between the last
// tombstone and the reset, the restarted graph holds only tombstoned nodes.
// Cleanup removes them and resets the graph to its initial size while an
// insert with a high id, which grew the node slice already, is still picking
// its entrypoint. The insert must not index the shrunk slice.
func TestInsert_CleanupEmptiesGraphDuringInsert(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncompressed", true: "rq8"}[compressed], func(t *testing.T) {
			f := newBootstrapFixtureWith(t, compressed, 8, 2)
			require.NoError(t, f.index.Delete(0))
			require.Equal(t, uint64(1), f.index.getEntrypoint())
			f.commitLog.dropReset.Store(true)
			require.NoError(t, f.index.Delete(1))
			f.commitLog.dropReset.Store(false)
			f.restart(t)
			require.Equal(t, uint64(1), f.index.getEntrypoint())
			require.True(t, f.index.hasTombstone(0))
			require.True(t, f.index.hasTombstone(1))

			id := uint64(cache.InitialSize + 500)
			f.vectors = append(f.vectors, make([][]float32, int(id)-len(f.vectors)+1)...)
			f.vectors[id] = f.vectors[1]
			f.evict(1)
			cleanupDone := make(chan error, 1)
			f.setVectorHook(1, func() {
				// the insert has grown the node slice and is checking the entrypoint
				f.index.RLock()
				require.Greater(t, len(f.index.nodes), int(id))
				f.index.RUnlock()

				go func() { cleanupDone <- f.index.CleanUpTombstonedNodes(neverStop) }()
				require.Eventually(t, func() bool {
					return f.index.nodeByID(0) == nil && f.index.nodeByID(1) == nil
				}, 10*time.Second, time.Millisecond)
				// give the reset a chance to shrink the slice; it must not
				time.Sleep(200 * time.Millisecond)
				// the entrypoint's object is gone too, so the insert bootstraps
				f.deleteFromObjectStore(1)
			})
			require.NoError(t, f.index.Add(f.ctx, id, f.vectors[id]))

			select {
			case err := <-cleanupDone:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("cleanup did not finish")
			}
			require.Equal(t, id, f.index.getEntrypoint())
			require.NotNil(t, f.index.nodeByID(id))
			require.False(t, f.index.hasTombstone(id))
			f.requireReachable(t, id)

			f.restart(t)
			require.Equal(t, id, f.index.getEntrypoint())
			f.requireReachable(t, id)
		})
	}
}

// TestInsert_GrowsShrunkNodeSlice: a reset between a batch growing the node
// slice and one of its inserts taking the insert lock leaves the slice too
// small for the id; the insert grows it again
func TestInsert_GrowsShrunkNodeSlice(t *testing.T) {
	f := newBootstrapFixtureWith(t, false, 8, 1)
	id := uint64(cache.InitialSize + 500)
	f.index.RLock()
	require.LessOrEqual(t, len(f.index.nodes), int(id))
	f.index.RUnlock()

	node := &vertex{level: 0}
	node.connections.GrowLayersTo(0)
	f.vectors = append(f.vectors, make([][]float32, int(id)-len(f.vectors)+1)...)
	f.vectors[id] = f.vectors[1]
	require.NoError(t, f.index.addOne(f.ctx, id, f.vectors[id], node))
	require.NotNil(t, f.index.nodeByID(id))
	f.requireReachable(t, 0, id)
}
