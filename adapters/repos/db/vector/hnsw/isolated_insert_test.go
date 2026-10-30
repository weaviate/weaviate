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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// An insert only ever links to live nodes: search drops tombstones from its results
// and skipNeighbor refuses them. With nothing but tombstones in reach the insert
// used to end up without a single edge and without an error, and tombstone cleanup
// never looks at such a node, because none of its neighbors has a tombstone. Search
// then found the entrypoint at most, whatever the limit: issue #13609.
func TestInsertWithOnlyTombstonesInReach(t *testing.T) {
	variants := []struct {
		name       string
		compressed bool
	}{
		{name: "uncompressed", compressed: false},
		{name: "rq8", compressed: true},
	}

	// tombstone without Delete, which would reset the emptied graph: this is the
	// state concurrent updates leave behind, a dead entrypoint and nothing live
	// reachable from it
	onlyTombstones := func(t *testing.T, f *bootstrapFixture) {
		require.NoError(t, f.index.addTombstone(0, 1, 2))
	}

	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			t.Run("inserts stay reachable", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				onlyTombstones(t, f)

				require.NoError(t, f.index.Add(f.ctx, 3, f.vectors[3]))
				f.requireReachable(t, 3)
				require.False(t, f.index.HasGhost(3))

				require.NoError(t, f.index.Add(f.ctx, 4, f.vectors[4]))
				f.requireReachable(t, 3, 4)
				require.False(t, f.index.HasGhost(4))
			})

			t.Run("inserts stay reachable after cleanup and restart", func(t *testing.T) {
				f := newBootstrapFixture(t, variant.compressed)
				onlyTombstones(t, f)

				for _, id := range []uint64{3, 4, 5} {
					require.NoError(t, f.index.Add(f.ctx, id, f.vectors[id]))
				}

				require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))
				f.requireReachable(t, 3, 4, 5)

				f.restart(t)
				f.requireReachable(t, 3, 4, 5)
			})

			t.Run("concurrent inserts all stay reachable", func(t *testing.T) {
				for range 50 {
					f := newBootstrapFixture(t, variant.compressed)
					onlyTombstones(t, f)

					var wg sync.WaitGroup
					start := make(chan struct{})
					for id := uint64(3); id < 8; id++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							<-start
							require.NoError(t, f.index.Add(f.ctx, id, f.vectors[id]))
						}()
					}
					close(start)
					wg.Wait()

					f.requireReachable(t, 3, 4, 5, 6, 7)
				}
			})
		})
	}
}

// A delete can overtake the insert of the same doc ID, two concurrent updates of an
// object do that. The tombstone stays, so the node must not become the entrypoint
// when it draws a high level: Delete only moves an entrypoint it is deleting, and
// this one was deleted before it was one.
func TestTombstonedNodeDoesNotBecomeEntrypoint(t *testing.T) {
	f := newBootstrapFixture(t, false)
	entrypoint := f.index.getEntrypoint()

	require.NoError(t, f.index.Delete(5))
	f.index.randFunc = func() float64 { return 0.0001 }
	require.NoError(t, f.index.Add(f.ctx, 5, f.vectors[5]))

	require.Equal(t, entrypoint, f.index.getEntrypoint())
	f.requireReachable(t, 0, 1, 2)
}

// A full neighbor list gets pruned, and a tombstone must not win a slot from a live
// node: the links of a node can then be tombstones only, while every live node of
// the graph points at it. With that node as the entrypoint, search finds nothing else.
func TestPruningKeepsLiveNeighborsOverTombstones(t *testing.T) {
	f := newBootstrapFixture(t, false)
	f.index.maximumConnectionsLayerZero = 2

	// node 1 and 2 sit on opposite sides of node 0, so neither prunes the other, and
	// the closer they are, the earlier the heuristic takes them: node 3 comes last
	place := func(id uint64, position ...float32) {
		vector := make([]float32, len(f.vectors[0]))
		copy(vector, position)
		f.vectors[id] = vector
		f.index.cache.Preload(id, vector)
	}
	place(0)
	place(1, 1)
	place(2, -1)
	place(3, 0, 100)

	require.NoError(t, f.index.addTombstone(1, 2))
	require.Equal(t, uint64(0), f.index.getEntrypoint())
	require.ElementsMatch(t, []uint64{1, 2}, f.index.nodeByID(0).connections.GetLayer(0))

	require.NoError(t, f.index.Add(f.ctx, 3, f.vectors[3]))

	connections := f.index.nodeByID(0).connections.GetLayer(0)
	require.Contains(t, connections, uint64(3))
	require.Len(t, connections, 2, "the spare slot stays with a tombstone, it may be a bridge")
	f.requireReachable(t, 0, 3)
}
