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

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

					eg := enterrors.NewErrorGroupWrapper(f.cfg.Logger)
					start := make(chan struct{})
					for id := uint64(3); id < 8; id++ {
						eg.Go(func() error {
							<-start
							return f.index.Add(f.ctx, id, f.vectors[id])
						})
					}
					close(start)
					require.NoError(t, eg.Wait())

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

// The delete of a doc can overtake its insert, and the node that goes in then is just
// another tombstone: it must not push a live neighbor out of a full list either.
func TestPruningKeepsLiveNeighborsOverTombstonedInsert(t *testing.T) {
	f := newBootstrapFixture(t, false)
	f.index.maximumConnectionsLayerZero = 2

	// node 3 sits right next to node 0 and takes the first slot, and being closer to
	// node 1 than node 1 is to node 0, it would prune node 1
	place := func(id uint64, position ...float32) {
		vector := make([]float32, len(f.vectors[0]))
		copy(vector, position)
		f.vectors[id] = vector
		f.index.cache.Preload(id, vector)
	}
	place(0)
	place(1, 1)
	place(2, -1)
	place(3, 0.01)

	require.ElementsMatch(t, []uint64{1, 2}, f.index.nodeByID(0).connections.GetLayer(0))

	require.NoError(t, f.index.Delete(3))
	require.NoError(t, f.index.Add(f.ctx, 3, f.vectors[3]))

	require.ElementsMatch(t, []uint64{1, 2}, f.index.nodeByID(0).connections.GetLayer(0))
	f.requireReachable(t, 0, 1, 2)
}

// A node whose delete has overtaken its insert needs no links. With only tombstones
// in reach it must not take over the entrypoint either: nothing would move it again.
func TestTombstonedInsertDoesNotTakeOverEntrypoint(t *testing.T) {
	f := newBootstrapFixture(t, false)
	require.NoError(t, f.index.addTombstone(0, 1, 2))
	entrypoint := f.index.getEntrypoint()

	require.NoError(t, f.index.Delete(3))
	require.NoError(t, f.index.Add(f.ctx, 3, f.vectors[3]))

	require.Equal(t, entrypoint, f.index.getEntrypoint())

	require.NoError(t, f.index.Add(f.ctx, 4, f.vectors[4]))
	f.requireReachable(t, 4)
}

// An insert searches from a local entrypoint it found on its way down. That one can
// sit among tombstones in a split graph while the dead global entrypoint still leads
// to live nodes: only an empty search from the global one means none are in reach.
func TestInsertRetriesFromGlobalEntrypointBeforeTakingOver(t *testing.T) {
	f := newBootstrapFixture(t, false)

	publish := func(id uint64) *vertex {
		node := &vertex{}
		node.connections.GrowLayersTo(0)
		f.index.shardedNodeLocks.Lock(id)
		f.index.nodes[id] = node
		f.index.shardedNodeLocks.Unlock(id)
		f.index.cache.Preload(id, f.vectors[id])
		return node
	}

	// a tombstone with no links, and a dead global entrypoint with live neighbors
	publish(5)
	require.NoError(t, f.index.addTombstone(5, 0))
	require.Equal(t, uint64(0), f.index.getEntrypoint())

	node := publish(3)
	nfc := newNeighborFinderConnector(f.index, 3, node, 5, f.vectors[3], nil, 0, 0,
		helpers.NewAllowList(3), false, nil)
	require.NoError(t, nfc.Do(f.ctx))

	require.Equal(t, uint64(0), f.index.getEntrypoint(), "live nodes are in reach of the entrypoint")
	require.NotEmpty(t, node.connections.GetLayer(0))
	f.requireReachable(t, 1, 2, 3)
}
