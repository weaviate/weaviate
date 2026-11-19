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
)

// randForLevel returns a randFunc value that makes generateLevel return level
func randForLevel(t *testing.T, index *hnsw, level uint8) float64 {
	t.Helper()
	for r := 1.0; r > 1e-6; r *= 0.9 {
		index.randFunc = func() float64 { return r }
		if index.generateLevel() == level {
			return r
		}
	}
	t.Fatalf("no rand value yields level %d", level)
	return 0
}

// TestInsert_NeighborAlreadyLinkedBack: node 2 joins while node 1 is still
// being inserted and links to it, because 1 is the only usable entrypoint
// left. When 1 then searches its bottom layer from 2, the search returns 1
// itself through that backlink. 1 must not link to itself and must not be
// appended to 2 a second time.
func TestInsert_NeighborAlreadyLinkedBack(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncompressed", true: "rq8"}[compressed], func(t *testing.T) {
			// the first node is always on layer 0; 3 and 4 are on layer 1, linked
			// there, and 3 is the entrypoint
			f := newBootstrapFixtureWith(t, compressed, 8, 1)
			level1 := randForLevel(t, f.index, 1)
			f.index.randFunc = func() float64 { return level1 }
			require.NoError(t, f.index.Add(f.ctx, 3, f.vectors[3]))
			require.NoError(t, f.index.Add(f.ctx, 4, f.vectors[4]))
			require.Equal(t, []uint64{4}, f.layer(3, 1))
			require.Equal(t, uint64(3), f.index.getEntrypoint())

			// 1 reads 4's vector while searching layer 1; by then 1 has joined the
			// graph but has no links at layer 0 yet
			f.evict(4)
			f.setVectorHook(4, func() {
				require.Equal(t, uint64(3), f.index.getEntrypoint())
				require.True(t, f.index.nodeByID(1).isUnderMaintenance())
				require.Empty(t, f.layer(1, 0))

				// every other node loses its object, so 2 (on layer 0) can only
				// start from 1, which becomes the global entrypoint; 2 links to 1
				// and 1 to 2
				f.deleteFromObjectStore(0)
				f.deleteFromObjectStore(3)
				f.deleteFromObjectStore(4)
				f.index.randFunc = func() float64 { return 1 }
				require.NoError(t, f.index.Add(f.ctx, 2, f.vectors[2]))
				require.Equal(t, uint64(1), f.index.getEntrypoint())
				require.Equal(t, []uint64{1}, f.layer(2, 0))
				require.Equal(t, []uint64{2}, f.layer(1, 0))

				// cleanup removes the tombstoned nodes, so 1's layer-0 entrypoint
				// (3, its layer-1 neighbor) is gone and the repair hands it 2
				require.NoError(t, f.index.CleanUpTombstonedNodes(neverStop))
				require.Nil(t, f.index.nodeByID(0))
				require.Nil(t, f.index.nodeByID(3))
				require.Nil(t, f.index.nodeByID(4))
			})
			require.NoError(t, f.index.Add(f.ctx, 1, f.vectors[1]))

			require.Equal(t, []uint64{2}, f.layer(1, 0), "node 1 links to itself")
			require.Equal(t, []uint64{1}, f.layer(2, 0), "node 2 links to 1 twice")
			require.Equal(t, uint64(1), f.index.getEntrypoint())
			f.requireLayersWellFormed(t)
			f.requireReachable(t, 1, 2)

			f.restart(t)
			f.requireLayersWellFormed(t)
			f.requireReachable(t, 1, 2)
		})
	}
}

// TestInsert_SelfInSearchResultsSkipsSelf: node 40 is linked to by node x
// before it is inserted, so its own search returns it. With enough results
// for the heuristic to run, the node must not be a candidate against which
// the others are pruned: the layer equals the one it gets without the
// backlink.
func TestInsert_SelfInSearchResultsSkipsSelf(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "uncompressed", true: "rq8"}[compressed], func(t *testing.T) {
			const nodes, id = 40, uint64(40)
			control := newBootstrapFixtureWith(t, compressed, 4, nodes)
			require.NoError(t, control.index.Add(control.ctx, id, control.vectors[id]))
			want := control.layer(id, 0)
			require.NotEmpty(t, want)

			f := newBootstrapFixtureWith(t, compressed, 4, nodes)
			var x uint64
			for x = range uint64(nodes) {
				if len(f.layer(x, 0)) < f.index.maximumConnectionsLayerZero && !containsID(want, x) {
					break
				}
			}
			require.Less(t, x, uint64(nodes), "no node with room that is not a neighbor of %d", id)
			f.leaveGhost(t, id)
			node := f.index.nodeByID(x)
			node.Lock()
			node.appendConnectionAtLevelNoLock(0, id, f.index.maximumConnectionsLayerZero)
			node.Unlock()
			require.NoError(t, f.index.commitLog.AddLinkAtLevel(x, 0, id))

			require.NoError(t, f.index.Add(f.ctx, id, f.vectors[id]))
			require.ElementsMatch(t, want, f.layer(id, 0))
			require.Contains(t, f.layer(x, 0), id)
			f.requireLayersWellFormed(t)
		})
	}
}
