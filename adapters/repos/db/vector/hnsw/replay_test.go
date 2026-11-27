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
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

// The queue of a shard hands a chunk over again whenever the batch it gave
// did not end well: a task of it failed with a transient error (the whole
// chunk is canceled and read again, with the tasks that had succeeded), the
// flush after it failed, or the process died before the chunk was removed.
// The index is then given vectors it holds, under their ids.
//
// A replay must leave the graph as good as it was: the entrypoint a node of
// the top layer, every id returned once, every vector finding itself, and
// the recall it had. HNSW inserts a replayed id as a new node, with a new
// level, in the place of the one it holds.
func TestAddBatchOfVectorsItHoldsIsAReplay(t *testing.T) {
	const (
		n    = 3000
		dims = 32
		k    = 10
	)
	ctx := context.Background()
	vectors, queries := testinghelpers.RandomVecsFixedSeed(n, 50, dims)
	for i := range vectors {
		vectors[i] = distancer.Normalize(vectors[i])
	}
	for i := range queries {
		queries[i] = distancer.Normalize(queries[i])
	}
	provider := distancer.NewCosineDistanceProvider()
	truth := make([][]uint64, len(queries))
	for q, query := range queries {
		type scored struct {
			id   uint64
			dist float32
		}
		all := make([]scored, n)
		for i, v := range vectors {
			d, err := provider.SingleDist(query, v)
			require.NoError(t, err)
			all[i] = scored{uint64(i), d}
		}
		slices.SortFunc(all, func(a, b scored) int {
			switch {
			case a.dist < b.dist:
				return -1
			case a.dist > b.dist:
				return 1
			}
			return 0
		})
		for _, s := range all[:k] {
			truth[q] = append(truth[q], s.id)
		}
	}

	// A chunk can be handed over again more than once.
	for _, rounds := range []int{1, 4} {
		t.Run(fmt.Sprintf("replayed %d times", rounds), func(t *testing.T) {
			index := createEmptyHnswIndexForTests(t, func(_ context.Context, id uint64) ([]float32, error) {
				return vectors[id], nil
			})
			ids := make([]uint64, n)
			for i := range ids {
				ids[i] = uint64(i)
			}
			require.NoError(t, index.AddBatch(ctx, ids, vectors))

			recall := func(at string) float64 {
				hits := 0
				for q, query := range queries {
					found, _, err := index.SearchByVector(ctx, query, k, nil)
					require.NoError(t, err, "%s: query %d", at, q)
					seen := map[uint64]struct{}{}
					for _, id := range found {
						_, twice := seen[id]
						assert.False(t, twice, "%s: query %d returned id %d twice", at, q, id)
						seen[id] = struct{}{}
						if slices.Contains(truth[q], id) {
							hits++
						}
					}
					assert.Len(t, found, k, "%s: query %d", at, q)
				}
				return float64(hits) / float64(k*len(queries))
			}
			// topLayer returns the level of the node that is the entrypoint, and the
			// top layer of the graph.
			topLayer := func() (entrypoint, top int) {
				index.RLock()
				defer index.RUnlock()
				return int(index.nodes[index.entryPointID].level), index.currentMaximumLayer
			}
			// links counts the links of the bottom layer, those of them that
			// repeat a neighbor of their list, and the links of the upper layers
			// that lead to a node that is not in their layer.
			links := func() (all, repeated, dangling int) {
				index.RLock()
				defer index.RUnlock()
				for _, node := range index.nodes {
					if node == nil {
						continue
					}
					node.Lock()
					seen := map[uint64]struct{}{}
					for _, to := range node.connectionsAtLevelNoLock(0) {
						all++
						if _, twice := seen[to]; twice {
							repeated++
						}
						seen[to] = struct{}{}
					}
					for level := 1; level <= int(node.level); level++ {
						for _, to := range node.connectionsAtLevelNoLock(level) {
							if other := index.nodes[to]; other == nil || int(other.level) < level {
								dangling++
							}
						}
					}
					node.Unlock()
				}
				return all, repeated, dangling
			}
			before := recall("before the replay")
			entrypoint, top := topLayer()
			require.Equal(t, top, entrypoint, "the entrypoint is a node of the top layer")
			require.Positive(t, top, "the graph has more than one layer")
			allBefore, repeatedBefore, danglingBefore := links()
			require.Zero(t, repeatedBefore, "no list repeats a neighbor before the replay")
			require.Zero(t, danglingBefore, "no link of an upper layer leads out of its layer before the replay")

			for round := range rounds {
				require.NoError(t, index.AddBatch(ctx, ids, vectors), "replay %d", round)
			}

			entrypoint, top = topLayer()
			assert.Equal(t, top, entrypoint, "after the replays the entrypoint is a node of level %d, and the graph has "+
				"layers up to %d: the search starts in a node that is not in the top layer", entrypoint, top)
			after := recall("after the replays")
			t.Logf("recall@%d %.4f before, %.4f after %d replays; the entrypoint is of level %d, the top layer %d", k, before,
				after, rounds, entrypoint, top)
			assert.GreaterOrEqual(t, after, before-0.01, "the recall after the replays, against %.4f before", before)
			allAfter, repeated, dangling := links()
			t.Logf("links of the bottom layer: %d before, %d after, %d of them repeat a neighbor of their list; %d links "+
				"of the upper layers lead to a node that is no longer in their layer", allBefore, allAfter, repeated, dangling)
			assert.Zero(t, repeated, "links of the bottom layer that repeat a neighbor of their list")
			assert.Zero(t, dangling, "links of the upper layers that lead to a node that is not in their layer")
			missed := 0
			for i := 0; i < n; i += 7 {
				found, _, err := index.SearchByVector(ctx, vectors[i], 1, nil)
				require.NoError(t, err)
				if len(found) != 1 || found[0] != uint64(i) {
					missed++
				}
			}
			assert.Zero(t, missed, "vectors that do not find themselves after the replays, of %d asked", (n+6)/7)
		})
	}
}
