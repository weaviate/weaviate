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

package postingedge

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
)

// heldCentroids counts the centroid vectors the index holds in float32, and
// their bytes.
func heldCentroids(c *centroidIndex) (n, bytes int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, v := range c.vectors {
		if v != nil {
			n++
			bytes += 4 * len(v)
		}
	}
	return n, bytes
}

// livePostings counts the postings of the index that are not retired.
func livePostings(g *growIndex) int {
	g.retiredMu.Lock()
	defer g.retiredMu.Unlock()
	live := 0
	for _, retired := range g.retired {
		if !retired {
			live++
		}
	}
	return live
}

// graphNodes counts the nodes of the centroid graph and its tombstones.
func graphNodes(t *testing.T, c *centroidIndex) (nodes, tombstones int) {
	t.Helper()
	stats, err := c.graph.Stats()
	require.NoError(t, err)
	for _, n := range stats.DistributionLayers {
		nodes += int(n)
	}
	return nodes, stats.NumTombstones
}

// The memory of the centroids follows the live postings, not every posting
// there ever was. A posting that splits retires, and its centroid goes with
// it: an index grown by many splits holds one centroid per live posting, and
// so does the same index opened again. In float32 that is one vector each in
// memory. With codes, as HFresh keeps its own, nothing is in float32 and the
// graph holds the code of every node. Either way the graph keeps nothing of a
// retired centroid but its tombstone, which its cleanup takes out of the
// graph, of its cache and of the bucket of its codes.
func TestPersistentCentroidsFollowTheLivePostings(t *testing.T) {
	for _, float32Centroids := range []bool{true, false} {
		for _, cacheMode := range cacheModes {
			t.Run(fmt.Sprintf("%s, centroids in float32 %v", cacheMode, float32Centroids), func(t *testing.T) {
				const total = 6000
				f := newPersistentFixtureMode(t, total, cacheMode, func(cfg *PersistentConfig) {
					cfg.float32Centroids = float32Centroids
				})
				f.add(t, 0, total)
				check := func(at string) int {
					f.index.mu.RLock()
					defer f.index.mu.RUnlock()
					g := f.index.g
					live := livePostings(g)
					require.Greater(t, g.meta.Splits, 10, "%s: the index split many times", at)
					require.Greater(t, g.meta.Postings, live, "%s: some postings retired", at)
					centroids := g.st.centroids
					held, bytes := heldCentroids(centroids)
					if float32Centroids {
						assert.Equal(t, live, held, "%s: the centroid vectors held, against %d live postings of the %d given out",
							at, live, g.meta.Postings)
						assert.Equal(t, 4*live*g.meta.Dims, bytes, "%s: the bytes of the centroid vectors held", at)
					} else {
						assert.True(t, centroids.codes, "%s: the centroids are codes", at)
						assert.True(t, centroids.graph.Compressed(), "%s: the graph searches with the codes", at)
						assert.Zero(t, held, "%s: no centroid is held in float32", at)
					}
					return live
				}
				check("grown")
				f.restart(t)
				live := check("opened again")

				graph := f.index.g.st.centroids.graph
				require.NoError(t, graph.CleanUpTombstonedNodes(func() bool { return false }))
				nodes, tombstones := graphNodes(t, f.index.g.st.centroids)
				assert.Zero(t, tombstones, "the graph keeps no tombstone once it is cleaned")
				assert.Equal(t, live, nodes, "the graph keeps one node per live posting once it is cleaned")
			})
		}
	}
}

// An index whose centroid graph holds the centroids in float32, written by a
// build or by a version of the index before codes, converts when it opens: the
// graph compresses them into codes, logs that it did, and lets the float32 go.
// It searches as well as it did, and opens with its codes from then on. The
// file of the centroids stays as it was, in float32, for the graph to be built
// again from it.
func TestPersistentCentroidsConvertToCodes(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) {
			const total, grown, k = 4000, 5000, 10
			f := newPersistentFixtureMode(t, grown, cacheMode, func(cfg *PersistentConfig) {
				cfg.float32Centroids = true
			})
			f.add(t, 0, total)
			before := f.recall(t, total, k, nil)
			require.Greater(t, before, 0.9, "the recall in float32")
			centroidFile := filepath.Join(f.index.g.dir, diskCentroidFile)
			info, err := os.Stat(centroidFile)
			require.NoError(t, err)
			_, err = os.Stat(filepath.Join(f.root, "lsm", helpers.PostingEdgeCentroidCodesBucketName(f.cfg.ID)))
			require.True(t, os.IsNotExist(err), "no bucket of codes before the conversion")

			f.close(t)
			f.cfg.float32Centroids = false
			f.open(t)
			check := func(at string, n int) {
				centroids := f.index.g.st.centroids
				require.True(t, centroids.codes, at)
				require.True(t, centroids.graph.Compressed(), "%s: the graph searches with the codes", at)
				held, _ := heldCentroids(centroids)
				require.Zero(t, held, "%s: no centroid is held in float32", at)
				nodes, tombstones := graphNodes(t, centroids)
				assert.Equal(t, livePostings(f.index.g), nodes-tombstones, "%s: the graph holds the live postings", at)
				after := f.recall(t, n, k, nil)
				assert.Greater(t, after, 0.9, "%s: the recall with codes", at)
				if n == total {
					assert.InDelta(t, before, after, 0.02, "%s: the recall with codes, against %.4f in float32", at, before)
				}
			}
			check("converted", total)
			again, err := os.Stat(centroidFile)
			require.NoError(t, err)
			assert.Equal(t, info.Size(), again.Size(), "the file of the centroids stays as it was")
			f.restart(t)
			check("opened again with codes", total)
			// And it grows with codes: its postings split, and the centroids
			// of the halves go to the file and to the graph as codes.
			splits := f.index.g.meta.Splits
			f.add(t, total, grown)
			require.Greater(t, f.index.g.meta.Splits, splits, "the index split with codes")
			check("grown with codes", grown)
			f.restart(t)
			check("grown with codes, opened again", grown)
		})
	}
}
