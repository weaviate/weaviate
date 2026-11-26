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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldCentroids counts the centroid vectors the index holds, and their bytes.
func heldCentroids(c *centroidIndex) (n, bytes int) {
	for _, v := range c.vectors {
		if v != nil {
			n++
			bytes += 4 * len(v)
		}
	}
	return n, bytes
}

// The memory of the centroids follows the live postings, not every posting
// there ever was. A posting that splits retires, and its centroid goes with
// it: an index grown by many splits holds one centroid vector per live
// posting, and so does the same index opened again. The graph over the
// centroids keeps nothing of a retired one but its tombstone, which its
// cleanup takes out.
func TestPersistentCentroidsFollowTheLivePostings(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) {
			const total = 6000
			f := newPersistentFixtureMode(t, total, cacheMode, nil)
			f.add(t, 0, total)
			livePostings := func() int {
				g := f.index.g
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
			check := func(at string) int {
				f.index.mu.RLock()
				defer f.index.mu.RUnlock()
				g := f.index.g
				live := livePostings()
				require.Greater(t, g.meta.Splits, 10, "%s: the index split many times", at)
				require.Greater(t, g.meta.Postings, live, "%s: some postings retired", at)
				held, bytes := heldCentroids(g.st.centroids)
				assert.Equal(t, live, held, "%s: the centroid vectors held, against %d live postings of the %d given out",
					at, live, g.meta.Postings)
				assert.Equal(t, 4*live*g.meta.Dims, bytes, "%s: the bytes of the centroid vectors held", at)
				return live
			}
			check("grown")
			f.restart(t)
			live := check("opened again")

			graph := f.index.g.st.centroids.graph
			require.NoError(t, graph.CleanUpTombstonedNodes(func() bool { return false }))
			stats, err := graph.Stats()
			require.NoError(t, err)
			nodes := 0
			for _, n := range stats.DistributionLayers {
				nodes += int(n)
			}
			assert.Zero(t, stats.NumTombstones, "the graph keeps no tombstone once it is cleaned")
			assert.Equal(t, live, nodes, "the graph keeps one node per live posting once it is cleaned")
		})
	}
}
