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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// deletedCentroids tells how many centroids of postings that were split or
// merged the graph still holds, deleted and not taken out.
func (f *persistentFixture) deletedCentroids(t *testing.T) int {
	t.Helper()
	f.index.mu.RLock()
	defer f.index.mu.RUnlock()
	stats, err := f.index.g.st.centroids.graph.Stats()
	require.NoError(t, err)
	return stats.NumTombstones
}

// The graph over the centroids is kept by the cycles of the node that keep
// every HNSW graph: a posting that splits leaves its centroid deleted in the
// graph, and the cycle of the tombstones takes it out for good. Without the
// cycles they stay, one for every split there ever was. The index finds what
// it holds as before, and opens again with a graph that has none.
func TestPersistentKeepsItsCentroidGraph(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const total = persistentBase + 1500
	never := func() bool { return false }
	tests := []struct {
		name string
		// kept gives the index the cycles.
		kept bool
	}{
		{name: "with the cycles of the node", kept: true},
		{name: "without them"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tombstones := cyclemanager.NewCallbackGroup("tombstones", testLogger(), 1)
			commitLogs := cyclemanager.NewCallbackGroup("commitlogs", testLogger(), 1)
			f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) {
				if tt.kept {
					cfg.TombstoneCallbacks, cfg.CommitLogCallbacks = tombstones, commitLogs
				}
			})
			f.add(t, 0, total)
			require.NoError(t, f.index.Settle(ctx))
			f.index.mu.RLock()
			splits := f.index.g.meta.Splits
			f.index.mu.RUnlock()
			require.Positive(t, splits)
			require.Equal(t, splits, f.deletedCentroids(t), "a deleted centroid for every posting that split")

			// The cycles of the node, once.
			tombstones.CycleCallback(never)
			commitLogs.CycleCallback(never)
			if !tt.kept {
				require.Equal(t, splits, f.deletedCentroids(t), "nothing takes them out")
				return
			}
			require.Zero(t, f.deletedCentroids(t), "the cycle of the tombstones took them out")
			f.covered(t, "with its graph cleaned")
			assert.Greater(t, f.recall(t, total, k, nil), 0.9)

			f.restart(t)
			require.Zero(t, f.deletedCentroids(t), "opened again, the graph has none")
			f.covered(t, "opened again")
			assert.Greater(t, f.recall(t, total, k, nil), 0.9)
		})
	}
}
