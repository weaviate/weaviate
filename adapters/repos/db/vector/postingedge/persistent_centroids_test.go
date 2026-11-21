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
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/cyclemanager"
	enterrors "github.com/weaviate/weaviate/entities/errors"
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

// The graph of an index on disk reads its centroids from goroutines that
// hold no lock of the index: the one that fills its cache once it is opened,
// and the cycle of the node that takes its deleted centroids out. A posting
// that splits meanwhile gives the index two centroids more, and the graph
// reads the ones it was told of, each the one it was.
func TestCentroidGraphReadsItsCentroidsWhileTheyGrow(t *testing.T) {
	ctx := context.Background()
	const dims, first, more = 8, 16, 4000
	centroid := func(id int) []float32 {
		vector := make([]float32, dims)
		for i := range vector {
			vector[i] = float32(id*dims + i)
		}
		return vector
	}
	vectors := make([][]float32, 0, first)
	for id := range first {
		vectors = append(vectors, centroid(id))
	}
	index, err := openCentroidGraphWith(ctx, testLogger(), t.TempDir(), vectors, centroidCycles{})
	require.NoError(t, err)
	defer func() { require.NoError(t, index.shutdown(ctx)) }()

	// told is how many centroids the graph has been told of.
	var told atomic.Int64
	told.Store(first)
	var done atomic.Bool
	readers := enterrors.NewErrorGroupWrapper(testLogger())
	for range 4 {
		readers.Go(func() error {
			for id := 0; !done.Load(); id++ {
				id %= int(told.Load())
				vector, err := index.publishedVector(ctx, uint64(id))
				if err != nil {
					return errors.Wrapf(err, "centroid %d", id)
				}
				if len(vector) != dims || vector[0] != float32(id*dims) {
					return errors.Errorf("centroid %d read as another one: %v", id, vector)
				}
			}
			return nil
		})
	}
	// As a split does, with the index locked: the two centroids, and then the
	// graph hears of them.
	for id := first; id < first+more; id += 2 {
		index.vectors = append(index.vectors, centroid(id), centroid(id+1))
		index.publish()
		told.Store(int64(id + 2))
	}
	done.Store(true)
	require.NoError(t, readers.Wait())
	_, err = index.publishedVector(ctx, first+more)
	require.Error(t, err, "a centroid the index does not have")
}

// The cycles of the node run when the node says, in goroutines of theirs,
// with the index adding, splitting its postings, deleting and searching: they
// hold none of its locks. The index is opened again halfway, so that the
// graph has to read its centroids from the index, which its cycle then does
// while a split gives the index two more.
func TestPersistentCentroidGraphIsKeptWhileTheIndexWrites(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const half = persistentBase + 800
	const total = persistentBase + 2400
	never := func() bool { return false }
	tombstones := cyclemanager.NewCallbackGroup("tombstones", testLogger(), 1)
	commitLogs := cyclemanager.NewCallbackGroup("commitlogs", testLogger(), 1)
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) {
		cfg.TombstoneCallbacks, cfg.CommitLogCallbacks = tombstones, commitLogs
		// A file of the commit log is closed every few splits, so that the
		// cycle of the logs has files to condense and combine.
		cfg.CommitLogThreshold = 4096
		cfg.centroidsEveryCycle = true
	})
	f.add(t, 0, half)
	f.restart(t)

	var done atomic.Bool
	cycles := enterrors.NewErrorGroupWrapper(testLogger())
	cycles.Go(func() error {
		for !done.Load() {
			tombstones.CycleCallback(never)
			commitLogs.CycleCallback(never)
		}
		return nil
	})
	readers := enterrors.NewErrorGroupWrapper(testLogger())
	for range 2 {
		readers.Go(func() error {
			for q := 0; !done.Load(); q++ {
				if _, _, err := f.index.SearchByVector(ctx, f.queries[q%len(f.queries)], k, nil); err != nil {
					return errors.Wrapf(err, "query %d", q)
				}
			}
			return nil
		})
	}
	const batch = 50
	var deleted [total]bool
	for from := half; from < total; from += batch {
		require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(batch, uint64(from)), f.vectors[from:from+batch]))
		// Deletes too: the pass that takes them out merges postings, which
		// leaves deleted centroids as a split does.
		for id := from - half; id < from-half+batch/2; id++ {
			require.NoError(t, f.index.Delete(uint64(id)))
			deleted[id] = true
		}
	}
	done.Store(true)
	require.NoError(t, cycles.Wait())
	require.NoError(t, readers.Wait())

	require.NoError(t, f.index.Settle(ctx))
	tombstones.CycleCallback(never)
	require.Zero(t, f.deletedCentroids(t), "the cycle of the tombstones took them out")
	f.covered(t, "at the end")
	f.vectorsIntact(t, "at the end")
	held := func(id uint64) bool { return !deleted[id] }
	assert.Greater(t, f.recall(t, total, k, held), 0.9)
	before := f.answers(t, k)
	f.restart(t)
	assert.Equal(t, before, f.answers(t, k), "the same answers")
	f.covered(t, "opened again")
}
