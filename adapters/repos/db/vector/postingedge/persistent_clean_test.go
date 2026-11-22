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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// marked tells how many vectors of the index are marked deleted and still in
// their postings.
func (f *persistentFixture) marked() int {
	f.index.mu.RLock()
	defer f.index.mu.RUnlock()
	return f.index.g.deletedCount
}

// Once enough of its vectors are deleted the index takes them out for good,
// by itself: in the background, next to whatever else it does. Nothing is
// left marked, no deleted vector is held or returned, the index is covered
// and finds what is left as well as before, and it opens again as it was. A
// doc id that left can be given again, and is then the new vector.
func TestPersistentCleansItsDeletes(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const total = persistentBase + 900
	f := newPersistentFixture(t, total)
	f.add(t, 0, total)
	deleted := map[uint64]bool{}
	held := func(id uint64) bool { return !deleted[id] }

	// Under the share that makes a pass due, the deleted vectors stay marked.
	for id := uint64(0); id < 30; id++ {
		require.NoError(t, f.index.Delete(id))
		deleted[id] = true
	}
	require.NoError(t, f.index.maintain(ctx))
	require.Equal(t, 30, f.marked(), "a pass is not due under %.0f %% of the vectors", 100*growCleanShare)
	require.Zero(t, f.index.cleans.Load())

	for id := uint64(30); id < total; id += 6 {
		require.NoError(t, f.index.Delete(id))
		deleted[id] = true
	}
	// A pass takes out the vectors that were marked when it started: those
	// deleted under it wait for the next, which is due only if they are
	// enough.
	due := func() bool {
		f.index.mu.RLock()
		defer f.index.mu.RUnlock()
		return f.index.g.cleanDue()
	}
	require.Eventually(t, func() bool { return f.index.cleans.Load() >= 1 && !due() }, 30*time.Second,
		5*time.Millisecond, "the deleted vectors leave once they are %.0f %% of the index", 100*growCleanShare)
	require.Less(t, f.marked(), len(deleted)/2, "most of them left")
	require.NoError(t, f.index.clean(ctx))
	require.Zero(t, f.marked(), "a pass takes out every vector that is marked")
	f.covered(t, "cleaned")
	f.vectorsIntact(t, "cleaned")
	f.index.mu.RLock()
	live := f.index.g.meta.Live
	f.index.mu.RUnlock()
	require.Equal(t, total-len(deleted), live, "what the index holds is what was not deleted")
	for id := range uint64(total) {
		require.Equal(t, held(id), f.index.ContainsDoc(id), "doc %d", id)
	}
	assert.Greater(t, f.recall(t, total, k, held), 0.9)
	before := f.answers(t, k)

	f.restart(t)
	assert.Equal(t, before, f.answers(t, k), "the same answers")
	for id := range uint64(total) {
		require.Equal(t, held(id), f.index.ContainsDoc(id), "doc %d, opened again", id)
	}
	f.covered(t, "opened again")

	// A doc id that left is given again, with the vector of another doc.
	const again, other = 36, total - 1
	require.True(t, deleted[again])
	require.NoError(t, f.index.Delete(other))
	require.NoError(t, f.index.Add(ctx, again, f.vectors[other]))
	require.True(t, f.index.ContainsDoc(again))
	ids, _, err := f.index.SearchByVector(ctx, f.vectors[other], 1, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{again}, ids, "the vector doc %d was given last is found under it", again)
	f.normalized[again] = distancer.Normalize(f.vectors[other])
	f.vectorsIntact(t, "a doc id given again")
	f.covered(t, "a doc id given again")
}

// The pass that takes the deleted vectors out goes in line with the writes:
// it starts with the delete that makes the marked ones 3 % of the index,
// takes out those that were marked then, and every delete and every insert
// after that takes it a step further, with nothing in the background. So it
// ends within a bounded number of writes, however they come.
func TestPersistentCleansInLineWithItsWrites(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const grown = 2 * persistentBase
	const total = grown + 300
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noBackground = true })
	f.add(t, 0, grown)
	removed := func() int {
		f.index.mu.RLock()
		defer f.index.mu.RUnlock()
		return f.index.g.cleaned.Removed
	}

	// One delete a call. Under the share nothing leaves.
	due := int(growCleanShare * grown)
	deleted := map[uint64]bool{}
	next := uint64(0)
	for ; len(deleted) < due-1; next++ {
		require.NoError(t, f.index.Delete(next))
		deleted[next] = true
	}
	require.Equal(t, due-1, f.marked())
	require.False(t, f.index.cleanOn())
	// The delete that makes them enough starts the pass, and those that
	// follow take it to its end.
	for ; f.index.cleans.Load() == 0; next++ {
		require.Less(t, len(deleted), due+200, "the pass ends within a couple of hundred deletes")
		require.NoError(t, f.index.Delete(next))
		deleted[next] = true
	}
	require.Equal(t, due, removed(), "the pass took out the vectors that were marked when it started")
	require.Equal(t, len(deleted)-due, f.marked(), "those deleted under it wait")
	f.covered(t, "after the first pass")

	// A pass that the deletes start and leave halfway is ended by inserts.
	for ; !f.index.cleanOn(); next++ {
		require.NoError(t, f.index.Delete(next))
		deleted[next] = true
	}
	marked := f.marked()
	inserted := grown
	for ; f.index.cleans.Load() < 2; inserted++ {
		require.Less(t, inserted, total, "the pass ends within three hundred inserts")
		require.NoError(t, f.index.Add(ctx, uint64(inserted), f.vectors[inserted]))
	}
	require.Equal(t, due+marked, removed())
	require.Zero(t, f.marked())
	f.covered(t, "after the second pass")
	f.vectorsIntact(t, "after the second pass")
	held := func(id uint64) bool { return id < uint64(inserted) && !deleted[id] }
	hits, wanted := 0, 0
	for i, query := range f.queries {
		ids, _, err := f.index.SearchByVector(ctx, query, k, nil)
		require.NoError(t, err)
		for _, id := range ids {
			require.True(t, held(id), "query %d returned doc %d", i, id)
		}
		want := f.truth(t, query, inserted, k, held)
		for _, id := range want {
			if slices.Contains(ids, id) {
				hits++
			}
		}
		wanted += len(want)
	}
	assert.Greater(t, float64(hits)/float64(wanted), 0.9)
}

// The cleanup of the deleted vectors and the layout of the vectors run in the
// background next to searches, inserts and deletes, and what they leave is
// the index: covered, every vector the one it was given, no deleted vector
// returned at any moment, and the same after it is opened again.
//
// The inserts go on until both have run next to them, with vectors to spare:
// when the background gets to a pass is not the test's to say, and a writer
// that stopped at a fixed count ended, on a busy machine, before the layout
// that was due had.
func TestPersistentMaintenanceNextToEverything(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) { persistentMaintenanceNextToEverything(t, cacheMode) })
	}
}

func persistentMaintenanceNextToEverything(t *testing.T, cacheMode string) {
	ctx := context.Background()
	const k = 10
	// atLeast is how many vectors go in whatever happens, and total how many
	// there are for the passes to run next to.
	const atLeast, total = 2*persistentBase + 400, 6 * persistentBase
	f := newPersistentFixtureMode(t, total, cacheMode, nil)
	f.add(t, 0, persistentBase+200)

	var deleted [total]atomic.Bool
	var stop atomic.Bool
	// added is how many vectors went in: the doc ids under it.
	var added atomic.Int64
	added.Store(persistentBase + 200)
	eg := enterrors.NewErrorGroupWrapper(testLogger())
	eg.Go(func() error {
		defer stop.Store(true)
		for next := persistentBase + 200; next < total; next++ {
			if next >= atLeast && f.index.cleans.Load() > 0 && f.index.layouts.Load() > 0 {
				break
			}
			if err := f.index.Add(ctx, uint64(next), f.vectors[next]); err != nil {
				return errors.Wrapf(err, "add doc %d", next)
			}
			added.Store(int64(next) + 1)
			if next%3 == 0 {
				// Deleted for the searches once the delete has returned: it
				// waits for whoever is writing.
				gone := uint64(next - 150)
				if err := f.index.Delete(gone); err != nil {
					return err
				}
				deleted[gone].Store(true)
			}
		}
		return nil
	})
	for range 3 {
		eg.Go(func() error {
			for q := 0; !stop.Load(); q++ {
				// A doc deleted before the search started is not in its answer.
				var gone [total]bool
				for id := range gone {
					gone[id] = deleted[id].Load()
				}
				ids, _, err := f.index.SearchByVector(ctx, f.queries[q%len(f.queries)], k, nil)
				if err != nil {
					return errors.Wrapf(err, "query %d", q)
				}
				for _, id := range ids {
					if gone[id] {
						return errors.Errorf("query %d returned doc %d, which was deleted", q, id)
					}
				}
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())
	n := int(added.Load())
	require.Positive(t, f.index.cleans.Load(), "the deleted vectors were taken out on the way, with %d vectors in", n)
	require.Positive(t, f.index.layouts.Load(), "the vectors were laid out on the way, with %d vectors in", n)
	t.Logf("%d vectors in when both had run", n)

	// What is left to do is done, and the index is whole.
	require.NoError(t, f.index.maintain(ctx))
	require.NoError(t, f.index.clean(ctx))
	require.Zero(t, f.marked())
	f.covered(t, "at the end")
	f.vectorsIntact(t, "at the end")
	held := func(id uint64) bool { return id < uint64(n) && !deleted[id].Load() }
	for id := range uint64(total) {
		require.Equal(t, held(id), f.index.ContainsDoc(id), "doc %d", id)
	}
	assert.Greater(t, f.recall(t, n, k, held), 0.9)
	before := f.answers(t, k)
	f.restart(t)
	assert.Equal(t, before, f.answers(t, k), "the same answers")
	f.covered(t, "opened again")
	f.vectorsIntact(t, "opened again")
}

// A pass over the deleted vectors that is told to stop stops between two
// postings, where what there is is an index, and the next one ends what it
// left. One over an index that is shut down does nothing. And an index of the
// first version, with a value per list of edges, keeps its deleted vectors
// marked: nothing takes them out.
func TestPersistentCleanStops(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const total = persistentBase + 600

	t.Run("told to stop", func(t *testing.T) {
		f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noMaintenance = true })
		f.add(t, 0, total)
		deleted := map[uint64]bool{}
		for id := uint64(0); id < total; id += 4 {
			require.NoError(t, f.index.Delete(id))
			deleted[id] = true
		}
		held := func(id uint64) bool { return !deleted[id] }
		stopped, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, f.index.clean(stopped), context.Canceled)
		require.Equal(t, len(deleted), f.marked())
		f.covered(t, "after a pass that was told to stop")
		assert.Greater(t, f.recall(t, total, k, held), 0.9)

		require.NoError(t, f.index.clean(ctx))
		require.Zero(t, f.marked())
		require.Equal(t, int64(1), f.index.cleans.Load())
		f.covered(t, "cleaned")
		assert.Greater(t, f.recall(t, total, k, held), 0.9)

		index := f.index
		f.close(t)
		require.NoError(t, index.clean(ctx), "a pass over an index that is shut down does nothing")
		f.open(t)
		f.covered(t, "opened again")
	})

	t.Run("an index of the first version", func(t *testing.T) {
		f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.listEdges = true })
		f.add(t, 0, total)
		deleted := map[uint64]bool{}
		for id := uint64(0); id < total; id += 4 {
			require.NoError(t, f.index.Delete(id))
			deleted[id] = true
		}
		require.NoError(t, f.index.maintain(ctx))
		require.Zero(t, f.index.cleans.Load())
		require.Equal(t, len(deleted), f.marked(), "its deleted vectors stay marked")
		require.ErrorContains(t, f.index.clean(ctx), "a value per list")
		f.covered(t, "marked")
		assert.Greater(t, f.recall(t, total, k, func(id uint64) bool { return !deleted[id] }), 0.9)
	})
}
