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
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// As it grows the index writes its vectors again in the order of their
// postings, by itself: once it has doubled, in the background, next to the
// inserts that follow. It answers the same before and after, every vector is
// the one it was given, only the files of the last generation are left, and
// it opens again as it was.
func TestPersistentLaysItsVectorsOutAsItGrows(t *testing.T) {
	const k = 10
	const total = 2*persistentBase + 300
	f := newPersistentFixture(t, total)
	f.add(t, 0, total)
	require.Eventually(t, func() bool { return f.index.layouts.Load() >= 1 }, 30*time.Second, 5*time.Millisecond,
		"the vectors are laid out again once the index has doubled")
	g := f.index.g
	f.index.mu.RLock()
	gen, laidOut, slots := g.meta.VectorGen, g.meta.VectorsLaidOut, int(g.nextSlot)
	f.index.mu.RUnlock()
	require.Equal(t, 2, gen)
	require.GreaterOrEqual(t, laidOut, 2*persistentBase, "every vector there was when it doubled")
	require.Equal(t, total, slots, "no slot is left without a vector")
	f.covered(t, "laid out")
	f.vectorsIntact(t, "laid out")
	before := f.answers(t, k)
	assert.Greater(t, f.recall(t, total, k, nil), 0.9)

	f.restart(t)
	require.Equal(t, gen, f.index.g.meta.VectorGen)
	require.Equal(t, []string{"slots-2.u32", "vectors-2.f32"}, generationFiles(t, f.index.g.dir))
	assert.Equal(t, before, f.answers(t, k), "the same answers")
	f.covered(t, "opened again")
	f.vectorsIntact(t, "opened again")
}

// A pass over the vectors runs next to searches, inserts and deletes, and
// what it leaves is the index: every vector the one it was given, whenever it
// came in. One that is told to stop, or that finds the index shut down,
// leaves the files the index had.
func TestPersistentLayoutNextToSearchesAndWrites(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const total = persistentBase + 900
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noMaintenance = true })
	f.add(t, 0, persistentBase+300)

	t.Run("told to stop", func(t *testing.T) {
		stopped, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, f.index.layOut(stopped), context.Canceled)
		require.Equal(t, 1, f.index.g.meta.VectorGen)
		require.Equal(t, []string{"slots-1.u32", "vectors-1.f32"}, generationFiles(t, f.index.g.dir))
		f.vectorsIntact(t, "after a pass that was told to stop")
	})

	t.Run("next to searches, inserts and deletes", func(t *testing.T) {
		var stop atomic.Bool
		eg := enterrors.NewErrorGroupWrapper(testLogger())
		eg.Go(func() error {
			defer stop.Store(true)
			for pass := range 3 {
				if err := f.index.layOut(ctx); err != nil {
					return errors.Wrapf(err, "pass %d", pass)
				}
			}
			return nil
		})
		eg.Go(func() error {
			for next := persistentBase + 300; next < total; next++ {
				if err := f.index.Add(ctx, uint64(next), f.vectors[next]); err != nil {
					return errors.Wrapf(err, "add doc %d", next)
				}
				if next%7 == 0 {
					if err := f.index.Delete(uint64(next - 250)); err != nil {
						return err
					}
				}
			}
			return nil
		})
		for range 3 {
			eg.Go(func() error {
				for q := 0; !stop.Load(); q++ {
					if _, _, err := f.index.SearchByVector(ctx, f.queries[q%len(f.queries)], k, nil); err != nil {
						return errors.Wrapf(err, "query %d", q)
					}
				}
				return nil
			})
		}
		require.NoError(t, eg.Wait())
		require.Equal(t, 4, f.index.g.meta.VectorGen)
		// Whatever came in after the last pass is in the next one.
		require.NoError(t, f.index.layOut(ctx))
		require.Equal(t, []string{"slots-5.u32", "vectors-5.f32"}, generationFiles(t, f.index.g.dir))
		f.covered(t, "laid out next to everything else")
		f.vectorsIntact(t, "laid out next to everything else")
		held := func(id uint64) bool { return f.index.ContainsDoc(id) }
		assert.Greater(t, f.recall(t, total, k, held), 0.9)
		before := f.answers(t, k)
		f.restart(t)
		assert.Equal(t, before, f.answers(t, k), "the same answers")
		f.vectorsIntact(t, "opened again")
	})

	t.Run("shut down", func(t *testing.T) {
		index := f.index
		f.close(t)
		require.NoError(t, index.layOut(ctx), "a pass over an index that is shut down does nothing")
		f.open(t)
		require.Equal(t, 5, f.index.g.meta.VectorGen)
	})
}

// An index written before the vectors had slots and the edges of a posting
// were one row opens as what it is, grows, deletes and restarts as it did,
// and lays nothing out.
func TestPersistentOpensAnIndexOfTheFirstVersion(t *testing.T) {
	const k = 10
	const total = 2*persistentBase + 300
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.listEdges = true })
	f.add(t, 0, persistentBase+200)
	g := f.index.g
	require.Zero(t, g.meta.Consolidate)
	require.Zero(t, g.meta.VectorGen)
	require.Nil(t, g.waiting)
	require.Equal(t, []string{"vectors.f32"}, generationFiles(t, g.dir))

	// Whoever opens it next writes a base in the format of now, and this is
	// not a base to write: it stays as it is.
	f.cfg.listEdges = false
	f.restart(t)
	g = f.index.g
	require.Zero(t, g.meta.Consolidate)
	require.Zero(t, g.meta.VectorGen)
	require.NoError(t, f.index.Delete(3, 700))
	f.add(t, persistentBase+200, total)
	require.NoError(t, f.index.maintain(context.Background()))
	require.Zero(t, f.index.layouts.Load(), "its vectors are at their doc ids")
	f.covered(t, "grown")
	held := func(id uint64) bool { return id != 3 && id != 700 }
	assert.Greater(t, f.recall(t, total, k, held), 0.9)
	before := f.answers(t, k)
	f.restart(t)
	assert.Equal(t, before, f.answers(t, k), "the same answers")
	_, err := os.Stat(filepath.Join(f.root, "lsm", helpers.PostingEdgeBucketName(helpers.PostingEdgeBucketPending, f.cfg.ID)))
	require.True(t, os.IsNotExist(err), "it keeps no bucket of what waits: %v", err)
}

// What a flush leaves on disk holds the changes to the edges that wait: an
// index opened from a copy of its files has them waiting as they were.
func TestPersistentKeepsWhatWaitsOfItsEdges(t *testing.T) {
	const total = persistentBase + 500
	f := newPersistentFixture(t, total)
	f.add(t, 0, total)
	require.NoError(t, f.index.Flush())
	g := f.index.g
	require.Equal(t, DefaultConsolidate, g.meta.Consolidate)
	require.True(t, g.meta.RowsWithMembers)
	require.NotNil(t, g.waiting)
	require.NotEmpty(t, g.pending, "changes wait for the rows of their postings")

	c := f.crashed(t)
	require.Len(t, c.index.g.pending, len(g.pending))
	require.Equal(t, g.pendingMembers, c.index.g.pendingMembers)
	for p, pend := range g.pending {
		require.NotNil(t, c.index.g.pending[p], "posting %d", p)
		require.Equal(t, pend.lists, c.index.g.pending[p].lists, "posting %d", p)
	}
	c.covered(t, "opened from what the flush left")
	assert.Equal(t, f.answers(t, 10), c.answers(t, 10), "the same answers")
}
