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
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// refreshState returns what the index says of the refresh of its edges: what
// the refreshes did, how many vectors it held when the last pass that ended
// started, and how many of its vectors have edges of half of those or fewer,
// which that pass should have refreshed.
func (f *persistentFixture) refreshState() (stats growRefreshStats, refreshedAt, old int) {
	f.index.mu.RLock()
	defer f.index.mu.RUnlock()
	g := f.index.g
	for docID := range g.live {
		if g.stale(uint64(docID), g.meta.RefreshedAt) {
			old++
		}
	}
	return g.refreshed, g.meta.RefreshedAt, old
}

// As it grows the index builds again the edges of its oldest vectors, by
// itself: once it holds twice the vectors of its base, in line with the
// inserts that follow, each of which refreshes a vector, and again at every
// doubling. When a pass ends no vector is left with edges of half the index
// it started with, the index is covered and finds what it holds, and it
// opens again as it was, knowing when it last refreshed.
func TestPersistentRefreshesItsEdgesAsItDoubles(t *testing.T) {
	const k = 10
	const total = 4*persistentBase + 100
	// Nothing in the background: the inserts alone take the passes to their
	// end, and where each ends is known.
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noBackground = true })
	f.add(t, 0, 2*persistentBase-50)
	stats, refreshedAt, _ := f.refreshState()
	require.Zero(t, stats.Refreshed, "nothing is refreshed before the index doubles")
	require.Equal(t, persistentBase, refreshedAt)

	// The first pass starts with the insert that doubles the base, takes the
	// base, a vector an insert, and ends when the index has grown by half.
	f.add(t, 2*persistentBase-50, 3*persistentBase-50)
	stats, refreshedAt, _ = f.refreshState()
	require.Equal(t, persistentBase-49, stats.Refreshed, "a vector for every insert since the index doubled")
	require.Equal(t, persistentBase, refreshedAt, "the pass is not done")
	require.Zero(t, f.index.refreshes.Load())
	f.add(t, 3*persistentBase-50, 3*persistentBase+50)
	stats, refreshedAt, old := f.refreshState()
	require.Equal(t, int64(1), f.index.refreshes.Load())
	require.Equal(t, persistentBase, stats.Refreshed, "the base")
	require.Equal(t, 2*persistentBase, refreshedAt)
	require.Zero(t, old)

	// The second starts when the index holds four times its base, and takes
	// the vectors that came before the first, and the one vector the first
	// refreshed with exactly half of what the index holds now: the others it
	// refreshed have newer edges.
	f.add(t, 3*persistentBase+50, total)
	stats, refreshedAt, _ = f.refreshState()
	require.Equal(t, persistentBase+101, stats.Refreshed)
	require.Equal(t, 2*persistentBase, refreshedAt)
	// With no more inserts, the pass is ended by whoever settles the index.
	require.NoError(t, f.index.Settle(context.Background()))
	stats, refreshedAt, old = f.refreshState()
	require.Equal(t, int64(2), f.index.refreshes.Load())
	require.Equal(t, 2*persistentBase+1, stats.Refreshed)
	require.Equal(t, 4*persistentBase, refreshedAt)
	require.Zero(t, old, "no vector is left with edges of half the index the pass started with")
	require.Positive(t, stats.ListsChanged)
	f.covered(t, "refreshed")
	f.vectorsIntact(t, "refreshed")
	assert.Greater(t, f.recall(t, total, k, nil), 0.9)
	before := f.answers(t, k)

	f.restart(t)
	_, reopenedAt, old := f.refreshState()
	require.Equal(t, refreshedAt, reopenedAt)
	require.Zero(t, old)
	assert.Equal(t, before, f.answers(t, k), "the same answers")
	f.covered(t, "opened again")
}

// A pass that the inserts leave halfway is ended in the background, by the
// index itself, and one that was on when the index closed goes on when it
// opens: which vectors have new edges is on disk.
func TestPersistentRefreshEndsInTheBackground(t *testing.T) {
	const total = 2*persistentBase + 100
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noBackground = true })
	f.add(t, 0, total)
	stats, refreshedAt, _ := f.refreshState()
	require.Equal(t, 101, stats.Refreshed)
	require.Equal(t, persistentBase, refreshedAt)

	// Closed with the pass on, and opened with its goroutine.
	f.cfg.noBackground = false
	f.restart(t)
	require.Eventually(t, func() bool { return f.index.refreshes.Load() >= 1 }, 60*time.Second, 5*time.Millisecond,
		"the pass that was on is ended once the index is open")
	stats, refreshedAt, old := f.refreshState()
	// The pass starts again over the index as it is: the vectors of the base
	// the one before had left, and the fifty that came before the index was
	// half of what it is now.
	require.Equal(t, persistentBase-101+50, stats.Refreshed)
	require.Equal(t, total, refreshedAt)
	require.Zero(t, old)
	f.covered(t, "refreshed in the background")
	assert.Greater(t, f.recall(t, total, 10, nil), 0.9)
}

// A refresh of the edges runs next to searches, inserts and deletes: every
// write of it is that of one writer, with those of the inserts. One that is
// told to stop stops between two vectors and the next ends what it left, and
// one over an index that is shut down does nothing.
func TestPersistentRefreshNextToSearchesAndWrites(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const total = 2*persistentBase + 600
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noBackground = true })
	f.add(t, 0, 2*persistentBase)

	// The insert that doubled the base started the pass, with one vector.
	stopped, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, f.index.refresh(stopped), context.Canceled)
	stats, refreshedAt, _ := f.refreshState()
	require.Equal(t, 1, stats.Refreshed)
	require.Equal(t, persistentBase, refreshedAt)

	var stop atomic.Bool
	var deleted [total]atomic.Bool
	eg := enterrors.NewErrorGroupWrapper(testLogger())
	eg.Go(func() error {
		defer stop.Store(true)
		return f.index.refresh(ctx)
	})
	eg.Go(func() error {
		for next := 2 * persistentBase; next < total; next++ {
			if err := f.index.Add(ctx, uint64(next), f.vectors[next]); err != nil {
				return errors.Wrapf(err, "add doc %d", next)
			}
			if next%10 == 0 {
				gone := uint64(next - 2*persistentBase)
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
				if _, _, err := f.index.SearchByVector(ctx, f.queries[q%len(f.queries)], k, nil); err != nil {
					return errors.Wrapf(err, "query %d", q)
				}
			}
			return nil
		})
	}
	require.NoError(t, eg.Wait())
	stats, refreshedAt, _ = f.refreshState()
	require.GreaterOrEqual(t, refreshedAt, 2*persistentBase)
	require.Greater(t, stats.Refreshed, persistentBase/2, "the base, but for the vectors deleted before their turn")
	require.Equal(t, int64(1), f.index.refreshes.Load())
	f.covered(t, "refreshed next to everything else")
	f.vectorsIntact(t, "refreshed next to everything else")
	held := func(id uint64) bool { return !deleted[id].Load() }
	assert.Greater(t, f.recall(t, total, k, held), 0.9)
	before := f.answers(t, k)
	f.restart(t)
	assert.Equal(t, before, f.answers(t, k), "the same answers")

	index := f.index
	f.close(t)
	require.NoError(t, index.refresh(ctx), "a refresh of an index that is shut down does nothing")
	f.open(t)
}
