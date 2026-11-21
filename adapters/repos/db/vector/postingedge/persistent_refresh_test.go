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

// refreshState returns what the index says of the refresh of its edges.
func (f *persistentFixture) refreshState() (stats growRefreshStats, refreshedAt, stale int) {
	f.index.mu.RLock()
	defer f.index.mu.RUnlock()
	g := f.index.g
	return g.refreshed, g.meta.RefreshedAt, len(g.staleDocs())
}

// As it grows the index builds again the edges of its oldest vectors, by
// itself: once it holds twice the vectors of its base, in the background,
// next to the inserts that follow, and again at every doubling. No vector is
// left with edges of half the index, the index is covered and finds what it
// holds, and it opens again as it was, knowing when it last refreshed.
func TestPersistentRefreshesItsEdgesAsItDoubles(t *testing.T) {
	const k = 10
	const total = 4*persistentBase + 100
	f := newPersistentFixture(t, total)
	f.add(t, 0, 2*persistentBase-50)
	require.NoError(t, f.index.Settle(context.Background()))
	stats, refreshedAt, _ := f.refreshState()
	require.Zero(t, stats.Refreshed, "nothing is refreshed before the index doubles")
	require.Equal(t, persistentBase, refreshedAt)

	f.add(t, 2*persistentBase-50, total)
	require.Eventually(t, func() bool { return f.index.refreshes.Load() >= 2 }, 60*time.Second, 5*time.Millisecond,
		"the edges are refreshed at each doubling")
	require.NoError(t, f.index.Settle(context.Background()))
	stats, refreshedAt, stale := f.refreshState()
	require.Zero(t, stale, "no vector is left with edges of half the index")
	require.GreaterOrEqual(t, refreshedAt, 4*persistentBase)
	// The base at the first doubling, and at the second the vectors that came
	// before the first: those refreshed then have newer edges, of an index
	// that went on growing under the pass.
	require.GreaterOrEqual(t, stats.Refreshed, 2*persistentBase)
	require.Positive(t, stats.ListsChanged)
	f.covered(t, "refreshed")
	f.vectorsIntact(t, "refreshed")
	assert.Greater(t, f.recall(t, total, k, nil), 0.9)
	before := f.answers(t, k)

	f.restart(t)
	_, reopenedAt, stale := f.refreshState()
	require.Equal(t, refreshedAt, reopenedAt)
	require.Zero(t, stale)
	assert.Equal(t, before, f.answers(t, k), "the same answers")
	f.covered(t, "opened again")
}

// A refresh of the edges runs next to searches, inserts and deletes: every
// write of it is that of one writer, with those of the inserts. One that is
// told to stop stops between two vectors and the next ends what it left, and
// one over an index that is shut down does nothing.
func TestPersistentRefreshNextToSearchesAndWrites(t *testing.T) {
	ctx := context.Background()
	const k = 10
	const total = 2*persistentBase + 600
	f := newPersistentFixtureWith(t, total, func(cfg *PersistentConfig) { cfg.noMaintenance = true })
	f.add(t, 0, 2*persistentBase)

	stopped, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, f.index.refresh(stopped), context.Canceled)
	stats, refreshedAt, stale := f.refreshState()
	require.Zero(t, stats.Refreshed)
	require.Equal(t, persistentBase, refreshedAt)
	require.Equal(t, persistentBase, stale)

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
