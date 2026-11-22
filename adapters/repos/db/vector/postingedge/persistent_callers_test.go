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

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// Several callers add to the index at once, as the shared workers of a node
// do with what a queue holds: their searches run next to each other and next
// to the queries, and their writes one at a time. Nothing a write takes from
// its search is stale, which the index checks here list by list. With deletes
// and with what the index does by itself on the way, what is left is the
// index: covered, every vector the one it was given, none that was deleted
// held or returned, and the same after it is opened again.
func TestPersistentAddsFromSeveralCallers(t *testing.T) {
	for _, cacheMode := range cacheModes {
		t.Run(cacheMode, func(t *testing.T) { persistentAddsFromSeveralCallers(t, cacheMode) })
	}
}

func persistentAddsFromSeveralCallers(t *testing.T, cacheMode string) {
	ctx := context.Background()
	const k = 10
	const callers, batch = 8, 25
	const total = persistentBase + 1600
	f := newPersistentFixtureMode(t, total, cacheMode, nil)
	f.add(t, 0, persistentBase)
	f.index.mu.Lock()
	f.index.g.verifyKept = true
	f.index.mu.Unlock()

	var next atomic.Int64
	next.Store(persistentBase)
	var deleted [total]atomic.Bool
	var done atomic.Bool
	writers := enterrors.NewErrorGroupWrapper(testLogger())
	for range callers {
		writers.Go(func() error {
			for {
				from := int(next.Add(batch)) - batch
				if from >= total {
					return nil
				}
				to := min(from+batch, total)
				if err := f.index.AddBatch(ctx, sequentialIDs(to-from, uint64(from)), f.vectors[from:to]); err != nil {
					return errors.Wrapf(err, "docs [%d, %d)", from, to)
				}
				// One of the base goes with every batch.
				gone := uint64(from-persistentBase) / 3
				if err := f.index.Delete(gone); err != nil {
					return err
				}
				deleted[gone].Store(true)
			}
		})
	}
	readers := enterrors.NewErrorGroupWrapper(testLogger())
	for range 3 {
		readers.Go(func() error {
			for q := 0; !done.Load(); q++ {
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
	require.NoError(t, writers.Wait())
	done.Store(true)
	require.NoError(t, readers.Wait())

	require.NoError(t, f.index.Settle(ctx))
	f.index.mu.RLock()
	g := f.index.g
	// What it holds: the vectors in its postings but for those that are marked
	// deleted and wait for a pass to take them out.
	verified, stale, reassigned, live := g.batchVerified, g.stats.StaleLists, g.stats.PlansReassign, g.held()
	f.index.mu.RUnlock()
	t.Logf("%d lists taken as their search left them and checked, %d read again, %d postings chosen again", verified, stale,
		reassigned)
	require.Positive(t, verified, "lists are taken from what the searches kept")
	gone := 0
	for id := range deleted {
		if deleted[id].Load() {
			gone++
		}
	}
	require.Equal(t, total-gone, live, "what the index holds is what was not deleted")
	f.covered(t, "at the end")
	f.vectorsIntact(t, "at the end")
	held := func(id uint64) bool { return !deleted[id].Load() }
	for id := range uint64(total) {
		require.Equal(t, held(id), f.index.ContainsDoc(id), "doc %d", id)
	}
	assert.Greater(t, f.recall(t, total, k, held), 0.9)
	before := f.answers(t, k)
	f.restart(t)
	assert.Equal(t, before, f.answers(t, k), "the same answers")
	f.covered(t, "opened again")
}

// A doc id is given once: of two callers that add it at the same time, one
// is told it is taken, whichever comes second, and the index holds the vector
// of the other.
func TestPersistentCallersWithTheSameDocID(t *testing.T) {
	ctx := context.Background()
	const total = persistentBase + 200
	f := newPersistentFixture(t, total)
	f.add(t, 0, persistentBase)
	for id := persistentBase; id < total; id++ {
		var refused atomic.Int64
		eg := enterrors.NewErrorGroupWrapper(testLogger())
		for range 4 {
			eg.Go(func() error {
				err := f.index.Add(ctx, uint64(id), f.vectors[id])
				if errors.Is(err, ErrNotSupported) {
					refused.Add(1)
					return nil
				}
				return err
			})
		}
		require.NoError(t, eg.Wait(), "doc %d", id)
		require.Equal(t, int64(3), refused.Load(), "doc %d is given once", id)
	}
	f.covered(t, "at the end")
	f.vectorsIntact(t, "at the end")
}
