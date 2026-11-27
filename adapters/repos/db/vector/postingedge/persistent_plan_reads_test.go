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

	"github.com/weaviate/weaviate/entities/concurrency"
	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// A search reads as the budget of its caller allows, that of an insert as
// that of a query. With a budget of one it reads its rows and its vectors one
// at a time, in the goroutine that calls. With more, or with none given, as
// the queue of a shard calls, it spreads them over goroutines of their own:
// an insert that read one at a time whatever its caller gave lost half its
// rate with the files out of the page cache. In both cache modes.
func TestPersistentSearchesReadAsTheirCallerAllows(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{ent.CacheModeRouting, ent.CacheModeNone} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			const (
				batch = 50
				part  = 300
				total = persistentBase + 2*part
			)
			f := newPersistentFixtureMode(t, total, mode, func(cfg *PersistentConfig) {
				cfg.noBackground = true
			})
			f.add(t, 0, persistentBase)
			counts := func() (fetch, rescore int64) {
				f.index.mu.RLock()
				defer f.index.mu.RUnlock()
				return f.index.g.fanouts.Load(), f.index.g.ix.rescoreFanouts.Load()
			}
			splits := func() int {
				f.index.mu.RLock()
				defer f.index.mu.RUnlock()
				return f.index.g.meta.Splits
			}
			add := func(ctx context.Context, from, to int) {
				for start := from; start < to; start += batch {
					end := min(start+batch, to)
					require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(end-start, uint64(start)), f.vectors[start:end]))
				}
			}
			search := func(ctx context.Context) {
				ids, _, err := f.index.SearchByVector(ctx, f.queries[0], 10, nil)
				require.NoError(t, err)
				require.NotEmpty(t, ids)
			}

			// With a budget of one, nothing is read in goroutines of its own.
			one := concurrency.CtxWithBudget(context.Background(), 1)
			fetchBefore, rescoreBefore := counts()
			add(one, persistentBase, persistentBase+part)
			fetchAfter, rescoreAfter := counts()
			assert.Equal(t, fetchBefore, fetchAfter, "no round of the inserts with a budget of one read its rows in goroutines")
			assert.Equal(t, rescoreBefore, rescoreAfter, "no rescore of the inserts with a budget of one read its vectors in goroutines")
			require.Positive(t, splits(), "the inserts split postings: their searches loaded several")
			search(one)
			fetchQuery, rescoreQuery := counts()
			assert.Equal(t, fetchAfter, fetchQuery, "no round of the query with a budget of one read its rows in goroutines")
			assert.Equal(t, rescoreAfter, rescoreQuery, "the rescore of the query with a budget of one read in line")

			// With no budget, as the queue of a shard calls: the default
			// spreads the reads of a search.
			none := context.Background()
			add(none, persistentBase+part, total)
			fetchSpread, rescoreSpread := counts()
			assert.Greater(t, fetchSpread, fetchQuery, "the rounds of the inserts with no budget read their rows in goroutines")
			assert.Greater(t, rescoreSpread, rescoreQuery, "the rescores of the inserts with no budget read their vectors in goroutines")
			search(none)
			fetchLast, rescoreLast := counts()
			assert.Greater(t, fetchLast, fetchSpread, "the rounds of a query with no budget read their rows in goroutines")
			assert.Greater(t, rescoreLast, rescoreSpread, "the rescore of a query with no budget read its vectors in goroutines")
			f.covered(t, "grown")
		})
	}
}
