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

	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// The search of an insert reads its rows and its vectors one at a time, in
// the goroutine of its caller, whatever budget its caller gives or does not:
// the inserts run next to each other as their callers do. A query with no
// budget still spreads its reads over goroutines of their own, and so does
// one whose budget allows it, in both cache modes.
func TestPersistentInsertsReadOneAtATime(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{ent.CacheModeRouting, ent.CacheModeNone} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			const total = persistentBase + 300
			f := newPersistentFixtureMode(t, total, mode, func(cfg *PersistentConfig) {
				cfg.noBackground = true
			})
			f.add(t, 0, persistentBase)
			counts := func() (fetch, rescore int64) {
				f.index.mu.RLock()
				defer f.index.mu.RUnlock()
				return f.index.g.fanouts.Load(), f.index.g.ix.rescoreFanouts.Load()
			}
			fetchBefore, rescoreBefore := counts()

			// With no budget, as the queue of a shard calls: the default
			// spreads the reads of a search over twice as many goroutines as
			// there are cores.
			ctx := context.Background()
			const batch = 50
			for start := persistentBase; start < total; start += batch {
				end := min(start+batch, total)
				require.NoError(t, f.index.AddBatch(ctx, sequentialIDs(end-start, uint64(start)), f.vectors[start:end]))
			}
			fetchAfter, rescoreAfter := counts()
			assert.Equal(t, fetchBefore, fetchAfter, "no round of the searches of the inserts read its rows in goroutines")
			assert.Equal(t, rescoreBefore, rescoreAfter, "no rescore of the searches of the inserts read its vectors in goroutines")
			f.index.mu.RLock()
			split := f.index.g.meta.Splits
			f.index.mu.RUnlock()
			require.Positive(t, split, "the inserts split postings: their searches loaded several")

			ids, _, err := f.index.SearchByVector(ctx, f.queries[0], 10, nil)
			require.NoError(t, err)
			require.NotEmpty(t, ids)
			fetchQuery, rescoreQuery := counts()
			assert.Greater(t, fetchQuery+rescoreQuery, fetchAfter+rescoreAfter, "a query with no budget spreads its reads")
			f.covered(t, "grown")
		})
	}
}
