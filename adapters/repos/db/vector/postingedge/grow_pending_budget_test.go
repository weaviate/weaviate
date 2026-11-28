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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ent "github.com/weaviate/weaviate/entities/vectorindex/postingedge"
)

// What waits of the edges takes at most its budget of heap: past it, the
// rows of the postings with the most waiting are written before their share
// of changes. The index stays the twin it grows next to, insert by insert,
// and opened again. Without a budget the same inserts leave more waiting
// than it, so the budget is what holds it.
func TestGrowPendingStaysUnderItsBudget(t *testing.T) {
	t.Parallel()
	const total, base, dims = 420, 300, 32
	const budget = 16 << 10
	for _, capped := range []bool{false, true} {
		t.Run(fmt.Sprintf("capped %v", capped), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			f := newSmallGrowFixture(t, total, base, dims, false, 64, 25, growConfig{consolidate: DefaultConsolidate})
			if capped {
				f.how.pendingBudget = budget
				f.reopen(t)
			}
			peak := int64(0)
			for f.inserted < total {
				at := f.inserted
				difference, err := f.insert(ctx)
				require.NoError(t, err, "doc %d", at)
				require.Empty(t, difference, "doc %d", at)
				heap := f.g.pendingHeap()
				if capped {
					require.LessOrEqual(t, heap, int64(budget), "what waits after doc %d", at)
				}
				peak = max(peak, heap)
			}
			if !capped {
				require.Greater(t, peak, int64(budget), "without a budget more than it waits")
				assert.Zero(t, f.g.stats.RowWritesBudget)
				return
			}
			assert.Positive(t, f.g.stats.RowWritesBudget, "rows were written before their share of changes")
			assert.Positive(t, f.g.stats.InverseEdges, "inserts change the lists of their neighbors in other postings")
			f.same(t, "grown under the budget")
			require.NoError(t, f.g.checkWaiting())
			f.reopen(t)
			f.same(t, "opened again")
		})
	}
}

// An index opened with no table of when the edges of its vectors were built
// writes the file as ever: opened to refresh, it reads from the file the
// table an index that kept it in memory has, for the same inserts.
func TestGrowDropsTheTableOfWhenEdgesWereBuilt(t *testing.T) {
	t.Parallel()
	const total, base, dims = 360, 300, 32
	grow := func(drop bool) *growFixture {
		f := newSmallGrowFixture(t, total, base, dims, false, 64, 25, growConfig{consolidate: DefaultConsolidate})
		f.how.dropBuilt = drop
		f.reopen(t)
		f.growAll(t, total)
		return f
	}
	kept, dropped := grow(false), grow(true)
	require.NotNil(t, kept.g.builtAt)
	require.Nil(t, dropped.g.builtAt, "no table in memory")
	require.False(t, dropped.g.stale(0, 2*total), "an index that keeps no table refreshes nothing")
	require.NoError(t, dropped.g.checkBuilt(), "the file knows every vector held")

	dropped.how.dropBuilt = false
	dropped.reopen(t)
	require.Equal(t, kept.g.builtAt, dropped.g.builtAt, "the table read from the file")
	require.NoError(t, dropped.g.checkBuilt())
}

// The index a shard opens caps what waits of its edges at 128 MB, and keeps no
// table of when its edges were built while the refresh is off. With a budget
// small enough to hold, it answers as it does without one.
func TestPersistentCapsWhatWaitsOfItsEdges(t *testing.T) {
	t.Parallel()
	const total = persistentBase + 200
	grow := func(budget int64) *persistentFixture {
		f := newPersistentFixtureMode(t, total, ent.CacheModeRouting, func(cfg *PersistentConfig) {
			cfg.noBackground = true
			cfg.pendingBudget = budget
		})
		f.add(t, 0, total)
		return f
	}
	byDefault := grow(0)
	byDefault.index.mu.RLock()
	assert.Equal(t, int64(DefaultPendingBudget), byDefault.index.g.pendingBudget)
	assert.Nil(t, byDefault.index.g.builtAt, "no table of when the edges were built while the refresh is off")
	byDefault.index.mu.RUnlock()

	const budget = 8 << 10
	capped, uncapped := grow(budget), grow(-1)
	capped.index.mu.RLock()
	assert.LessOrEqual(t, capped.index.g.pendingHeap(), int64(budget))
	assert.Positive(t, capped.index.g.stats.RowWritesBudget, "rows were written before their share of changes")
	capped.index.mu.RUnlock()
	uncapped.index.mu.RLock()
	assert.Zero(t, uncapped.index.g.pendingBudget)
	assert.Greater(t, uncapped.index.g.pendingHeap(), int64(budget), "without a budget more than it waits")
	uncapped.index.mu.RUnlock()
	capped.covered(t, "capped")
	assert.Equal(t, uncapped.answers(t, 10), capped.answers(t, 10), "the same answers")
}
