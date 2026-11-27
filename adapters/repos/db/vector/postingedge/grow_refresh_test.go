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
)

// The refresh of the edges on disk is the one the experiments measured in
// memory (v2.4): every time the index doubles the same vectors are stale, the
// refresh of each finds the same neighbors, writes the same list and enters
// the same lists of its neighbors, and the two indexes are the same index
// after it, and through the inserts that follow. With the edges in a value
// per list and in a row, with the store flushed on the way, and with the
// index closed and opened before the index doubles and after it: when the
// edges of a vector were built is on disk. TestGrowRefreshEqualsMemoryAtScale,
// of the bench harness, grows larger indexes, through two doublings, over more
// formats.
func TestGrowRefreshEqualsMemory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name       string
		format     growConfig
		cosine     bool
		bits       int
		threshold  int
		base       int
		total      int
		flushEvery int
		reopenAt   int
	}{
		{name: "a value per list", format: growConfig{}, bits: 64, threshold: 20, base: 50, total: 110, flushEvery: 23},
		{name: "the format of a shard", format: shardFormat(8), bits: 64, threshold: 20, base: 50, total: 110, flushEvery: 29, reopenAt: 80},
		{
			name: "no codes in memory, cosine, the full code", format: haloFormat(DefaultConsolidate, false), cosine: true, threshold: 20,
			base: 50, total: 110, reopenAt: 80,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const dims = 32
			f := newGrowFixtureFormat(t, tt.total, tt.base, dims, tt.cosine, tt.bits, tt.threshold, tt.format)
			f.rescoring(fewRescored)
			f.queries = f.queries[:3]
			f.same(t, "the base")
			require.Equal(t, tt.base, f.g.meta.RefreshedAt)
			require.Empty(t, f.g.staleDocs())
			require.False(t, f.g.refreshDue())
			passes, changed, entered := 0, 0, 0
			for f.inserted < tt.total {
				at := f.inserted
				difference, err := f.insert(ctx)
				require.NoError(t, err, "doc %d", at)
				require.Empty(t, difference)
				if tt.flushEvery > 0 && f.inserted%tt.flushEvery == 0 {
					require.NoError(t, f.g.flushAll())
				}
				if f.inserted == tt.reopenAt {
					f.reopen(t)
					f.same(t, "opened again")
				}
				if !f.g.refreshDue() {
					continue
				}
				// The index has doubled since its edges were last refreshed.
				stale := f.twin.staleEdges(sequentialIDs(f.inserted, 0))
				require.NotEmpty(t, stale)
				require.Equal(t, stale, f.g.staleDocs(), "with %d vectors: the vectors whose edges are old", f.inserted)
				inMemory, err := f.twin.refreshEdges(ctx, stale)
				require.NoError(t, err)
				before := f.g.refreshed
				inserts := f.g.stats
				require.NoError(t, f.g.refreshStale(ctx))
				did := f.g.refreshed
				passes++
				require.Equal(t, inMemory.Refreshed, did.Refreshed-before.Refreshed, "the vectors refreshed")
				require.Equal(t, inMemory.SelfMissing, did.SelfMissing-before.SelfMissing)
				require.Equal(t, inMemory.ListsChanged, did.ListsChanged-before.ListsChanged, "the lists that changed")
				require.Equal(t, inMemory.EdgesBefore, did.EdgesBefore-before.EdgesBefore)
				require.Equal(t, inMemory.EdgesAfter, did.EdgesAfter-before.EdgesAfter)
				require.Equal(t, inMemory.EdgesKept, did.EdgesKept-before.EdgesKept)
				require.Equal(t, inMemory.Work.SearchLoads, did.SearchLoads-before.SearchLoads, "the loads of the searches")
				require.Equal(t, inMemory.Work.SearchRounds, did.SearchRounds-before.SearchRounds)
				require.Equal(t, inMemory.Work.InverseChecks, did.InverseChecks-before.InverseChecks)
				require.Equal(t, inMemory.Work.InverseEdges, did.InverseEdges-before.InverseEdges, "the inverse edges")
				require.Equal(t, inserts.InverseEdges, f.g.stats.InverseEdges, "the inserts count what they did, and no more")
				require.Equal(t, inserts.SearchLoads, f.g.stats.SearchLoads)
				changed += inMemory.ListsChanged
				entered += inMemory.Work.InverseEdges
				require.Equal(t, f.inserted, f.g.meta.RefreshedAt)
				require.False(t, f.g.refreshDue())
				require.Empty(t, f.g.staleDocs(), "no vector is left with old edges")
				f.same(t, "refreshed")
			}
			require.Positive(t, passes)
			t.Logf("%d passes: %d lists changed, %d inverse edges entered", passes, changed, entered)
			// With postings of at most twenty members the clusters of the
			// fixture are split in several postings, and a vector has
			// neighbors outside its own that the base did not give it.
			require.Positive(t, changed, "lists changed")
			require.Positive(t, entered, "refreshed vectors entered the lists of their neighbors")
			f.same(t, "at the end")
			f.reopen(t)
			f.same(t, "at the end, opened again")
			require.NoError(t, f.g.checkBuilt())
			if tt.format.slots {
				f.vectorsIntact(t, "at the end")
			}
		})
	}
}

// When the edges of a vector were built is what the index says: those of
// the base with the base, those of an insert with the vectors there were with
// it, and those of a refresh with the vectors there were then. A vector that
// is marked deleted is not refreshed, and one that left has no time.
func TestGrowRefreshKnowsWhenEdgesWereBuilt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const base, total, dims = 40, 90, 32
	format := shardFormat(8)
	format.open.deletes = true
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, 60, format)
	f.rescoring(fewRescored)
	g := f.g
	for docID := range base {
		require.Equal(t, uint32(base), g.builtAt[docID], "doc %d of the base", docID)
	}
	for f.inserted < 2*base+2 {
		require.False(t, g.refreshDue(), "with %d vectors", f.inserted)
		_, err := f.insert(ctx)
		require.NoError(t, err)
		require.Equal(t, uint32(f.inserted), g.builtAt[f.inserted-1], "the edges of an insert are of the index with it")
		if f.inserted == 2*base-1 {
			// One vector short of the double: nothing is due, and nothing old.
			require.Empty(t, g.staleDocs())
		}
		if f.inserted >= 2*base {
			break
		}
	}
	require.True(t, g.refreshDue(), "the index has doubled")
	for f.inserted < 2*base+2 {
		_, err := f.insert(ctx)
		require.NoError(t, err)
	}
	// A marked vector keeps its edges, and one that left has none. With it
	// marked the index holds 2*base+1 vectors: the base is old, and the first
	// insert, which came with base+1, is not.
	marked, err := g.remove(3)
	require.NoError(t, err)
	require.True(t, marked)
	held := 2*base + 1
	require.Equal(t, held, g.held())
	stale := g.staleDocs()
	require.NotContains(t, stale, uint64(3))
	require.Contains(t, stale, uint64(4))
	require.NotContains(t, stale, uint64(base))
	require.Len(t, stale, base-1)
	require.NoError(t, g.refreshStale(ctx))
	require.Equal(t, uint32(base), g.builtAt[3], "a marked vector is not refreshed")
	require.Equal(t, uint32(held), g.builtAt[4], "a refreshed vector has edges of the vectors the index holds")
	require.Equal(t, uint32(base+1), g.builtAt[base], "the first insert came with more than half the index")
	require.Equal(t, base-1, g.refreshed.Refreshed, "the base but the marked one")
	require.Equal(t, held, g.meta.RefreshedAt)
	require.False(t, g.refreshDue())
	require.NoError(t, g.cleanCentroidGraph())
	require.NoError(t, g.checkCoverage())

	require.NoError(t, g.clean(ctx, 0, 0))
	require.Zero(t, g.builtAt[3], "a vector that left has no edges")
	require.NoError(t, g.checkCoverage())
	f.reopen(t)
	g = f.g
	require.Zero(t, g.builtAt[3])
	require.Equal(t, uint32(held), g.builtAt[4])
	require.Equal(t, held, g.meta.RefreshedAt)
	require.NoError(t, g.cleanCentroidGraph())
	require.NoError(t, g.checkCoverage())
	// The twin was not told of the delete: it is left behind here.
	assert.Equal(t, held, g.held())
}
