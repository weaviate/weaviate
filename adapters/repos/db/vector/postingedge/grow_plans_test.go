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

	"github.com/stretchr/testify/require"
)

// edgesOfDoc reads the list of edges of a vector from the store, with what
// waits of it in memory.
func edgesOfDoc(t *testing.T, g *growIndex, docID uint64) []uint32 {
	t.Helper()
	p := g.st.side.postingOf[docID]
	_, ids, lists, err := g.readEdges(p)
	require.NoError(t, err)
	for i, id := range ids {
		if id == docID {
			return lists[i]
		}
	}
	require.Failf(t, "no list", "doc %d has no list of edges in posting %d", docID, p)
	return nil
}

// The search of an insert can run while other inserts write, as the searches
// of the callers of a shard do: its writes come later, after those of the
// inserts that searched before it, and they look again at what was written
// since. Every list of edges a write takes from what its search kept is read
// again here and must be the one the index has: a list that another write
// changed, taken as the search left it, would lose the edge that write gave
// it. Windows of searches from one to thirty-two, with postings that split
// between a search and its writes.
func TestGrowPlansWriteAfterOtherWrites(t *testing.T) {
	ctx := context.Background()
	const total, base, dims = 2200, 1000, 64
	format := shardFormat(8)
	format.open.deletes = true
	f := newGrowFixtureFormat(t, total, base, dims, false, 64, 60, format)
	g := f.g
	g.verifyKept = true
	next := uint64(base)
	write := func(plan *growPlan) {
		t.Helper()
		require.False(t, g.outdated(plan))
		require.NoError(t, g.refresh(ctx, plan), "doc %d", plan.docID)
		require.NoError(t, g.apply(ctx, plan), "doc %d", plan.docID)
	}
	for _, window := range []int{1, 2, 4, 8, 32} {
		stale, verified, reassigned := g.stats.StaleLists, g.batchVerified, g.stats.PlansReassign
		for range 6 {
			plans := make([]*growPlan, window)
			for i := range plans {
				plans[i] = newGrowPlan()
				docID := next + uint64(i)
				require.NoError(t, g.plan(ctx, plans[i], docID, f.all[docID]))
			}
			for _, plan := range plans {
				write(plan)
			}
			next += uint64(window)
		}
		require.NoError(t, g.cleanCentroidGraph())
		require.NoError(t, g.checkCoverage(), "windows of %d", window)
		t.Logf("windows of %d: %d lists read again, %d taken as the search left them and checked, %d postings chosen again",
			window, g.stats.StaleLists-stale, g.batchVerified-verified, g.stats.PlansReassign-reassigned)
		require.Greater(t, g.batchVerified, verified, "lists are taken from what the search kept")
		if window == 1 {
			require.Equal(t, stale, g.stats.StaleLists, "nothing is written between a search and its own writes")
		}
		if window >= 8 {
			require.Greater(t, g.stats.StaleLists, stale, "writes of the window changed lists a later one had kept")
		}
	}

	// A neighbor that is marked deleted between the search and the writes is
	// not one: the vector has no edge to it.
	plan := newGrowPlan()
	require.NoError(t, g.plan(ctx, plan, next, f.all[next]))
	require.NotEmpty(t, plan.neighbors)
	var gone uint64
	found := false
	for _, u := range plan.neighbors {
		if g.st.side.postingOf[u] != uint32(plan.entry[0]) {
			gone, found = u, true
			break
		}
	}
	require.True(t, found, "the vector has a neighbor outside its posting")
	marked, err := g.remove(gone)
	require.NoError(t, err)
	require.True(t, marked)
	write(plan)
	require.NotContains(t, edgesOfDoc(t, g, next), uint32(gone))
	require.NotContains(t, edgesOfDoc(t, g, gone), uint32(next), "nor does the marked vector take it in")
	next++

	// A search that ran before a vector left the index runs again: its doc
	// id may have been given to another vector since.
	require.NoError(t, g.plan(ctx, plan, next, f.all[next]))
	require.NoError(t, g.clean(ctx, 0, 0))
	require.False(t, g.live[gone])
	require.True(t, g.outdated(plan))
	require.NoError(t, g.plan(ctx, plan, plan.docID, plan.vector))
	write(plan)
	require.NoError(t, g.cleanCentroidGraph())
	require.NoError(t, g.checkCoverage())
	f.inserted = int(next) + 1
	f.vectorsIntact(t, "at the end")
	f.reopen(t)
	require.NoError(t, f.g.cleanCentroidGraph())
	require.NoError(t, f.g.checkCoverage())
}
