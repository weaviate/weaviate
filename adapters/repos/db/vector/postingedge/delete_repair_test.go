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
	"testing"

	"github.com/stretchr/testify/require"
)

// The maintenance pass of v6.1 (bench-report.md, "v6.1: limpieza en dos
// fases"), in its two phases: repair, in which every live vector with enough
// of its edges leading to deleted vectors builds its list again by a search
// that goes through the deleted ones, and eliminate, which takes the deleted
// vectors and the edges to them out and merges the postings.
func TestDeleteTwoPhases(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		// num/den is the share of dead edges from which a vector is
		// repaired: 0/1 is one dead edge or more.
		num, den int
		// mergeUnder and sumWithin are the merges of the second phase.
		mergeUnder, sumWithin int
		share                 float64
		clustered, cosine     bool
	}{
		{name: "one dead edge or more, the merge of v6.0", num: 0, den: 1, mergeUnder: 20, share: 0.3},
		{name: "one dead edge or more, the merge that fills", num: 0, den: 1, mergeUnder: 30, sumWithin: 120, share: 0.3},
		{name: "a quarter or more, the merge of v6.0", num: 1, den: 4, mergeUnder: 20, share: 0.3},
		{name: "a quarter or more, the merge that fills", num: 1, den: 4, mergeUnder: 30, sumWithin: 120, share: 0.3},
		{name: "half or more, the merge of v6.0", num: 1, den: 2, mergeUnder: 20, share: 0.3},
		{name: "a quarter or more, cosine", num: 1, den: 4, mergeUnder: 30, sumWithin: 120, share: 0.3, cosine: true},
		{name: "half deleted", num: 0, den: 1, mergeUnder: 30, sumWithin: 120, share: 0.5},
		{name: "whole postings deleted", num: 1, den: 4, mergeUnder: 30, sumWithin: 120, share: 0.3, clustered: true},
		{name: "a merge that may make any size", num: 1, den: 4, mergeUnder: 60, share: 0.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const n, dims, k, threshold = 4000, 48, 10, 120
			f := newDeleteFixture(t, n, n, dims, tt.cosine, threshold, tt.mergeUnder)
			s, d, st := f.s, f.d, f.s.st
			if tt.clustered {
				gone := 0
				for p := range st.postings.lists {
					if float64(gone) >= tt.share*n {
						break
					}
					for _, docID := range slices.Clone(st.postings.lists[p].docIDs) {
						require.NoError(t, d.remove(docID))
						gone++
					}
				}
			} else {
				f.removeShare(t, tt.share, 13)
			}
			live, deleted := s.liveCount, d.stats.Deleted
			before := d.counts(f.uc.TargetPostingSize)
			require.Positive(t, before.DeadEdges)

			// The first phase: the vectors over the share, and no other.
			need := d.withDeadEdges(tt.num, tt.den)
			require.NotEmpty(t, need)
			require.True(t, slices.IsSorted(need))
			if tt.num == 0 {
				require.GreaterOrEqual(t, len(need), len(d.withDeadEdges(1, 4)), "one dead edge or more holds a quarter or more")
			}
			repaired, err := d.repair(ctx, tt.num, tt.den)
			require.NoError(t, err)
			require.Equal(t, len(need), repaired.Refreshed)
			require.Positive(t, repaired.Work.SearchLoads)
			require.False(t, st.deletedAsSources, "the search goes back to what it was")
			require.False(t, st.skipDeletedEndpoints)
			require.NoError(t, d.checkCoverage())
			require.Empty(t, d.withDeadEdges(tt.num, tt.den), "no vector is left over the share")
			for _, docID := range need {
				l := &st.postings.lists[st.side.postingOf[docID]]
				for _, endpoint := range l.edgesOf(l.member(docID)) {
					require.Falsef(t, d.deleted[endpoint], "the repaired list of doc %d leads to doc %d, which is deleted", docID, endpoint)
				}
			}
			between := d.counts(f.uc.TargetPostingSize)
			require.Equal(t, before.Members, between.Members, "the first phase takes no member out")
			require.Equal(t, before.Postings, between.Postings)
			require.Less(t, between.DeadEdges, before.DeadEdges)
			if tt.num == 0 {
				require.Zero(t, between.DeadEdges, "every list with a dead edge was built again")
			}

			// The second phase.
			postings := len(st.postings.lists)
			require.NoError(t, d.eliminate(ctx, tt.mergeUnder, tt.sumWithin))
			require.NoError(t, d.checkCoverage())
			require.NoError(t, d.checkEliminated())
			require.Equal(t, live, s.liveCount, "the pass loses no live vector")
			after := d.counts(f.uc.TargetPostingSize)
			require.Equal(t, live, after.LiveMembers)
			require.Equal(t, live, after.Members, "no deleted vector takes room")
			require.Zero(t, after.DeadEdges)
			require.Equal(t, deleted, d.stats.DroppedDead, "every deleted member was dropped")
			require.Equal(t, between.DeadEdges, d.stats.DroppedEdges, "the edges the first phase left were taken out")
			require.LessOrEqual(t, after.Postings, between.Postings)
			under := 0
			for p := range st.postings.lists {
				if s.retired[p] {
					continue
				}
				size := len(st.postings.lists[p].docIDs)
				switch {
				case d.received[uint32(p)] && tt.sumWithin > 0:
					require.LessOrEqualf(t, size, tt.sumWithin, "posting %d was merged past the size", p)
				case !d.received[uint32(p)] && p < postings && tt.sumWithin == 0:
					require.GreaterOrEqualf(t, size, tt.mergeUnder, "posting %d was left under the size of the merges", p)
				case !d.received[uint32(p)] && p < postings && size < tt.mergeUnder:
					under++
				}
			}
			require.LessOrEqual(t, under, d.stats.MergesSkipped, "a posting left under the size is one a merge was not made for")
			if tt.sumWithin > 0 {
				require.Zero(t, d.stats.MergeSplits, "a merge within the size is followed by no split")
			}
			require.Positive(t, d.stats.Merges+d.stats.EmptyRetired, "the pass merged something")
			if tt.clustered {
				require.Positive(t, d.stats.EmptyRetired)
			}

			answers := f.answers(t, k)
			require.Greater(t, f.recall(t, answers, k), deleteRecallFloor)

			// A second pass finds nothing to do.
			again, err := d.repair(ctx, tt.num, tt.den)
			require.NoError(t, err)
			require.Zero(t, again.Refreshed)
			rewritten, dropped := d.stats.Rewritten, d.stats.DroppedEdges
			require.NoError(t, d.eliminate(ctx, tt.mergeUnder, tt.sumWithin))
			require.Equal(t, rewritten, d.stats.Rewritten)
			require.Equal(t, dropped, d.stats.DroppedEdges)
			require.NoError(t, d.checkCoverage())
		})
	}
}

// The first phase searches through the deleted vectors: on two indexes that
// are the same, the repair scores more members than the same lists built
// again by a search that skips them, since every deleted member of a posting
// it loads is scored.
func TestDeleteRepairGoesThroughTheDeleted(t *testing.T) {
	ctx := context.Background()
	const n, dims, threshold = 4000, 48, 120
	scanned := func(navigable bool) (int, int) {
		f := newDeleteFixture(t, n, n, dims, false, threshold, 20)
		f.removeShare(t, 0.3, 13)
		if navigable {
			stats, err := f.d.repair(ctx, 1, 4)
			require.NoError(t, err)
			return stats.Refreshed, stats.Work.SearchScanned
		}
		stats, err := f.s.refreshEdges(ctx, f.d.withDeadEdges(1, 4))
		require.NoError(t, err)
		return stats.Refreshed, stats.Work.SearchScanned
	}
	with, withScanned := scanned(true)
	without, withoutScanned := scanned(false)
	require.Equal(t, without, with, "the two build the same lists again")
	require.Greater(t, withScanned, withoutScanned)
}

// What the second phase must leave is checked, and a pass that leaves a
// deleted vector behind is told.
func TestDeleteEliminatedCatches(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		wants string
		harm  func(f *deleteFixture, gone uint64)
	}{
		{name: "nothing wrong"},
		{
			name:  "an edge to a deleted vector",
			wants: "still has an edge",
			harm: func(f *deleteFixture, gone uint64) {
				for p := range f.s.st.postings.lists {
					if l := &f.s.st.postings.lists[p]; len(l.edges) > 0 {
						l.edges[0] = uint32(gone)
						return
					}
				}
			},
		},
		{
			name:  "a deleted vector in a posting",
			wants: "still holds",
			harm: func(f *deleteFixture, gone uint64) {
				for p := range f.s.st.postings.lists {
					if l := &f.s.st.postings.lists[p]; len(l.docIDs) > 0 {
						l.docIDs[0] = gone
						return
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDeleteFixture(t, 2000, 2000, 32, false, 120, 20)
			gone := f.removeShare(t, 0.3, 13)
			_, err := f.d.repair(ctx, 1, 4)
			require.NoError(t, err)
			require.NoError(t, f.d.eliminate(ctx, 20, 0))
			if tt.harm != nil {
				tt.harm(f, gone[0])
			}
			err = f.d.checkEliminated()
			if tt.wants == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wants)
		})
	}
}
