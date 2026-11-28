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

//go:build postingedgebench

package postingedge

import (
	"context"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// The tests of the cells of v6.1 and v6.2 (bench-report.md), whose subject
// is deleteIndex, the model of the deletes of the experiments in test code,
// at the sizes they were written for: 3 000 and 4 000 vectors of 48
// dimensions.

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
	require.Positive(t, with, "the repair has lists to build again")
	require.Equal(t, without, with, "the two build the same lists again")
	require.Greater(t, withScanned, withoutScanned)
}

// The pass of every cell of v6.2 (bench-report.md, "v6.2: reparación en el
// churn") as the run at 1M calls it: which vectors its first phase takes,
// what its second phase does with the edges to deleted vectors, and what it
// reports of both, over rounds of deletes.
func TestDeletePassByCell(t *testing.T) {
	ctx := context.Background()
	const n, dims, threshold, rounds = 4000, 48, 120, 4
	for _, spec := range memorySpecs {
		t.Run(spec.key, func(t *testing.T) {
			f := newDeleteFixture(t, n, n, dims, false, threshold, 30)
			s, d := f.s, f.d
			d.remember()
			repaired, substituted := 0, 0
			for round := range rounds {
				f.removeShare(t, 0.06, uint64(70+round))
				live := s.liveCount
				before := d.counts(f.uc.TargetPostingSize)

				// Who the first phase must take, with the lists as they are.
				var want []uint64
				switch {
				case spec.counted:
					var err error
					want, err = d.overShare(memoryNum, memoryDen, spec.substitute)
					require.NoError(t, err)
				case !spec.substitute:
					want = d.withDeadEdges(memoryNum, memoryDen)
				}
				// The postings that hold a deleted vector are of the second
				// phase whatever the first one does.
				holding := 0
				for p := range s.st.postings.lists {
					if !s.retired[p] && slices.ContainsFunc(s.st.postings.lists[p].docIDs,
						func(docID uint64) bool { return d.deleted[docID] }) {
						holding++
					}
				}
				pending := d.pendingPostings()
				dropped, without, merges, trial := d.stats.DroppedEdges, d.sub.Without, d.stats.Merges, d.trial

				pass, err := d.pass(ctx, spec, 30, threshold)
				require.NoError(t, err)

				require.NoError(t, d.checkCoverage())
				require.NoError(t, d.checkEliminated())
				require.NoError(t, d.checkEdgeLists())
				require.Equal(t, live, s.liveCount)

				require.Equal(t, len(want), pass.Repair.Refreshed, "round %d: the lists built again", round)
				for _, docID := range want {
					require.Zero(t, d.lost[docID], "doc %d was repaired", docID)
					require.True(t, d.rebuilt[docID], "doc %d was repaired", docID)
				}
				require.GreaterOrEqual(t, pass.SecondPhasePostings, holding)
				require.LessOrEqual(t, pass.SecondPhasePostings, pending)
				require.Equal(t, d.stats.Merges-merges, pass.Merges)
				require.Equal(t, d.memory(), pass.Memory)

				if spec.substitute {
					// Nothing is taken out by the phase of v6.1: every edge
					// to a deleted vector was substituted or dropped first.
					require.Equal(t, dropped, d.stats.DroppedEdges)
					require.Equal(t, d.sub.Without-without, pass.Dropped)
					require.Equal(t, pass.Substitution.Without, pass.Dropped)
				} else {
					require.Zero(t, pass.Substitution)
					require.Equal(t, d.stats.DroppedEdges-dropped, pass.Dropped)
				}
				require.LessOrEqual(t, pass.Substitution.Substituted+pass.Dropped, before.DeadEdges)
				if spec.counted && spec.substitute {
					require.Equal(t, d.trial.minus(trial), pass.Rehearsal)
					require.Positive(t, pass.Rehearsal.CodeDistances)
				} else {
					require.Zero(t, pass.Rehearsal)
				}
				if !spec.counted && spec.substitute {
					// B searches nothing: every dead edge is substituted or
					// dropped, and the lists change by nothing else.
					require.Zero(t, pass.Repair.Refreshed)
					require.Equal(t, before.DeadEdges, pass.Substitution.Substituted+pass.Dropped)
					require.Equal(t, before.Edges-pass.Dropped, d.counts(f.uc.TargetPostingSize).Edges)
				}
				repaired += pass.Repair.Refreshed
				substituted += pass.Substitution.Substituted
			}
			// Every cell does what it is for.
			if spec.substitute {
				require.Positive(t, substituted)
			} else {
				require.Positive(t, repaired)
			}
			t.Logf("%s: %d lists built again and %d edges substituted in %d rounds", spec.key, repaired, substituted, rounds)
		})
	}
}

// The edge recall of the sample of v6.2 is the edge recall of v2.x: the
// sample counted against a brute force computed once gives what
// streamIndex.edgeRecall gives, in a streaming index with deletes and in a
// batch index, and its groups split it.
func TestDeleteEdgesOfSample(t *testing.T) {
	ctx := context.Background()
	const n, dims, threshold = 3000, 48, 120

	t.Run("a streaming index after the pass of C", func(t *testing.T) {
		f := newDeleteFixture(t, n, n-300, dims, false, threshold, 30)
		s, d := f.s, f.d
		d.remember()
		inserted := make([]bool, n)
		for round := range 3 {
			f.removeShare(t, 0.08, uint64(90+round))
			for id := n - 300 + round*100; id < n-300+(round+1)*100; id++ {
				require.NoError(t, s.insert(ctx, uint64(id), f.all[id]))
				inserted[id] = true
			}
			_, err := d.pass(ctx, memorySpecs[3], 30, threshold)
			require.NoError(t, err)
		}
		sample := memorySample(s.live, 400)
		require.Len(t, sample, 400)
		require.True(t, slices.IsSorted(sample))
		require.Equal(t, sample, memorySample(s.live, 400), "the sample is drawn the same every time")
		nearest, err := memoryNearest(ctx, testLogger(), f.provider, s.vectorOf, s.live, sample, s.neighbors)
		require.NoError(t, err)
		var top topK
		for i, docID := range sample {
			require.True(t, s.live[docID])
			want, err := s.nearestLive(docID, &top)
			require.NoError(t, err)
			require.Equal(t, want, nearest[i], "the nearest live neighbors of doc %d", docID)
		}

		// A vector with a substitute that a search has built the list of
		// before is of the group of the substitutes: marked by hand, since
		// in an index this small C repairs none.
		for _, docID := range sample {
			l := &s.st.postings.lists[s.st.side.postingOf[docID]]
			if !inserted[docID] && len(d.substitutesOf(docID, l.edgesOf(l.member(docID)))) > 0 {
				d.rebuilt[docID] = true
				break
			}
		}

		got, err := d.edgesOfSample(sample, nearest, inserted)
		require.NoError(t, err)
		want, err := s.edgeRecall(ctx, testLogger(), sample)
		require.NoError(t, err)
		require.Equal(t, want, got.All.edgeCounts)
		require.InDelta(t, want.recall(), got.All.Recall, 1e-12)
		require.InDelta(t, want.postingRecall(), got.All.PostingRecall, 1e-12)
		require.InDelta(t, want.degree(), got.All.Degree, 1e-12)

		// The groups split the sample, and each holds what it says.
		var sum edgeCounts
		subs, trueSubs := 0, 0
		names := map[string]int{}
		for _, group := range got.Groups {
			sum.add(group.edgeCounts)
			subs += group.Substitutes
			trueSubs += group.SubstitutesTrue
			names[group.Name] = group.Vectors
			require.LessOrEqual(t, group.SubstitutesTrue, group.Substitutes)
		}
		require.Equal(t, got.All.edgeCounts, sum)
		require.Equal(t, got.All.Substitutes, subs)
		require.Equal(t, got.All.SubstitutesTrue, trueSubs)
		byHand := map[string]int{}
		handSubs, handTrue := 0, 0
		// trueOf is a substitute that is a true edge, and of whom, of one
		// that is not in the sample: its place in the map is moved by hand
		// below, and the vectors of the sample must be where the map has
		// them.
		trueOf, trueSub, movable := uint64(0), uint32(0), false
		for i, docID := range sample {
			p := s.st.side.postingOf[docID]
			l := &s.st.postings.lists[p]
			given := d.substitutesOf(docID, l.edgesOf(l.member(docID)))
			handSubs += len(given)
			for _, sub := range given {
				if s.st.side.postingOf[sub] != p &&
					slices.ContainsFunc(nearest[i], func(nb scored) bool { return nb.id == uint64(sub) }) {
					handTrue++
					if _, sampled := slices.BinarySearch(sample, uint64(sub)); !sampled {
						trueOf, trueSub, movable = docID, sub, true
					}
				}
			}
			switch {
			case inserted[docID]:
				byHand[memoryGroupNames[0]]++
			case len(given) > 0:
				byHand[memoryGroupNames[1]]++
			case d.rebuilt[docID]:
				byHand[memoryGroupNames[2]]++
			default:
				byHand[memoryGroupNames[3]]++
			}
		}
		require.Equal(t, byHand, names)
		require.Equal(t, handSubs, got.All.Substitutes)
		require.Equal(t, handTrue, got.All.SubstitutesTrue)
		require.Positive(t, handTrue, "the sample holds a substitute that is a true edge")
		require.True(t, movable, "a substitute that is a true edge is out of the sample")

		// A substitute that has come to be in the posting of its vector is
		// not a true edge of it any more: none is inside the posting.
		was := s.st.side.postingOf[trueSub]
		s.st.side.postingOf[trueSub] = s.st.side.postingOf[trueOf]
		moved, err := d.edgesOfSample(sample, nearest, inserted)
		s.st.side.postingOf[trueSub] = was
		require.NoError(t, err)
		require.Less(t, moved.All.SubstitutesTrue, got.All.SubstitutesTrue)
		require.Positive(t, names[memoryGroupNames[0]], "the sample holds inserted vectors")
		require.Positive(t, names[memoryGroupNames[1]], "the sample holds vectors with a substitute")
		require.Positive(t, got.All.Substitutes)

		// A vector of the sample that is not live stops it.
		gone := f.removeShare(t, 0.5, 99)
		at := slices.IndexFunc(sample, func(docID uint64) bool { return slices.Contains(gone, docID) })
		require.GreaterOrEqual(t, at, 0)
		_, err = d.edgesOfSample(sample, nearest, inserted)
		require.ErrorContains(t, err, "is not live")
	})

	t.Run("a batch index", func(t *testing.T) {
		vectors := clusteredVectors(n, dims, deleteClusters(n), 6011)
		provider := distancer.NewL2SquaredProvider()
		uc := smallConfig()
		uc.Distance = "l2-squared"
		index := buildTestIndex(t, vectors, provider, uc)
		batch := index.state.Load()
		vectorOf := func(id uint64) []float32 { return vectors[id] }
		live := make([]bool, n)
		for i := range live {
			live[i] = true
		}
		sample := memorySample(live, 300)
		nearest, err := memoryNearest(ctx, testLogger(), provider, vectorOf, live, sample, uc.Edges)
		require.NoError(t, err)
		got, err := edgesOfBatch(batch, n, sample, nearest)
		require.NoError(t, err)

		s, err := newStreamIndex(index, n, streamConfig{threshold: threshold, inverse: true, seed: 7}, vectorOf)
		require.NoError(t, err)
		want, err := s.edgeRecall(ctx, testLogger(), sample)
		require.NoError(t, err)
		require.Equal(t, want, got.All.edgeCounts)
		require.Positive(t, got.All.Recall)
		require.Empty(t, got.Groups)

		// Keyed by doc id it is not a batch index any more, and a doc id
		// the index does not hold is told.
		_, err = edgesOfBatch(s.st, n, sample, nearest)
		require.ErrorContains(t, err, "keyed by doc id")
		_, err = edgesOfBatch(batch, n+1, []uint64{uint64(n)}, nearest[:1])
		require.ErrorContains(t, err, "is not in the index")
	})
}

// deleteRoundsCell is a pass of v6.2 run over rounds of deletes.
type deleteRoundsCell struct {
	name                  string
	remembers, searches   bool
	substitutes, rehearse bool
}

// deleteRoundsCells are the passes of v6.2: the pass of v6.1, which takes its
// share anew at every pass, the one that remembers what a list has lost (A),
// the one that substitutes (B), and the one that substitutes and searches for
// the lists that lose too much with no substitute (C).
var deleteRoundsCells = []deleteRoundsCell{
	{name: "the pass of v6.1", searches: true},
	{name: "A", remembers: true, searches: true},
	{name: "B", remembers: true, substitutes: true},
	{name: "C", remembers: true, searches: true, substitutes: true, rehearse: true},
}

// deleteRoundsResult is what a cell leaves after its rounds: the lists a
// search built again, and the edges a live vector has at the end.
type deleteRoundsResult struct {
	repaired int
	edges    float64
}

// deleteRepairRounds runs the pass of tt over rounds of deletes of a share
// of the live vectors, on an index of n vectors of its own, the same deletes
// for every cell, and checks after every round what the pass must leave.
func deleteRepairRounds(t *testing.T, n, dims, rounds int, share float64, tt deleteRoundsCell) deleteRoundsResult {
	t.Helper()
	ctx := context.Background()
	const k, threshold = 10, 120
	f := newDeleteFixture(t, n, n, dims, false, threshold, 30)
	s, d, st := f.s, f.d, f.s.st
	if tt.remembers {
		d.remember()
	}
	var out deleteRoundsResult
	for round := range rounds {
		f.removeShare(t, share, uint64(40+round))
		live := s.liveCount

		// Who the first phase must take, worked out here, and what
		// every vector has lost and is about to lose.
		lostBefore := slices.Clone(d.lost)
		dead := map[uint64]int{}
		var want []uint64
		for p := range st.postings.lists {
			l := &st.postings.lists[p]
			for i, docID := range l.docIDs {
				if d.deleted[docID] {
					continue
				}
				edges := l.edgesOf(i)
				for _, endpoint := range edges {
					if d.deleted[endpoint] {
						dead[docID]++
					}
				}
				if tt.remembers && !tt.rehearse {
					lost := int(d.lost[docID])
					if gone := lost + dead[docID]; gone > 0 && 4*gone >= len(edges)+lost {
						want = append(want, docID)
					}
				}
			}
		}
		slices.Sort(want)

		var repaired []uint64
		switch {
		case !tt.searches:
		case !tt.remembers:
			repaired = d.withDeadEdges(1, 4)
			stats, err := d.repair(ctx, 1, 4)
			require.NoError(t, err)
			require.Equal(t, len(repaired), stats.Refreshed)
		default:
			var err error
			repaired, err = d.overShare(1, 4, tt.rehearse)
			require.NoError(t, err)
			if !tt.rehearse {
				require.Equal(t, want, repaired, "round %d", round)
			} else {
				for _, docID := range repaired {
					require.Positive(t, int(d.lost[docID])+dead[docID])
				}
			}
			stats, err := d.repairOver(ctx, 1, 4, tt.rehearse)
			require.NoError(t, err)
			require.Equal(t, len(repaired), stats.Refreshed)
		}
		out.repaired += len(repaired)

		// The edges to deleted vectors the first phase leaves. A
		// vector it did not repair can have fewer than before: a
		// repaired vector that takes its place in a list puts the
		// worst edge out, and that is not an edge lost.
		left := map[uint64]int{}
		for p := range st.postings.lists {
			l := &st.postings.lists[p]
			for i, docID := range l.docIDs {
				if d.deleted[docID] {
					continue
				}
				for _, endpoint := range l.edgesOf(i) {
					if d.deleted[endpoint] {
						left[docID]++
					}
				}
				require.LessOrEqual(t, left[docID], dead[docID])
			}
		}

		without := d.sub.Without
		if tt.substitutes {
			require.NoError(t, d.eliminateSubstituting(ctx, 30, threshold))
		} else {
			require.NoError(t, d.eliminate(ctx, 30, threshold))
		}
		require.NoError(t, d.checkCoverage())
		require.NoError(t, d.checkEliminated())
		require.NoError(t, d.checkEdgeLists())
		require.Equal(t, live, s.liveCount)

		// What every vector remembers after the pass.
		if tt.remembers {
			sum := 0
			for docID, isLive := range s.live {
				if !isLive {
					continue
				}
				had := int(lostBefore[docID])
				if _, wasRepaired := slices.BinarySearch(repaired, uint64(docID)); wasRepaired {
					require.Zerof(t, left[uint64(docID)], "doc %d was repaired in round %d", docID, round)
					had = 0
				}
				lost := int(d.lost[docID])
				if !tt.substitutes {
					require.Equalf(t, had+left[uint64(docID)], lost, "doc %d in round %d", docID, round)
					continue
				}
				require.GreaterOrEqual(t, lost, had)
				require.LessOrEqual(t, lost, had+left[uint64(docID)])
				sum += lost - had
			}
			if tt.substitutes {
				require.Equal(t, d.sub.Without-without, sum, "the edges lost in round %d are those left with no substitute", round)
			}
		}
	}
	counts := d.counts(f.uc.TargetPostingSize)
	out.edges = float64(counts.Edges) / float64(counts.Live)
	t.Logf("%d lists built again in %d rounds, %.2f edges a vector at the end; %d edges given a substitute and %d left with none",
		out.repaired, rounds, out.edges, d.sub.Substituted, d.sub.Without)

	answers := f.answers(t, k)
	require.Greater(t, f.recall(t, answers, k), deleteRecallFloor)
	return out
}

// The passes of v6.2 over rounds of deletes, each cell on an index of its
// own with the same deletes, over 4 000 vectors of 48 dimensions and six
// rounds of 5 %: every cell leaves after every round what it must, and,
// compared with each other, a list that remembers what it lost is repaired
// sooner and left longer, the substitutes keep the lists longer with no
// search, and C asks the search for less than A.
func TestDeleteRepairsInRounds(t *testing.T) {
	results := make([]deleteRoundsResult, len(deleteRoundsCells))
	for i, cell := range deleteRoundsCells {
		t.Run(cell.name, func(t *testing.T) {
			results[i] = deleteRepairRounds(t, 4000, 48, 6, 0.05, cell)
		})
	}
	if t.Failed() {
		return
	}
	old, a, b, c := results[0], results[1], results[2], results[3]
	require.Greater(t, a.repaired, old.repaired, "a list that remembers what it lost is repaired sooner")
	require.Greater(t, a.edges, old.edges, "and the lists are left longer")
	require.Zero(t, b.repaired)
	require.Greater(t, b.edges, old.edges, "the substitutes keep the lists longer")
	require.Less(t, c.repaired, a.repaired, "the search is asked for less when there are substitutes")
	require.GreaterOrEqual(t, c.edges, b.edges)
}

// The search of cell C is a net under the substitution: it takes the vectors
// that lose a share of their list with no substitute, where A takes those
// that lose it at all. Where whole postings are deleted many edges have no
// substitute to take, since the list of a deleted vector leads to deleted
// vectors, and the net has work to do.
func TestDeleteSubstituteNet(t *testing.T) {
	ctx := context.Background()
	const n, dims, k, threshold = 4000, 48, 10, 120
	tests := []struct {
		name     string
		rehearse bool
	}{
		{name: "A: every edge to a deleted vector counts"},
		{name: "C: the edges with no substitute count", rehearse: true},
	}
	repaired := map[bool]int{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDeleteFixture(t, n, n, dims, false, threshold, 30)
			s, d, st := f.s, f.d, f.s.st
			d.remember()
			gone := 0
			for p := range st.postings.lists {
				if float64(gone) >= 0.3*n {
					break
				}
				for _, docID := range slices.Clone(st.postings.lists[p].docIDs) {
					require.NoError(t, d.remove(docID))
					gone++
				}
			}
			live := s.liveCount

			// Who the first phase must take: of every live vector, the edges
			// it is about to lose, which with the rehearsal are those the
			// substitution would leave with no substitute.
			var (
				want    []uint64
				scratch substituteStats
			)
			for p := range st.postings.lists {
				l := &st.postings.lists[p]
				for i, docID := range l.docIDs {
					if d.deleted[docID] {
						continue
					}
					edges := l.edgesOf(i)
					pending := 0
					for _, endpoint := range edges {
						if d.deleted[endpoint] {
							pending++
						}
					}
					if tt.rehearse && pending > 0 {
						kept, subs, without, err := d.substitutesFor(docID, uint32(p), edges, &scratch)
						require.NoError(t, err)
						require.Equal(t, pending, len(subs)+without)
						require.Equal(t, len(edges), len(kept)+pending)
						pending = without
					}
					if pending > 0 && 4*pending >= len(edges) {
						want = append(want, docID)
					}
				}
			}
			slices.Sort(want)
			require.NotEmpty(t, want)

			stats, err := d.repairOver(ctx, 1, 4, tt.rehearse)
			require.NoError(t, err)
			require.Equal(t, len(want), stats.Refreshed)
			for _, docID := range want {
				require.Truef(t, d.rebuilt[docID], "doc %d was to be repaired", docID)
			}
			require.Equal(t, len(want), d.memory().Rebuilt)
			if tt.rehearse {
				require.Equal(t, scratch.CodeDistances, d.trial.CodeDistances, "the rehearsal estimates what the substitution would")
				require.Zero(t, d.sub.CodeDistances, "and changes nothing")
				require.NoError(t, d.eliminateSubstituting(ctx, 30, threshold))
				require.Positive(t, d.sub.Substituted)
			} else {
				require.Zero(t, d.trial.CodeDistances)
				require.NoError(t, d.eliminate(ctx, 30, threshold))
			}
			require.NoError(t, d.checkCoverage())
			require.NoError(t, d.checkEliminated())
			require.NoError(t, d.checkEdgeLists())
			require.Equal(t, live, s.liveCount)
			for _, docID := range want {
				require.Zerof(t, d.lost[docID], "doc %d was repaired", docID)
			}
			repaired[tt.rehearse] = len(want)
			t.Logf("%d lists built again; %d edges given a substitute and %d left with none", len(want), d.sub.Substituted, d.sub.Without)

			answers := f.answers(t, k)
			require.Greater(t, f.recall(t, answers, k), deleteRecallFloor)
		})
	}
	if len(repaired) != len(tests) {
		return
	}
	require.Positive(t, repaired[true], "the net has lists to build again")
	require.Less(t, repaired[true], repaired[false], "and fewer than with no substitution")
}

// overShare lists, by doc id, the live vectors the share of v6.2 takes: those
// for which what they have lost since their list was built, and what they are
// about to lose, are num/den or more of their list as it would be had it lost
// nothing, and are at least one edge. What a vector is about to lose is its
// edges to deleted vectors, or with rehearse those of them the substitution
// would leave without a substitute, with the lists as they are now.
func (d *deleteIndex) overShare(num, den int, rehearse bool) ([]uint64, error) {
	if d.lost == nil {
		return nil, errors.New("the index keeps no count of the edges lost")
	}
	var out []uint64
	for p := range d.s.st.postings.lists {
		l := &d.s.st.postings.lists[p]
		for i, docID := range l.docIDs {
			if d.deleted[docID] {
				continue
			}
			edges := l.edgesOf(i)
			pending := 0
			for _, endpoint := range edges {
				if d.deleted[endpoint] {
					pending++
				}
			}
			if rehearse && pending > 0 {
				var err error
				if _, _, pending, err = d.substitutesFor(docID, uint32(p), edges, &d.trial); err != nil {
					return nil, err
				}
			}
			lost := int(d.lost[docID])
			if gone := lost + pending; gone > 0 && den*gone >= num*(len(edges)+lost) {
				out = append(out, docID)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}

// repairOver is the first phase of the pass with its memory: every vector
// overShare lists when it starts builds its list again by search, with the
// deleted vectors navigable, as repair does.
func (d *deleteIndex) repairOver(ctx context.Context, num, den int, rehearse bool) (refreshStats, error) {
	st := d.s.st
	sources, skip := st.deletedAsSources, st.skipDeletedEndpoints
	st.deletedAsSources, st.skipDeletedEndpoints = true, false
	defer func() { st.deletedAsSources, st.skipDeletedEndpoints = sources, skip }()

	docIDs, err := d.overShare(num, den, rehearse)
	if err != nil {
		return refreshStats{}, err
	}
	stats, err := d.s.refreshEdges(ctx, docIDs)
	if err != nil {
		return stats, err
	}
	return stats, d.checkLists(docIDs, "repair")
}

// memorySpec is a cell of v6.2: what the first phase of its pass repairs and
// what the second does with an edge that leads to a deleted vector.
type memorySpec struct {
	key, name string
	// counted has the first phase take its share over what a vector has
	// lost since its list was built (A and C). Without it, and without
	// substitute, the share is taken over the dead edges of the pass alone,
	// as v6.1 does.
	counted bool
	// substitute has the second phase give such an edge a substitute (B and
	// C). A cell that substitutes and does not count has no first phase (B).
	substitute bool
	// mine marks the cell the order did not ask for: the reference.
	mine bool
}

// memorySpecs are the cells of v6.2, in the order they run.
var memorySpecs = []memorySpec{
	{key: "v61", name: "the pass of v6.1: repaired with a quarter of the list dead in the pass, edges taken out", mine: true},
	{key: "A", name: "A: repaired with a quarter of the list lost since it was built, edges taken out", counted: true},
	{key: "B", name: "B: no repair, edges substituted", substitute: true},
	{
		key: "C", name: "C: edges substituted, repaired with a quarter of the list lost with no substitute",
		counted: true, substitute: true,
	},
}

// The share of the first phase of every cell of v6.2, a quarter, and the
// vectors of the sample its edge recall is counted on.
const (
	memoryNum, memoryDen = 1, 4
	memorySampleSize     = 2000
)

// memoryPass is one maintenance pass of a cell of v6.2.
type memoryPass struct {
	// Repair is its first phase: the lists a search built again.
	Repair refreshStats `json:"repair"`
	// SecondPhasePostings counts the postings its second phase reads and
	// writes: those that hold a deleted vector or a list that changes.
	SecondPhasePostings int `json:"postingsOfSecondPhase"`
	// Substitution is what the substitution of its second phase did, and
	// Rehearsal what the rehearsal of cell C did before its first phase,
	// which changes nothing.
	Substitution substituteStats `json:"substitution"`
	Rehearsal    substituteStats `json:"rehearsal"`
	// Dropped counts the edges to deleted vectors taken out with nothing in
	// their place, and Merges the postings merged into another.
	Dropped int `json:"edgesDropped"`
	Merges  int `json:"merges"`
	// Memory is what the index remembers of its live vectors after the pass.
	Memory memoryCounts `json:"memory"`
}

// pass runs one maintenance pass as the cell does it: its first phase, if it
// has one, and its second, with the merges of the winner of v6.1.
func (d *deleteIndex) pass(ctx context.Context, spec memorySpec, mergeUnder, sumWithin int) (memoryPass, error) {
	var out memoryPass
	sub, trial, dropped, merges := d.sub, d.trial, d.stats.DroppedEdges, d.stats.Merges
	var err error
	switch {
	case spec.counted:
		out.Repair, err = d.repairOver(ctx, memoryNum, memoryDen, spec.substitute)
	case !spec.substitute:
		out.Repair, err = d.repair(ctx, memoryNum, memoryDen)
	}
	if err != nil {
		return out, errors.Wrap(err, "the first phase")
	}
	out.SecondPhasePostings = d.pendingPostings()
	if spec.substitute {
		err = d.eliminateSubstituting(ctx, mergeUnder, sumWithin)
	} else if err = d.eliminate(ctx, mergeUnder, sumWithin); err == nil {
		err = d.checkEdgeLists()
	}
	if err != nil {
		return out, errors.Wrap(err, "the second phase")
	}
	out.Substitution, out.Rehearsal = d.sub.minus(sub), d.trial.minus(trial)
	out.Dropped = d.stats.DroppedEdges - dropped + out.Substitution.Without
	out.Merges = d.stats.Merges - merges
	out.Memory = d.memory()
	return out, nil
}

// memorySample draws the vectors the edge recall of a stage of v6.2 is
// counted on: size of the live ones, with a fixed seed, by doc id.
func memorySample(live []bool, size int) []uint64 {
	var ids []uint64
	for id, isLive := range live {
		if isLive {
			ids = append(ids, uint64(id))
		}
	}
	rand.New(rand.NewPCG(8, 8)).Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	ids = ids[:min(size, len(ids))]
	slices.Sort(ids)
	return ids
}

// memoryNearest returns, for every vector of the sample, its neighbors
// nearest among the live vectors, itself left out, nearest first, by brute
// force: what streamIndex.nearestLive returns for it.
func memoryNearest(ctx context.Context, logger logrus.FieldLogger, provider distancer.Provider,
	vectorOf func(uint64) []float32, live []bool, sample []uint64, neighbors int,
) ([][]scored, error) {
	out := make([][]scored, len(sample))
	err := parallelChunks(ctx, logger, len(sample), 1, func(_, start, end int) error {
		var nearest topK
		for i := start; i < end; i++ {
			from := vectorOf(sample[i])
			nearest.reset(neighbors)
			for candidate, isLive := range live {
				if !isLive || uint64(candidate) == sample[i] {
					continue
				}
				dist, err := provider.SingleDist(from, vectorOf(uint64(candidate)))
				if err != nil {
					return err
				}
				nearest.push(uint64(candidate), dist)
			}
			out[i] = slices.Clone(nearest.sorted())
		}
		return nil
	})
	return out, err
}

// memoryGroup is the edge recall of the vectors of the sample that are of
// one group.
type memoryGroup struct {
	Name string `json:"name"`
	edgeCounts
	Recall        float64 `json:"recall"`
	PostingRecall float64 `json:"postingRecall"`
	Degree        float64 `json:"degree"`
	// Substitutes counts the edges of the group that are substitutes still
	// in the list they were given to, and SubstitutesTrue those of them
	// that are true edges: among the nearest live neighbors of their vector
	// and outside its posting.
	Substitutes     int `json:"substitutes"`
	SubstitutesTrue int `json:"substitutesTrue"`
}

func (g *memoryGroup) read() {
	g.Recall, g.PostingRecall, g.Degree = g.recall(), g.postingRecall(), g.degree()
}

// memoryEdges is the edge recall of v2.x on the sample of a stage of v6.2:
// of the nearest live neighbors of a vector, those outside its posting are
// its true edges, and the recall is how many of them are in its list.
type memoryEdges struct {
	Sample int         `json:"sample"`
	All    memoryGroup `json:"all"`
	// Groups splits the sample: the vectors inserted in the churn; of the
	// others, those whose list holds a substitute; of the rest, those whose
	// list a search built again; and those left, whose list is the one of
	// the batch build, with the edges it may have lost.
	Groups []memoryGroup `json:"groups,omitempty"`
}

// The groups of memoryEdges, in the order a vector is tried against them.
var memoryGroupNames = []string{
	"inserted in the churn", "with a substitute", "repaired by search", "neither repaired nor with a substitute",
}

// edgesOfSample counts the vectors of the sample as they are now, against
// their nearest live neighbors, in all and by group. inserted marks the
// vectors inserted in the churn, and is nil without one.
func (d *deleteIndex) edgesOfSample(sample []uint64, nearest [][]scored, inserted []bool) (memoryEdges, error) {
	st := d.s.st
	out := memoryEdges{Sample: len(sample), All: memoryGroup{Name: "all"}}
	groups := make([]memoryGroup, len(memoryGroupNames))
	for g := range groups {
		groups[g].Name = memoryGroupNames[g]
	}
	for i, docID := range sample {
		if !d.s.live[docID] {
			return out, errors.Errorf("doc %d of the sample is not live", docID)
		}
		p := st.side.postingOf[docID]
		l := &st.postings.lists[p]
		m := l.member(docID)
		if m < 0 {
			return out, errors.Errorf("doc %d is not in posting %d, where the map has it", docID, p)
		}
		edges := l.edgesOf(m)
		subs := d.substitutesOf(docID, edges)
		g := 3
		switch {
		case inserted != nil && inserted[docID]:
			g = 0
		case len(subs) > 0:
			g = 1
		case d.rebuilt != nil && d.rebuilt[docID]:
			g = 2
		}
		counts := countEdgesIn(st.side.postingOf, p, edges, nearest[i])
		trueSubs := 0
		for _, sub := range subs {
			if st.side.postingOf[sub] != p &&
				slices.ContainsFunc(nearest[i], func(nb scored) bool { return nb.id == uint64(sub) }) {
				trueSubs++
			}
		}
		for _, group := range []*memoryGroup{&out.All, &groups[g]} {
			group.add(counts)
			group.Substitutes += len(subs)
			group.SubstitutesTrue += trueSubs
		}
	}
	out.All.read()
	for g := range groups {
		if groups[g].Vectors > 0 {
			groups[g].read()
			out.Groups = append(out.Groups, groups[g])
		}
	}
	return out, nil
}

// edgesOfBatch counts the vectors of the sample in a batch index, which is
// keyed by slot, against their nearest live neighbors.
func edgesOfBatch(st *state, keys int, sample []uint64, nearest [][]scored) (memoryEdges, error) {
	store := st.postings
	out := memoryEdges{Sample: len(sample), All: memoryGroup{Name: "all"}}
	if store.lists != nil {
		return out, errors.New("the index is keyed by doc id")
	}
	postingOf, slotOf := make([]uint32, keys), make([]uint32, keys)
	for i := range postingOf {
		postingOf[i] = unplaced
	}
	for p := range store.postings() {
		for slot := store.start[p]; slot < store.start[p+1]; slot++ {
			docID := store.docIDs[slot]
			if docID >= uint64(keys) {
				return out, errors.Errorf("doc id %d is past the %d keys", docID, keys)
			}
			postingOf[docID], slotOf[docID] = uint32(p), slot
		}
	}
	for i, docID := range sample {
		p := postingOf[docID]
		if p == unplaced {
			return out, errors.Errorf("doc %d of the sample is not in the index", docID)
		}
		slots := store.edgesOf(slotOf[docID])
		edges := make([]uint32, len(slots))
		for e, endpoint := range slots {
			edges[e] = uint32(store.docIDs[endpoint])
		}
		out.All.add(countEdgesIn(postingOf, p, edges, nearest[i]))
	}
	out.All.read()
	return out, nil
}
