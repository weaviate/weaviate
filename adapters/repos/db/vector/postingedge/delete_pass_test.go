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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// The pass of every cell of v6.2 (bench-report.md, "v6.2: reparación en el
// churn") as the run at 1M calls it: which vectors its first phase takes,
// what its second phase does with the edges to deleted vectors, and what it
// reports of both, over rounds of deletes.
func TestDeletePassByCell(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const n, dims, threshold, rounds = 240, 16, 120, 3
	for _, spec := range memorySpecs {
		t.Run(spec.key, func(t *testing.T) {
			t.Parallel()
			f := newDeleteFixture(t, n, n, dims, false, threshold, 30)
			s, d := f.s, f.d
			d.remember()
			repaired, substituted := 0, 0
			for round := range rounds {
				f.removeShare(t, 0.08, uint64(70+round))
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
	t.Parallel()
	ctx := context.Background()
	const n, dims, threshold, rounds, inserts, sampled = 300, 16, 120, 3, 25, 100

	t.Run("a streaming index after the pass of C", func(t *testing.T) {
		t.Parallel()
		const base = n - rounds*inserts
		f := newDeleteFixture(t, n, base, dims, false, threshold, 30)
		s, d := f.s, f.d
		d.remember()
		inserted := make([]bool, n)
		for round := range rounds {
			f.removeShare(t, 0.08, uint64(90+round))
			for id := base + round*inserts; id < base+(round+1)*inserts; id++ {
				require.NoError(t, s.insert(ctx, uint64(id), f.all[id]))
				inserted[id] = true
			}
			_, err := d.pass(ctx, memorySpecs[3], 30, threshold)
			require.NoError(t, err)
		}
		sample := memorySample(s.live, sampled)
		require.Len(t, sample, sampled)
		require.True(t, slices.IsSorted(sample))
		require.Equal(t, sample, memorySample(s.live, sampled), "the sample is drawn the same every time")
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
		t.Parallel()
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
		sample := memorySample(live, sampled)
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
