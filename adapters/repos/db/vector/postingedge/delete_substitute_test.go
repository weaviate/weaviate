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

// The local substitution of v6.2 (bench-report.md, "v6.2: reparación en el
// churn"), cell B: the second phase gives every edge to a deleted vector a
// substitute out of the list of that vector, with no search.
func TestDeleteSubstitutes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tests := []struct {
		name              string
		share             float64
		clustered, cosine bool
	}{
		{name: "30 % deleted, cosine", share: 0.3, cosine: true},
		{name: "half deleted", share: 0.5},
		{name: "whole postings deleted", share: 0.3, clustered: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const n, dims, k, threshold = 240, 16, 10, 120
			f := newDeleteFixture(t, n, n, dims, tt.cosine, threshold, 30)
			s, d, st := f.s, f.d, f.s.st
			d.remember()
			require.NoError(t, d.checkEdgeLists(), "the lists of the batch")
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

			// What a vector with one edge to a deleted vector must end with,
			// worked out apart from the pass: the edges it keeps and, last,
			// the nearest by its code of the endpoints of the deleted vector
			// that it may take.
			expected := map[uint64][]uint32{}
			gains := 0
			for p := range st.postings.lists {
				l := &st.postings.lists[p]
				for i, docID := range l.docIDs {
					if d.deleted[docID] {
						continue
					}
					var kept, dead []uint32
					for _, endpoint := range l.edgesOf(i) {
						if d.deleted[endpoint] {
							dead = append(dead, endpoint)
						} else {
							kept = append(kept, endpoint)
						}
					}
					if len(dead) != 1 {
						continue
					}
					from := &st.postings.lists[st.side.postingOf[dead[0]]]
					var (
						best     uint32
						bestDist float32
						found    bool
					)
					for _, candidate := range from.edgesOf(from.member(uint64(dead[0]))) {
						if d.deleted[candidate] || uint64(candidate) == docID || st.side.postingOf[candidate] == uint32(p) || slices.Contains(kept, candidate) {
							continue
						}
						dist, err := d.codeDistance(st.side.code(uint32(docID)), st.side.code(candidate))
						require.NoError(t, err)
						if !found || dist < bestDist {
							best, bestDist, found = candidate, dist, true
						}
					}
					if found {
						kept = append(kept, best)
						gains++
					}
					expected[docID] = kept
				}
			}
			require.NotEmpty(t, expected)
			require.Positive(t, gains, "some vector has a substitute to take")

			pending := d.pendingPostings()
			require.Positive(t, pending)
			require.NoError(t, d.eliminateSubstituting(ctx, 30, threshold))
			require.NoError(t, d.checkCoverage())
			require.NoError(t, d.checkEliminated())
			require.NoError(t, d.checkEdgeLists())
			require.Zero(t, d.trial, "the substitution counts nothing as a rehearsal")
			require.Equal(t, live, s.liveCount, "the pass loses no live vector")
			after := d.counts(f.uc.TargetPostingSize)
			require.Equal(t, live, after.Members, "no deleted vector takes room")
			require.Zero(t, after.DeadEdges)
			require.Equal(t, deleted, d.stats.DroppedDead)
			require.Zero(t, d.stats.DroppedEdges, "the second phase found no edge left to take out")

			// Every edge to a deleted vector was given a substitute or taken
			// out, and the edges that are left add up.
			require.Positive(t, d.sub.Substituted)
			require.Equal(t, before.DeadEdges, d.sub.Substituted+d.sub.Without)
			t.Logf("%d of %d edges to deleted vectors given a substitute, with %.1f candidates and %.1f distances between codes an edge",
				d.sub.Substituted, before.DeadEdges, float64(d.sub.Candidates)/float64(before.DeadEdges), float64(d.sub.CodeDistances)/float64(before.DeadEdges))
			require.Equal(t, before.Edges-d.sub.Without, after.Edges)
			require.LessOrEqual(t, d.sub.PostingsWritten, pending)
			require.Zero(t, d.sub.Missing, "every deleted vector was still in its posting")
			require.GreaterOrEqual(t, d.sub.Candidates, d.sub.CodeDistances)
			memory := d.memory()
			require.Equal(t, d.sub.Without, memory.Lost, "an edge taken out with no substitute is counted as lost")
			require.Equal(t, d.sub.Substituted, memory.SubstituteEdges)
			require.Zero(t, memory.Rebuilt, "no list was built by a search")
			for docID, want := range expected {
				l := &st.postings.lists[st.side.postingOf[docID]]
				got := l.edgesOf(l.member(docID))
				require.Truef(t, slices.Equal(want, got), "the list of doc %d is %v, %v expected", docID, got, want)
			}

			answers := f.answers(t, k)
			require.Greater(t, f.recall(t, answers, k), deleteRecallFloor)

			// A second pass finds nothing to do.
			sub := d.sub
			rewritten := d.stats.Rewritten
			require.Zero(t, d.pendingPostings())
			require.NoError(t, d.eliminateSubstituting(ctx, 30, threshold))
			require.Equal(t, sub, d.sub)
			require.Equal(t, rewritten, d.stats.Rewritten)
		})
	}
}

// What must not be given as a substitute is told, and so is a list that does
// not hold together.
func TestDeleteSubstituteCatches(t *testing.T) {
	t.Parallel()
	f := newDeleteFixture(t, 200, 200, 16, false, 120, 30)
	d, st := f.d, f.s.st
	d.remember()
	gone := f.removeShare(t, 0.3, 13)

	// A live vector with edges, and a live vector in another posting that
	// is not one of them.
	var (
		docID uint64
		p     uint32
		kept  []uint32
		other uint32
		same  uint32
	)
	for q := range st.postings.lists {
		l := &st.postings.lists[q]
		for i, member := range l.docIDs {
			if d.deleted[member] || len(l.edgesOf(i)) < 2 || len(l.docIDs) < 2 {
				continue
			}
			docID, p = member, uint32(q)
			for _, endpoint := range l.edgesOf(i) {
				if !d.deleted[endpoint] {
					kept = append(kept, endpoint)
				}
			}
			for _, neighbor := range l.docIDs {
				if neighbor != member && !d.deleted[neighbor] {
					same = uint32(neighbor)
				}
			}
			break
		}
		if len(kept) > 0 && same != 0 {
			break
		}
		kept, same = nil, 0
	}
	require.NotEmpty(t, kept)
	for candidate, isLive := range f.s.live {
		if isLive && st.side.postingOf[candidate] != p && !slices.Contains(kept, uint32(candidate)) {
			other = uint32(candidate)
			break
		}
	}

	tests := []struct {
		name  string
		subs  []uint32
		wants string
	}{
		{name: "nothing wrong", subs: []uint32{other}},
		{name: "a deleted vector", subs: []uint32{uint32(gone[0])}, wants: "which is deleted"},
		{name: "the vector itself", subs: []uint32{uint32(docID)}, wants: "itself"},
		{name: "a vector of its posting", subs: []uint32{same}, wants: "in its posting"},
		{name: "a vector it had", subs: []uint32{kept[0]}, wants: "which it had"},
		{name: "the same vector twice", subs: []uint32{other, other}, wants: "which it had"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := d.checkSubstitutes(docID, p, kept, tt.subs)
			if tt.wants == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wants)
		})
	}

	t.Run("a list with an endpoint twice", func(t *testing.T) {
		require.NoError(t, d.eliminateSubstituting(context.Background(), 30, 120))
		require.NoError(t, d.checkEdgeLists())
		for q := range st.postings.lists {
			l := &st.postings.lists[q]
			for i := range l.docIDs {
				if edges := l.edgesOf(i); len(edges) >= 2 {
					edges[1] = edges[0]
					require.ErrorContains(t, d.checkEdgeLists(), "twice among its edges")
					return
				}
			}
		}
		t.Fatal("no list with two edges")
	})
}
