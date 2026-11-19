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
	ctx := context.Background()
	tests := []struct {
		name              string
		share             float64
		clustered, cosine bool
	}{
		{name: "30 % deleted", share: 0.3},
		{name: "30 % deleted, cosine", share: 0.3, cosine: true},
		{name: "3 % deleted", share: 0.03},
		{name: "half deleted", share: 0.5},
		{name: "whole postings deleted", share: 0.3, clustered: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const n, dims, k, threshold = 4000, 48, 10, 120
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
				require.Equalf(t, want, l.edgesOf(l.member(docID)), "the list of doc %d", docID)
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

// The passes of v6.2 over rounds of deletes, each cell on an index of its
// own with the same deletes: the pass of v6.1, which takes its share anew at
// every pass, the one that remembers what a list has lost (A), the one that
// substitutes (B), and the one that substitutes and searches for the lists
// that lose too much with no substitute (C).
func TestDeleteRepairsInRounds(t *testing.T) {
	ctx := context.Background()
	const n, dims, k, threshold, rounds = 4000, 48, 10, 120, 6
	type result struct {
		repaired int
		edges    float64
	}
	tests := []struct {
		name                  string
		remembers, searches   bool
		substitutes, rehearse bool
	}{
		{name: "the pass of v6.1", searches: true},
		{name: "A", remembers: true, searches: true},
		{name: "B", remembers: true, substitutes: true},
		{name: "C", remembers: true, searches: true, substitutes: true, rehearse: true},
	}
	results := map[string]result{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDeleteFixture(t, n, n, dims, false, threshold, 30)
			s, d, st := f.s, f.d, f.s.st
			if tt.remembers {
				d.remember()
			}
			var out result
			for round := range rounds {
				f.removeShare(t, 0.05, uint64(40+round))
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
			results[tt.name] = out
			t.Logf("%d lists built again in %d rounds, %.2f edges a vector at the end; %d edges given a substitute and %d left with none",
				out.repaired, rounds, out.edges, d.sub.Substituted, d.sub.Without)

			answers := f.answers(t, k)
			require.Greater(t, f.recall(t, answers, k), deleteRecallFloor)
		})
	}
	if len(results) != len(tests) {
		return
	}
	old, a, b, c := results["the pass of v6.1"], results["A"], results["B"], results["C"]
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

// What must not be given as a substitute is told, and so is a list that does
// not hold together.
func TestDeleteSubstituteCatches(t *testing.T) {
	f := newDeleteFixture(t, 2000, 2000, 32, false, 120, 30)
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
