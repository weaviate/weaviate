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
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// deleteFixture is a small streaming index with deletes: the first base
// vectors built in batch, the rest left to insert.
type deleteFixture struct {
	index    *Index
	s        *streamIndex
	d        *deleteIndex
	all      [][]float32
	queries  [][]float32
	provider distancer.Provider
	uc       UserConfig
	base     int
}

func newDeleteFixture(t *testing.T, total, base, dims int, cosine bool, threshold, minLive int) *deleteFixture {
	t.Helper()
	ctx := context.Background()
	f := &deleteFixture{
		all:      clusteredVectors(total, dims, 12, 6007),
		queries:  clusteredVectors(60, dims, 12, 6008),
		provider: distancer.NewL2SquaredProvider(),
		uc:       smallConfig(),
		base:     base,
	}
	f.uc.Distance, f.uc.EF = "l2-squared", 48
	if cosine {
		f.provider = distancer.NewCosineDistanceProvider()
		f.uc.Distance = "cosine"
		for i, vector := range f.all {
			f.all[i] = distancer.Normalize(vector)
		}
		for i, vector := range f.queries {
			f.queries[i] = distancer.Normalize(vector)
		}
	}
	index, err := New(testConfig(t, f.all, 0, f.provider), f.uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(ctx, sequentialIDs(base, 0), f.all[:base]))
	require.NoError(t, index.Build(ctx))
	f.index = index
	f.s, err = newStreamIndex(index, total, streamConfig{threshold: threshold, inverse: true, seed: 7},
		func(id uint64) []float32 { return f.all[id] })
	require.NoError(t, err)
	f.d = newDeleteIndex(f.s, 0.30, minLive)
	return f
}

// removeShare deletes a share of the live vectors, drawn at random.
func (f *deleteFixture) removeShare(t *testing.T, share float64, seed uint64) []uint64 {
	t.Helper()
	var live []uint64
	for docID, isLive := range f.s.live {
		if isLive {
			live = append(live, uint64(docID))
		}
	}
	rand.New(rand.NewPCG(seed, 1)).Shuffle(len(live), func(i, j int) { live[i], live[j] = live[j], live[i] })
	gone := live[:int(share*float64(len(live)))]
	for _, docID := range gone {
		require.NoError(t, f.d.remove(docID))
	}
	return gone
}

// deleteAnswer is what a search returned and the work it took.
type deleteAnswer struct {
	ids       []uint64
	dists     []float32
	loads     int
	scanned   int
	endpoints int
}

// answers searches every query and checks that no deleted vector comes back.
func (f *deleteFixture) answers(t *testing.T, k int) []deleteAnswer {
	t.Helper()
	out := make([]deleteAnswer, len(f.queries))
	for i, query := range f.queries {
		var work SearchStats
		ids, dists, err := f.index.search(context.Background(), query, k, nil, &work)
		require.NoError(t, err)
		for _, id := range ids {
			require.Falsef(t, f.d.deleted[id], "query %d returned doc %d, which is deleted", i, id)
			require.Truef(t, f.s.live[id], "query %d returned doc %d, which is not live", i, id)
		}
		out[i] = deleteAnswer{ids: ids, dists: dists, loads: work.PostingsLoaded, scanned: work.VectorsScanned, endpoints: work.EndpointsScored}
	}
	return out
}

// recall is the share of the k nearest live vectors of every query, by brute
// force, that the answers hold.
func (f *deleteFixture) recall(t *testing.T, answers []deleteAnswer, k int) float64 {
	t.Helper()
	found, wanted := 0, 0
	for i, query := range f.queries {
		var nearest topK
		nearest.reset(k)
		for docID, isLive := range f.s.live {
			if !isLive {
				continue
			}
			dist, err := f.provider.SingleDist(query, f.all[docID])
			require.NoError(t, err)
			nearest.push(uint64(docID), dist)
		}
		for _, want := range nearest.sorted() {
			wanted++
			if slices.Contains(answers[i].ids, want.id) {
				found++
			}
		}
	}
	return float64(found) / float64(wanted)
}

// A search over an index with marked vectors returns what it returns over
// the same index with those vectors taken out of their postings: the same
// ids and distances, with the same postings loaded, members scored and
// endpoints scored. With the deleted endpoints scored, the edges that lead
// to them are still there and still lead somewhere; without, it is as if
// those edges were gone too.
func TestDeleteSearchIsPhysicalRemoval(t *testing.T) {
	tests := []struct {
		name   string
		cosine bool
		share  float64
	}{
		{name: "a third deleted", share: 0.3},
		{name: "a third deleted, cosine", share: 0.3, cosine: true},
		{name: "most of it deleted", share: 0.7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const n, dims, k = 3000, 48, 10
			f := newDeleteFixture(t, n, n, dims, tt.cosine, 1<<20, 0)
			before := f.answers(t, k)
			f.removeShare(t, tt.share, 11)
			require.NoError(t, f.d.checkCoverage())

			f.d.scoreDeletedEndpoints(true)
			hints := f.answers(t, k)
			f.d.scoreDeletedEndpoints(false)
			plain := f.answers(t, k)
			require.NotEqual(t, before, hints, "the deletes change what is found")
			scoredWith, scoredWithout := 0, 0
			for i := range hints {
				scoredWith += hints[i].endpoints
				scoredWithout += plain[i].endpoints
			}
			require.Greater(t, scoredWith, scoredWithout, "the deleted endpoints are scored only as hints")
			require.Greater(t, f.recall(t, plain, k), 0.8)
			require.Greater(t, f.recall(t, hints, k), 0.8)

			// The same index with the deleted members out of their postings,
			// and no mark: their codes and their place in the map stay.
			st := f.s.st
			for p := range st.postings.lists {
				f.d.rewrite(uint32(p))
			}
			st.deleted, st.skipDeletedEndpoints = nil, false
			require.Equal(t, hints, f.answersUnmarked(t, k), "deleted members taken out, the edges to them left")

			// And with the edges that lead to them out of every list.
			for p := range st.postings.lists {
				l := &st.postings.lists[p]
				for i := range l.docIDs {
					kept := slices.DeleteFunc(slices.Clone(l.edgesOf(i)), func(endpoint uint32) bool { return f.d.deleted[endpoint] })
					l.setEdges(i, kept)
				}
			}
			require.Equal(t, plain, f.answersUnmarked(t, k), "deleted members and the edges to them taken out")
		})
	}
}

// answersUnmarked searches every query over an index without marks.
func (f *deleteFixture) answersUnmarked(t *testing.T, k int) []deleteAnswer {
	t.Helper()
	out := make([]deleteAnswer, len(f.queries))
	for i, query := range f.queries {
		var work SearchStats
		ids, dists, err := f.index.search(context.Background(), query, k, nil, &work)
		require.NoError(t, err)
		out[i] = deleteAnswer{ids: ids, dists: dists, loads: work.PostingsLoaded, scanned: work.VectorsScanned, endpoints: work.EndpointsScored}
	}
	return out
}

// The cleanup pass leaves no posting with more than its share of deleted
// members and none under the minimum that nothing was merged into, loses no
// live vector, and keeps the map with the postings.
func TestDeleteCleanup(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		share     float64
		clustered bool
		cosine    bool
		threshold int
		minLive   int
		// What the case is made to meet.
		merges, empties, splits bool
	}{
		{name: "a tenth deleted", share: 0.1, threshold: 120, minLive: 20, merges: true},
		{name: "a third deleted", share: 0.3, threshold: 120, minLive: 20, merges: true},
		{name: "a third deleted, cosine", share: 0.3, cosine: true, threshold: 120, minLive: 20, merges: true},
		{name: "half deleted", share: 0.5, threshold: 120, minLive: 20, merges: true},
		{name: "half deleted, merges that end in splits", share: 0.5, threshold: 44, minLive: 30, merges: true, splits: true},
		{name: "no minimum: nothing is merged", share: 0.5, threshold: 120, minLive: 0},
		{name: "whole postings deleted", share: 0.3, clustered: true, threshold: 120, minLive: 20, empties: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const n, dims, k = 4000, 48, 10
			f := newDeleteFixture(t, n, n, dims, tt.cosine, tt.threshold, tt.minLive)
			s, d := f.s, f.d
			if tt.clustered {
				// The members of the first postings, up to the share.
				gone := 0
				for p := range s.st.postings.lists {
					if float64(gone) >= tt.share*n {
						break
					}
					for _, docID := range slices.Clone(s.st.postings.lists[p].docIDs) {
						require.NoError(t, d.remove(docID))
						gone++
					}
				}
			} else {
				f.removeShare(t, tt.share, 13)
			}
			require.NoError(t, d.checkCoverage())
			live := s.liveCount
			marked := d.counts(f.uc.TargetPostingSize)
			require.Equal(t, n, marked.Members, "a delete takes no member out of its posting")
			require.Equal(t, live, marked.LiveMembers)
			d.scoreDeletedEndpoints(false)
			f.answers(t, k)

			postings := len(s.st.postings.lists)
			require.NoError(t, d.cleanup(ctx))
			require.NoError(t, d.checkCoverage())
			require.Equal(t, live, s.liveCount, "the cleanup loses no live vector")
			after := d.counts(f.uc.TargetPostingSize)
			require.Equal(t, live, after.LiveMembers)
			require.Equal(t, marked.Members-after.Members, d.stats.DroppedDead, "the deleted members dropped")
			for p := range s.st.postings.lists {
				if s.retired[p] {
					continue
				}
				members, dead := len(s.st.postings.lists[p].docIDs), d.dead(uint32(p))
				require.LessOrEqualf(t, float64(dead), d.rewriteOver*float64(members), "posting %d keeps %d deleted of %d", p, dead, members)
				if p < postings && !d.received[uint32(p)] {
					require.GreaterOrEqualf(t, members-dead, tt.minLive, "posting %d was left under the minimum", p)
				}
			}
			assert.Positive(t, d.stats.Rewritten+d.stats.Merges+d.stats.EmptyRetired, "the pass did something")
			assert.Equal(t, tt.merges, d.stats.Merges > 0, "%d merges", d.stats.Merges)
			assert.Equal(t, tt.empties, d.stats.EmptyRetired > 0, "%d postings retired empty", d.stats.EmptyRetired)
			assert.Equal(t, tt.splits, d.stats.MergeSplits > 0, "%d splits after a merge", d.stats.MergeSplits)
			if tt.minLive == 0 {
				assert.Zero(t, d.stats.MovedLive)
			}
			answers := f.answers(t, k)
			require.Greater(t, f.recall(t, answers, k), 0.8)

			// A second pass finds nothing over the share to write again.
			rewritten := d.stats.Rewritten
			require.NoError(t, d.cleanup(ctx))
			require.NoError(t, d.checkCoverage())
			require.Equal(t, rewritten, d.stats.Rewritten)
		})
	}
}

// The refresh pass leaves no live vector with half or more of its edges
// leading to deleted vectors, and no refreshed list leads to one.
func TestDeleteRefresh(t *testing.T) {
	ctx := context.Background()
	for _, share := range []float64{0.3, 0.6} {
		t.Run("", func(t *testing.T) {
			const n, dims, k = 4000, 48, 10
			f := newDeleteFixture(t, n, n, dims, false, 120, 20)
			d := f.d
			f.removeShare(t, share, 17)
			d.scoreDeletedEndpoints(false)
			require.NoError(t, d.cleanup(ctx))
			before := d.counts(f.uc.TargetPostingSize)
			stale := d.halfDead()
			require.NotEmpty(t, stale)
			require.Len(t, stale, before.HalfDead)
			require.True(t, slices.IsSorted(stale))

			stats, err := d.refresh(ctx)
			require.NoError(t, err)
			require.NoError(t, d.checkCoverage())
			require.Equal(t, len(stale), stats.Refreshed)
			require.Positive(t, stats.Work.SearchLoads)
			after := d.counts(f.uc.TargetPostingSize)
			require.Empty(t, d.halfDead())
			require.Zero(t, after.HalfDead)
			require.Less(t, after.DeadEdges, before.DeadEdges)
			require.Equal(t, before.LiveMembers, after.LiveMembers)
			st := f.s.st
			for _, docID := range stale {
				l := &st.postings.lists[st.side.postingOf[docID]]
				for _, endpoint := range l.edgesOf(l.member(docID)) {
					require.False(t, d.deleted[endpoint], "doc %d", docID)
				}
			}
			answers := f.answers(t, k)
			require.Greater(t, f.recall(t, answers, k), 0.8)
		})
	}
}

// Deletes and inserts in rounds, with the passes after each: the index stays
// whole, never returns a deleted vector, and finds what was inserted.
func TestDeleteChurn(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name    string
		cleanup bool
		refresh bool
	}{
		{name: "marks only"},
		{name: "with the cleanup pass", cleanup: true},
		{name: "with the cleanup and the refresh passes", cleanup: true, refresh: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const total, base, dims, k, rounds, each = 3600, 2400, 48, 10, 6, 200
			f := newDeleteFixture(t, total, base, dims, false, 90, 20)
			s, d := f.s, f.d
			d.scoreDeletedEndpoints(false)
			if tt.cleanup {
				d.dropAtSplit()
			}
			rng := rand.New(rand.NewPCG(19, 1))
			next := uint64(base)
			for round := range rounds {
				var live []uint64
				for docID, isLive := range s.live {
					if isLive && docID < base {
						live = append(live, uint64(docID))
					}
				}
				rng.Shuffle(len(live), func(i, j int) { live[i], live[j] = live[j], live[i] })
				for _, docID := range live[:each] {
					require.NoError(t, d.remove(docID))
				}
				for range each {
					require.NoError(t, s.insert(ctx, next, f.all[next]), "doc %d", next)
					next++
				}
				if tt.cleanup {
					require.NoError(t, d.cleanup(ctx))
				}
				if tt.refresh {
					_, err := d.refresh(ctx)
					require.NoError(t, err)
				}
				require.NoError(t, d.checkCoverage(), "round %d", round)
				require.Equal(t, base, s.liveCount)
				f.answers(t, k)
			}
			require.Equal(t, rounds*each, d.stats.Deleted)
			require.Positive(t, s.stats.Splits)
			if tt.cleanup {
				require.Positive(t, d.stats.DroppedDead)
			} else {
				require.Zero(t, d.stats.DroppedDead)
				require.Equal(t, base+rounds*each, d.counts(f.uc.TargetPostingSize).Members, "every deleted vector is still in a posting")
			}
			if tt.refresh {
				require.Empty(t, d.halfDead())
			}
			// What was inserted is found, and so is what was there.
			found := 0
			for docID := uint64(base); docID < next; docID++ {
				ids, _, err := f.index.search(ctx, f.all[docID], 1, nil, nil)
				require.NoError(t, err)
				if len(ids) == 1 && ids[0] == docID {
					found++
				}
			}
			require.GreaterOrEqual(t, found, rounds*each*95/100)
			require.Greater(t, f.recall(t, f.answers(t, k), k), 0.8)
		})
	}
}

// The coverage check tells an index that does not hold together.
func TestDeleteCoverageCatches(t *testing.T) {
	tests := []struct {
		name    string
		breakIt func(f *deleteFixture)
		wantErr string
	}{
		{name: "a live vector out of its posting", breakIt: func(f *deleteFixture) {
			l := &f.s.st.postings.lists[0]
			gone := l.docIDs[0]
			f.d.deleted[gone] = true
			f.d.rewrite(0)
			f.d.deleted[gone] = false
		}, wantErr: "is in 0 postings"},
		{name: "a vector in two postings", breakIt: func(f *deleteFixture) {
			lists := f.s.st.postings.lists
			words := f.s.st.postings.codeWords
			lists[1].append(lists[0].docIDs[0], lists[0].codes[:words], nil)
		}, wantErr: "which the map puts in 0"},
		{name: "a member that is not live and has no mark", breakIt: func(f *deleteFixture) {
			docID := f.s.st.postings.lists[0].docIDs[0]
			f.s.live[docID] = false
		}, wantErr: "neither live nor marked deleted"},
		{name: "a live vector with the mark", breakIt: func(f *deleteFixture) {
			f.d.deleted[f.s.st.postings.lists[0].docIDs[0]] = true
		}, wantErr: "is marked deleted"},
		{name: "a retired posting that holds vectors", breakIt: func(f *deleteFixture) {
			f.s.retired[0] = true
		}, wantErr: "retired posting 0 holds"},
		{name: "a live count that is off", breakIt: func(f *deleteFixture) {
			f.s.liveCount++
		}, wantErr: "live docs counted"},
		{name: "an edge to a vector that never was", breakIt: func(f *deleteFixture) {
			l := &f.s.st.postings.lists[0]
			l.setEdges(0, []uint32{uint32(len(f.s.live) - 1)})
		}, wantErr: "which was never in the index"},
		{name: "a posting whose arrays do not match", breakIt: func(f *deleteFixture) {
			l := &f.s.st.postings.lists[0]
			l.codes = l.codes[:len(l.codes)-1]
		}, wantErr: "does not hold together"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// One key more than vectors: a doc id that is never inserted.
			f := newDeleteFixture(t, 801, 800, 32, false, 120, 10)
			f.removeShare(t, 0.2, 23)
			require.NoError(t, f.d.checkCoverage())
			tt.breakIt(f)
			require.ErrorContains(t, f.d.checkCoverage(), tt.wantErr)
		})
	}
}

// A vector is deleted once, and only if it is in the index.
func TestDeleteErrors(t *testing.T) {
	f := newDeleteFixture(t, 801, 800, 32, false, 120, 10)
	tests := []struct {
		name  string
		docID uint64
	}{
		{name: "a doc past the keys", docID: 5000},
		{name: "a doc that was never inserted", docID: 800},
		{name: "a doc deleted already", docID: 7},
	}
	require.NoError(t, f.d.remove(7))
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := f.d.stats
			require.ErrorContains(t, f.d.remove(tt.docID), "is not in the index")
			require.Equal(t, before, f.d.stats)
		})
	}
	require.Equal(t, 799, f.s.liveCount)
	require.NoError(t, f.d.checkCoverage())
}

// With the deleted members as sources (cell 1b), a deleted vector is scored
// and its edges are followed, and it is still never returned. Where whole
// postings are deleted that is what lets a search out of them: without it, a
// search that enters through a posting with no live member finds nothing.
func TestDeleteSources(t *testing.T) {
	const n, dims, k = 4000, 48, 10
	tests := []struct {
		name      string
		clustered bool
		cosine    bool
		share     float64
	}{
		{name: "a third deleted at random", share: 0.3},
		{name: "half deleted at random, cosine", share: 0.5, cosine: true},
		{name: "whole postings deleted", share: 0.3, clustered: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newDeleteFixture(t, n, n, dims, tt.cosine, 1<<20, 0)
			s, d := f.s, f.d
			untouched := f.answers(t, k)
			d.deletedAreSources(true)
			require.Equal(t, untouched, f.answers(t, k), "without deletes the sources change nothing")
			d.deletedAreSources(false)

			if tt.clustered {
				gone := 0
				for p := range s.st.postings.lists {
					if float64(gone) >= tt.share*n {
						break
					}
					for _, docID := range slices.Clone(s.st.postings.lists[p].docIDs) {
						require.NoError(t, d.remove(docID))
						gone++
					}
				}
			} else {
				f.removeShare(t, tt.share, 29)
			}
			require.NoError(t, d.checkCoverage())
			d.scoreDeletedEndpoints(true)

			skipped := f.answers(t, k)
			d.deletedAreSources(true)
			sources := f.answers(t, k)
			require.NoError(t, d.checkCoverage())
			scannedSkipped, scannedSources, short, shortSources := 0, 0, 0, 0
			for i := range skipped {
				scannedSkipped += skipped[i].scanned
				scannedSources += sources[i].scanned
				if len(skipped[i].ids) < k {
					short++
				}
				if len(sources[i].ids) < k {
					shortSources++
				}
			}
			require.Greater(t, scannedSources, scannedSkipped, "a deleted source is scored")
			require.Zero(t, shortSources, "every query finds its k live vectors")
			require.Greater(t, f.recall(t, sources, k), 0.8)
			if tt.clustered {
				require.Positive(t, short, "a search that enters through a posting without live members finds nothing")
				require.Greater(t, f.recall(t, sources, k), f.recall(t, skipped, k))
			}
			// It changes how the index is searched and nothing of the index.
			d.deletedAreSources(false)
			require.Equal(t, skipped, f.answers(t, k))
		})
	}
}
