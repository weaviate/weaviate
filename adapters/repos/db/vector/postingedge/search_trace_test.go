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

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/visited"
)

// edgeTraceSummary is the edgeTrace of the searches at one operating point,
// over the query set: means per query, and the shares the v1.6
// pre-registration reads (bench-report.md, "Redundancia por query").
type edgeTraceSummary struct {
	RedundantMean       float64 `json:"redundantMean"`
	ReachedDirectlyMean float64 `json:"reachedDirectlyMean"`
	RankSetMean         float64 `json:"rankSetMean"`
	RankSetLoadedMean   float64 `json:"rankSetLoadedMean"`
	RankedLoadedMean    float64 `json:"rankedLoadedMean"`
	// RedundantShare is (1) of the pre-registration: the scored endpoints
	// that came through a redundant edge, over all the scored endpoints.
	// NetShare leaves out those a direct edge reached later.
	RedundantShare float64 `json:"redundantShare"`
	NetShare       float64 `json:"netShare"`
	// RankSetShare is the share of the redundant endpoints that set the
	// rank of their posting, RankSetLoadedShare the share of those postings
	// that were loaded.
	RankSetShare       float64 `json:"rankSetShare"`
	RankSetLoadedShare float64 `json:"rankSetLoadedShare"`
	// LoadsShare is (2) of the pre-registration: the share of the postings
	// loaded by rank whose rank came from a redundant endpoint.
	LoadsShare   float64 `json:"loadsShare"`
	Mispredicted int     `json:"mispredicted"`
}

// edgeTraceSummaryOf sums the traces of the searches of an operating point.
// Searches without a trace are left out.
func edgeTraceSummaryOf(stats []SearchStats) *edgeTraceSummary {
	var sum edgeTraceCounts
	scored, traced := 0, 0
	for _, s := range stats {
		if s.trace == nil {
			continue
		}
		traced++
		c := s.trace.edgeTraceCounts
		scored += s.EndpointsScored
		sum.Redundant += c.Redundant
		sum.ReachedDirectly += c.ReachedDirectly
		sum.RankSet += c.RankSet
		sum.RankSetLoaded += c.RankSetLoaded
		sum.RankedLoaded += c.RankedLoaded
		sum.Mispredicted += c.Mispredicted
	}
	share := func(part, whole int) float64 {
		if whole == 0 {
			return 0
		}
		return float64(part) / float64(whole)
	}
	return &edgeTraceSummary{
		RedundantMean:       share(sum.Redundant, traced),
		ReachedDirectlyMean: share(sum.ReachedDirectly, traced),
		RankSetMean:         share(sum.RankSet, traced),
		RankSetLoadedMean:   share(sum.RankSetLoaded, traced),
		RankedLoadedMean:    share(sum.RankedLoaded, traced),
		RedundantShare:      share(sum.Redundant, scored),
		NetShare:            share(sum.Redundant-sum.ReachedDirectly, scored),
		RankSetShare:        share(sum.RankSet, sum.Redundant),
		RankSetLoadedShare:  share(sum.RankSetLoaded, sum.RankSet),
		LoadsShare:          share(sum.RankSetLoaded, sum.RankedLoaded),
		Mispredicted:        sum.Mispredicted,
	}
}

func (s *edgeTraceSummary) String() string {
	return fmt.Sprintf("(1) %.1f redundant scored endpoints per query, %.1f%% of the scored (%.1f%% without the %.1f a direct edge reached later); "+
		"(2) %.2f set the rank of their posting (%.1f%% of the redundant), %.2f of them loaded (%.1f%%), "+
		"%.1f%% of the %.1f postings loaded by rank; mispredicted loads %d",
		s.RedundantMean, 100*s.RedundantShare, 100*s.NetShare, s.ReachedDirectlyMean,
		s.RankSetMean, 100*s.RankSetShare, s.RankSetLoadedMean, 100*s.RankSetLoadedShare,
		100*s.LoadsShare, s.RankedLoadedMean, s.Mispredicted)
}

func TestRedundantEdges(t *testing.T) {
	// slot -> posting: 0, 1 in posting 0; 2, 3, 4 in posting 1; 5 in posting 2
	postingOf := []uint32{0, 0, 1, 1, 1, 2}
	tests := []struct {
		name string
		row  []uint32
		want []bool
	}{
		{name: "no edges", row: nil, want: []bool{}},
		{name: "one edge", row: []uint32{2}, want: []bool{false}},
		{name: "every edge into another posting", row: []uint32{2, 5, 0}, want: []bool{false, false, false}},
		{name: "a farther edge into a posting of a nearer one", row: []uint32{2, 5, 3}, want: []bool{false, false, true}},
		{name: "every edge after the first into one posting", row: []uint32{4, 2, 3}, want: []bool{false, true, true}},
		{name: "two postings, each reached twice", row: []uint32{0, 2, 1, 3}, want: []bool{false, false, true, true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			marks := make([]bool, 7) // reused, longer than any row
			assert.Equal(t, tt.want, redundantEdges(tt.row, postingOf, marks))
		})
	}
}

// traceStore is a posting store of three postings and seven slots: posting 0
// holds slots 0 and 1, posting 1 slots 2 to 4, posting 2 slots 5 and 6.
//
//	slot 0: 2, 3, 5, 6  (3 and 6 are redundant: 2 and 5 lead to their postings)
//	slot 1: 3, 4        (4 is redundant)
//	slot 2: 6
//	slot 3: 5, 6        (6 is redundant)
func traceStore() (*postingStore, []uint32) {
	edgeStart := []uint32{0, 4, 6, 7, 9, 9, 9, 9}
	edges := []uint32{2, 3, 5, 6, 3, 4, 6, 5, 6}
	return &postingStore{
		codeWords: 1,
		start:     []uint32{0, 2, 5, 7},
		docIDs:    make([]uint64, 7),
		codes:     make([]uint64, 7),
		edgeStart: edgeStart,
		edges:     edges,
	}, []uint32{0, 0, 1, 1, 1, 2, 2}
}

// traceLoad loads posting p the way postingSearch.load does once the scan is
// done, with the trace on: the members have the given estimates, the
// endpoints those of endpointDist.
func traceLoad(tr *edgeTrace, sc *searchScratch, store *postingStore, postingOf []uint32, p uint32,
	memberDist []float32, cut float32, endpointDist map[uint32]float32,
) {
	post := store.load(p)
	sc.memberDist = append(sc.memberDist[:0], memberDist...)
	tr.picked(sc, p)
	tr.predict(sc, postingOf, &post, cut)
	sc.collectEndpoints(postingOf, &post, cut)
	dists := make([]float32, len(sc.unscored))
	for i, endpoint := range sc.unscored {
		dists[i] = endpointDist[endpoint]
	}
	sc.rankPostings(postingOf, dists)
	tr.scored(sc, postingOf, dists)
}

// Posting 0 is the entry posting. Once it is loaded, posting 1 is picked and
// loaded, and the search ends with posting 2 ranked but not loaded.
func TestEdgeTraceCounts(t *testing.T) {
	far, near := []float32{1, 1, 1}, []float32{0.1, 1, 1}
	tests := []struct {
		name string
		// the estimates of the members of postings 0 and 1, the cut of
		// both loads, and the estimates of the endpoints
		entry, second []float32
		cut           float32
		endpointDist  map[uint32]float32
		want          edgeTraceCounts
	}{
		{
			// 3 is reached by the direct edge of slot 1, in the same load.
			// 4 sets the rank of posting 1, and 6 that of posting 2.
			name:  "redundant endpoints set both ranks",
			entry: []float32{0.1, 0.2}, second: far, cut: 0.5,
			endpointDist: map[uint32]float32{2: 0.5, 3: 0.4, 4: 0.3, 5: 0.9, 6: 0.7},
			want:         edgeTraceCounts{Redundant: 3, ReachedDirectly: 1, RankSet: 2, RankSetLoaded: 1, RankedLoaded: 1},
		},
		{
			// 3 sets the rank of posting 1, but a direct edge reached it
			// before posting 1 was picked. 5 sets that of posting 2.
			name:  "a rank set by an endpoint reached directly before the pick",
			entry: []float32{0.1, 0.2}, second: far, cut: 0.5,
			endpointDist: map[uint32]float32{2: 0.5, 3: 0.1, 4: 0.3, 5: 0.6, 6: 0.7},
			want:         edgeTraceCounts{Redundant: 3, ReachedDirectly: 1, RankedLoaded: 1},
		},
		{
			name:  "a source beyond the cut follows no edge",
			entry: []float32{0.1, 0.9}, second: far, cut: 0.5,
			endpointDist: map[uint32]float32{2: 0.5, 3: 0.4, 5: 0.9, 6: 0.7},
			want:         edgeTraceCounts{Redundant: 2, RankSet: 2, RankSetLoaded: 1, RankedLoaded: 1},
		},
		{
			// The fold keeps the first of equal estimates: the direct ones.
			name:  "a redundant endpoint that only ties does not set the rank",
			entry: []float32{0.1, 0.9}, second: far, cut: 0.5,
			endpointDist: map[uint32]float32{2: 0.4, 3: 0.4, 5: 0.7, 6: 0.7},
			want:         edgeTraceCounts{Redundant: 2, RankedLoaded: 1},
		},
		{
			// Slot 2, in posting 1, reaches 6 directly after posting 1 was
			// picked: posting 1 counts, posting 2 no longer does.
			name:  "an endpoint reached directly after its posting was ranked",
			entry: []float32{0.1, 0.9}, second: near, cut: 0.5,
			endpointDist: map[uint32]float32{2: 0.5, 3: 0.4, 5: 0.9, 6: 0.7},
			want:         edgeTraceCounts{Redundant: 2, ReachedDirectly: 1, RankSet: 1, RankSetLoaded: 1, RankedLoaded: 1},
		},
		{
			// Slot 3 reaches 6 again, but through a redundant edge of its own.
			name:  "an endpoint reached again through another redundant edge",
			entry: []float32{0.1, 0.9}, second: []float32{1, 0.1, 1}, cut: 0.5,
			endpointDist: map[uint32]float32{2: 0.5, 3: 0.4, 5: 0.9, 6: 0.7},
			want:         edgeTraceCounts{Redundant: 2, RankSet: 2, RankSetLoaded: 1, RankedLoaded: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, postingOf := traceStore()
			sc := newSearchScratch(store.postings())
			sc.endpoints = visited.NewSparseSet(store.vectors(), 8)
			tr := newEdgeTrace()

			sc.markLoaded(0)
			traceLoad(tr, sc, store, postingOf, 0, tt.entry, tt.cut, tt.endpointDist)
			require.Equal(t, []uint32{1, 2}, sc.frontier, "both postings ranked")
			sc.markLoaded(1)
			traceLoad(tr, sc, store, postingOf, 1, tt.second, tt.cut, tt.endpointDist)
			tr.finish(sc)

			assert.Equal(t, tt.want, tr.edgeTraceCounts)
		})
	}

	t.Run("a load other than predicted is counted", func(t *testing.T) {
		store, postingOf := traceStore()
		sc := newSearchScratch(store.postings())
		sc.endpoints = visited.NewSparseSet(store.vectors(), 8)
		tr := newEdgeTrace()
		tr.predicted = []uint32{4}
		sc.unscored = []uint32{2}
		tr.scored(sc, postingOf, []float32{0.5})
		assert.Equal(t, 1, tr.Mispredicted)
		assert.Empty(t, tr.setter)
	})
}

// The trace changes nothing a search does, predicts every load right, and
// its counts hold together, over margins, batches and efs. With one edge per
// target posting there is nothing for it to count.
func TestEdgeTraceOfABuild(t *testing.T) {
	const k = 10
	all := clusteredVectors(3100, 24, 30, 914)
	vectors, queries := all[:3000], all[3000:]
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, distancer.NewL2SquaredProvider(), uc)
	st := index.state.Load()
	ctx := context.Background()

	run := func(t *testing.T) edgeTraceCounts {
		var total edgeTraceCounts
		for _, margin := range []float32{0, 2} {
			for _, batch := range []int{1, 4} {
				for _, ef := range []int{10, 40, 160} {
					uc.EF, uc.LoadBatch, uc.SourceMargin = ef, batch, margin
					require.NoError(t, index.UpdateUserConfig(uc, func() {}))
					for qi, query := range queries {
						name := fmt.Sprintf("margin %.0f batch %d ef %d query %d", margin, batch, ef, qi)
						ids, dists, err := index.SearchByVector(ctx, query, k, nil)
						require.NoError(t, err)
						traced := SearchStats{trace: newEdgeTrace()}
						tracedIDs, tracedDists, err := index.search(ctx, query, k, nil, &traced)
						require.NoError(t, err)
						require.Equal(t, ids, tracedIDs, name)
						require.Equal(t, dists, tracedDists, name)

						c := traced.trace.edgeTraceCounts
						require.Zero(t, c.Mispredicted, name)
						assert.LessOrEqual(t, c.Redundant, traced.EndpointsScored, name)
						assert.LessOrEqual(t, c.ReachedDirectly, c.Redundant, name)
						assert.LessOrEqual(t, c.RankSet, c.Redundant, name)
						assert.LessOrEqual(t, c.RankSetLoaded, c.RankSet, name)
						assert.LessOrEqual(t, c.RankSetLoaded, c.RankedLoaded, name)
						assert.Equal(t, traced.PostingsLoaded-uc.EntryPostings, c.RankedLoaded, name)

						total.Redundant += c.Redundant
						total.ReachedDirectly += c.ReachedDirectly
						total.RankSet += c.RankSet
						total.RankSetLoaded += c.RankSetLoaded
						total.RankedLoaded += c.RankedLoaded
					}
				}
			}
		}
		return total
	}

	t.Run("edges as built", func(t *testing.T) {
		total := run(t)
		assert.Positive(t, total.Redundant)
		assert.Positive(t, total.ReachedDirectly)
		assert.Positive(t, total.RankSet)
		assert.Positive(t, total.RankSetLoaded)
	})

	t.Run("one edge per target posting", func(t *testing.T) {
		before := edgeRedundancyOf(st.postings.edgeStart, st.postings.edges, st.side.postingOf)
		require.Positive(t, before.Shared)
		keptStart, kept := onePerTarget(st.postings.edgeStart, st.postings.edges, st.side.postingOf)
		after := edgeRedundancyOf(keptStart, kept, st.side.postingOf)
		require.Zero(t, after.Shared)
		require.Equal(t, before.Targets, after.Targets)
		require.Equal(t, before.Edges-before.Shared, after.Edges)

		require.NoError(t, index.replaceEdges(keptStart, kept))
		total := run(t)
		assert.Equal(t, edgeTraceCounts{RankedLoaded: total.RankedLoaded}, total)
		assert.Positive(t, total.RankedLoaded)
	})
}

func TestEdgeTraceSummaryOf(t *testing.T) {
	traced := func(scored int, c edgeTraceCounts) SearchStats {
		tr := newEdgeTrace()
		tr.edgeTraceCounts = c
		return SearchStats{EndpointsScored: scored, trace: tr}
	}
	tests := []struct {
		name  string
		stats []SearchStats
		want  edgeTraceSummary
	}{
		{name: "no searches", want: edgeTraceSummary{}},
		{
			name:  "searches without a trace are left out",
			stats: []SearchStats{{EndpointsScored: 100}},
			want:  edgeTraceSummary{},
		},
		{
			name: "two searches",
			stats: []SearchStats{
				traced(100, edgeTraceCounts{Redundant: 30, ReachedDirectly: 10, RankSet: 6, RankSetLoaded: 2, RankedLoaded: 10}),
				traced(300, edgeTraceCounts{Redundant: 50, ReachedDirectly: 10, RankSet: 2, RankSetLoaded: 2, RankedLoaded: 30}),
				{EndpointsScored: 1000},
			},
			want: edgeTraceSummary{
				RedundantMean: 40, ReachedDirectlyMean: 10, RankSetMean: 4, RankSetLoadedMean: 2, RankedLoadedMean: 20,
				RedundantShare: 0.2, NetShare: 0.15, RankSetShare: 0.1, RankSetLoadedShare: 0.5, LoadsShare: 0.1,
			},
		},
		{
			name:  "mispredicted loads add up",
			stats: []SearchStats{traced(0, edgeTraceCounts{Mispredicted: 1}), traced(0, edgeTraceCounts{Mispredicted: 2})},
			want:  edgeTraceSummary{Mispredicted: 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := edgeTraceSummaryOf(tt.stats)
			assert.InDeltaMapValues(t, summaryMap(tt.want), summaryMap(*got), 1e-12)
			assert.Equal(t, tt.want.Mispredicted, got.Mispredicted)
			assert.NotEmpty(t, got.String())
		})
	}
}

func summaryMap(s edgeTraceSummary) map[string]float64 {
	return map[string]float64{
		"redundantMean": s.RedundantMean, "reachedDirectlyMean": s.ReachedDirectlyMean,
		"rankSetMean": s.RankSetMean, "rankSetLoadedMean": s.RankSetLoadedMean, "rankedLoadedMean": s.RankedLoadedMean,
		"redundantShare": s.RedundantShare, "netShare": s.NetShare, "rankSetShare": s.RankSetShare,
		"rankSetLoadedShare": s.RankSetLoadedShare, "loadsShare": s.LoadsShare,
	}
}
