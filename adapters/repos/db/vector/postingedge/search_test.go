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
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// smallConfig shrinks the build parameters to corpora of a few thousand
// vectors, where the defaults would produce a handful of postings.
func smallConfig() UserConfig {
	uc := NewDefaultUserConfig()
	uc.TargetPostingSize = 40
	uc.BuildProbe = 16
	uc.RescoreLimit = 100
	return uc
}

// When every posting is an entry posting, every vector is scanned, and with
// a rescore limit above the corpus size every vector is compared exactly: the
// result must be the exact top k, with exact distances.
func TestSearchIsExactWhenEverythingIsLoadedAndRescored(t *testing.T) {
	tests := []struct {
		name     string
		provider distancer.Provider
		distance string
	}{
		{name: "l2", provider: distancer.NewL2SquaredProvider(), distance: "l2-squared"},
		{name: "cosine", provider: distancer.NewCosineDistanceProvider(), distance: "cosine"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const k = 10
			all := clusteredVectors(1100, 24, 20, 61)
			vectors, queries := all[:1000], all[1000:]

			uc := smallConfig()
			uc.Distance = tt.distance
			uc.EntryPostings = 1000
			uc.RescoreLimit = 2000
			index := buildTestIndex(t, vectors, tt.provider, uc)

			truth := vectors
			if tt.distance == "cosine" {
				truth = normalizedCopy(vectors)
			}
			for _, query := range queries {
				ids, dists, stats, err := index.SearchByVectorWithStats(context.Background(), query, k, nil)
				require.NoError(t, err)
				require.Equal(t, len(vectors), stats.VectorsScanned)
				require.Zero(t, stats.Rounds, "nothing is left to load after the entry postings")

				exactQuery := query
				if tt.distance == "cosine" {
					exactQuery = distancer.Normalize(query)
				}
				want := exactNearest(t, truth, exactQuery, k, -1, tt.provider)
				require.Len(t, ids, k)
				for i, w := range want {
					require.Equal(t, w.id, ids[i])
					require.InDelta(t, w.dist, dists[i], 1e-5)
				}
			}
		})
	}
}

// The index must find the true neighbors through its edges, starting from a
// single posting, and ef must buy recall. The corpus has no cluster structure
// at all (one blob around a shared offset, the anisotropic shape of real
// embeddings), so the neighbors of a query are spread over many postings and
// nothing but the edges can lead to them.
func TestSearchRecallGrowsWithEF(t *testing.T) {
	const k = 10
	provider := distancer.NewCosineDistanceProvider()
	all := clusteredVectors(8200, 32, 1, 71)
	vectors, queries := all[:8000], all[8000:]
	truth := normalizedCopy(vectors)
	normalizedQueries := normalizedCopy(queries)

	index := buildTestIndex(t, vectors, provider, smallConfig())
	memory, ok := index.MemoryStats()
	require.True(t, ok)
	require.Greater(t, memory.Postings, 100, "the corpus must be spread over many postings")

	recallWith := func(ef int) float64 {
		uc := smallConfig()
		uc.EF = ef
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))
		return recallAt(t, index, truth, normalizedQueries, k, provider)
	}

	atK := recallWith(k)
	at4K := recallWith(4 * k)
	at20K := recallWith(20 * k)
	t.Logf("recall@%d: ef=k %.3f, ef=4k %.3f, ef=20k %.3f", k, atK, at4K, at20K)

	// Measured 0.83 / 0.97 / 0.999; the bounds leave room for the graph
	// over the centroids, which is built concurrently and so differs
	// slightly from run to run.
	assert.Greater(t, atK, 0.72, "the natural stop at ef=k must already find most neighbors")
	assert.Greater(t, at4K, 0.93)
	assert.Greater(t, at20K, 0.985)
	assert.Greater(t, at4K, atK, "ef must buy recall")
}

// A search with a larger ef follows the same path and stops later, so it
// never loads fewer postings.
func TestSearchLoadsMoreWithALargerEF(t *testing.T) {
	const k = 10
	provider := distancer.NewL2SquaredProvider()
	all := clusteredVectors(3100, 24, 40, 81)
	vectors, queries := all[:3000], all[3000:]

	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)

	loadedWith := func(ef int) []SearchStats {
		next := uc
		next.EF = ef
		require.NoError(t, index.UpdateUserConfig(next, func() {}))
		out := make([]SearchStats, len(queries))
		for i, query := range queries {
			_, _, stats, err := index.SearchByVectorWithStats(context.Background(), query, k, nil)
			require.NoError(t, err)
			out[i] = stats
		}
		return out
	}

	small, large := loadedWith(k), loadedWith(8*k)
	grew := false
	for i := range queries {
		require.GreaterOrEqualf(t, large[i].PostingsLoaded, small[i].PostingsLoaded, "query %d", i)
		require.GreaterOrEqual(t, large[i].Rounds, small[i].Rounds)
		grew = grew || large[i].PostingsLoaded > small[i].PostingsLoaded
	}
	assert.True(t, grew, "a larger ef must load more postings for at least one query")
}

// The stats must add up to the work the configuration allows.
func TestSearchStatsAccountForTheWork(t *testing.T) {
	const k = 10
	provider := distancer.NewL2SquaredProvider()
	all := clusteredVectors(3050, 24, 40, 91)
	vectors, queries := all[:3000], all[3000:]

	tests := []struct {
		name    string
		entries int
		batch   int
	}{
		{name: "one entry, one posting per round", entries: 1, batch: 1},
		{name: "one entry, four postings per round", entries: 1, batch: 4},
		{name: "three entries, two postings per round", entries: 3, batch: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			uc.Distance = "l2-squared"
			uc.EntryPostings = tt.entries
			uc.LoadBatch = tt.batch
			// Far more candidates than the entry postings hold, so that
			// every search has to leave them.
			uc.EF = 20 * k
			uc.RescoreLimit = 20 * k
			index := buildTestIndex(t, vectors, provider, uc)

			rounds := 0
			for _, query := range queries {
				_, _, stats, err := index.SearchByVectorWithStats(context.Background(), query, k, nil)
				require.NoError(t, err)

				require.GreaterOrEqual(t, stats.PostingsLoaded, tt.entries)
				require.LessOrEqual(t, stats.PostingsLoaded, tt.entries+stats.Rounds*tt.batch)
				if tt.batch == 1 {
					require.Equal(t, tt.entries+stats.Rounds, stats.PostingsLoaded)
				}
				require.Positive(t, stats.VectorsScanned)
				require.Equal(t, min(uc.RescoreLimit, stats.VectorsScanned), stats.Rescored)

				// A posting is loaded by a round because one of its
				// members was scored as an endpoint: the scan meets at
				// least that member a second time. The entry postings
				// are loaded before any endpoint is scored.
				require.GreaterOrEqual(t, stats.ScannedAgain, stats.PostingsLoaded-tt.entries)
				require.Less(t, stats.ScannedAgain, stats.VectorsScanned)

				// Every followed edge is accounted for exactly once.
				require.GreaterOrEqual(t, stats.EndpointsRepeated(), 0)
				require.Equal(t, stats.EdgesFollowed,
					stats.EndpointsInLoaded+stats.EndpointsRepeated()+stats.EndpointsScored)
				rounds += stats.Rounds
			}
			require.Positive(t, rounds, "the searches must leave their entry postings")
		})
	}
}

// The source margin is a cut on the scanned vectors: a vector further than
// the margin past the stop threshold keeps its place among the candidates but
// its edges are not followed. This drives one posting load by hand and checks
// the cut against the rule, member by member.
func TestSourceMarginCutsTheSourcesBeyondTheThreshold(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	all := clusteredVectors(3010, 24, 1, 301)
	vectors, queries := all[:3000], all[3000:]
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)
	st := index.state.Load()

	tests := []struct {
		name   string
		ef     int
		margin float32
	}{
		{name: "no margin follows every edge", ef: 10, margin: 0},
		{name: "a tight margin", ef: 10, margin: 0.2},
		{name: "a wider margin", ef: 10, margin: 1},
		{name: "a margin that reaches every member", ef: 10, margin: 1e6},
		{name: "no cut while the search still lacks ef candidates", ef: 100000, margin: 0.2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cutSomething := false
			for qi, query := range queries {
				p := uint32(qi * 7 % st.postings.postings())
				post := st.postings.load(p)

				sc := st.borrowScratch()
				sc.pool.reset(uc.RescoreLimit)
				sc.stop.reset(tt.ef)
				var stats SearchStats
				search := postingSearch{
					st: st, sc: sc, estimator: st.quantizer.NewDistancer(query),
					stop: &sc.stop, margin: tt.margin, stats: &stats,
				}
				sc.markLoaded(p)
				require.NoError(t, search.load(p))
				scored := len(sc.unscored)
				st.returnScratch(sc)

				// The rule, from scratch: the threshold is the ef-th
				// best estimate among the members, and a member is cut
				// if its estimate is more than the margin beyond it.
				estimator := st.quantizer.NewDistancer(query)
				estimates := make([]float32, len(post.docIDs))
				var best topK
				best.reset(tt.ef)
				for i := range post.docIDs {
					dist, err := estimator.Distance(post.codes[i*st.postings.codeWords : (i+1)*st.postings.codeWords])
					require.NoError(t, err)
					estimates[i] = dist
					best.push(uint64(i), dist)
				}
				wantCut, wantEdgesCut, wantFollowed := 0, 0, 0
				for i, dist := range estimates {
					if tt.margin > 0 && dist > best.worst()+tt.margin {
						wantCut++
						wantEdgesCut += len(post.edgesOf(i))
					} else {
						wantFollowed += len(post.edgesOf(i))
					}
				}

				require.Equal(t, wantCut, stats.SourcesCut)
				require.Equal(t, wantEdgesCut, stats.EdgesCut)
				require.Equal(t, wantFollowed, stats.EdgesFollowed)
				require.Equal(t, len(post.edges), stats.EdgesCut+stats.EdgesFollowed, "every edge is cut or followed")
				require.Equal(t, scored, stats.EndpointsScored)
				cutSomething = cutSomething || wantCut > 0
			}

			switch tt.name {
			case "a tight margin", "a wider margin":
				assert.True(t, cutSomething, "the margin must cut some sources in this fixture")
			default:
				assert.False(t, cutSomething)
			}
		})
	}
}

// A margin trades work for recall. Wide enough it changes nothing; tighter,
// it must save endpoint scoring, and what it costs in recall must stay small
// as long as it is not absurdly tight.
func TestSourceMarginSavesWork(t *testing.T) {
	const k = 10
	provider := distancer.NewCosineDistanceProvider()
	all := clusteredVectors(8200, 32, 1, 71)
	vectors, queries := all[:8000], all[8000:]
	truth := normalizedCopy(vectors)
	normalizedQueries := normalizedCopy(queries)

	base := smallConfig()
	base.EF = 4 * k
	index := buildTestIndex(t, vectors, provider, base)

	type outcome struct {
		results   [][]uint64
		endpoints int
		cut       int
		recall    float64
	}
	run := func(margin float32) outcome {
		uc := base
		uc.SourceMargin = margin
		require.NoError(t, index.UpdateUserConfig(uc, func() {}))
		var out outcome
		for _, query := range queries {
			ids, _, stats, err := index.SearchByVectorWithStats(context.Background(), query, k, nil)
			require.NoError(t, err)
			out.results = append(out.results, ids)
			out.endpoints += stats.EndpointsScored
			out.cut += stats.SourcesCut
			require.Equal(t, stats.EdgesFollowed,
				stats.EndpointsInLoaded+stats.EndpointsRepeated()+stats.EndpointsScored)
		}
		out.recall = recallAt(t, index, truth, normalizedQueries, k, provider)
		return out
	}

	none := run(0)
	wide := run(10)
	tight := run(0.02)
	t.Logf("endpoints scored: no margin %d, margin 0.02 %d (%d sources cut); recall %.3f vs %.3f",
		none.endpoints, tight.endpoints, tight.cut, none.recall, tight.recall)

	assert.Zero(t, none.cut)
	assert.Zero(t, wide.cut, "a margin wider than any distance cuts nothing")
	assert.Equal(t, none.results, wide.results)
	assert.Equal(t, none.endpoints, wide.endpoints)

	assert.Positive(t, tight.cut)
	assert.Less(t, tight.endpoints, none.endpoints*3/4, "the cut must save a good part of the scoring")
	assert.Greater(t, tight.recall, none.recall-0.05, "and must not wreck the recall")
}

func TestSearchWithoutRescoringReturnsEstimates(t *testing.T) {
	const k = 10
	provider := distancer.NewL2SquaredProvider()
	all := clusteredVectors(2020, 24, 30, 101)
	vectors, queries := all[:2000], all[2000:]

	uc := smallConfig()
	uc.Distance = "l2-squared"
	uc.RescoreLimit = 0
	uc.EF = 4 * k
	index := buildTestIndex(t, vectors, provider, uc)

	for _, query := range queries {
		ids, dists, stats, err := index.SearchByVectorWithStats(context.Background(), query, k, nil)
		require.NoError(t, err)
		require.Len(t, ids, k)
		require.Zero(t, stats.Rescored)
		require.True(t, slices.IsSorted(dists), "results are ordered nearest first")
	}
}

func TestSearchReturnsFewerThanKOnATinyIndex(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(3, 8, 1, 111)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)

	ids, dists, err := index.SearchByVector(context.Background(), vectors[1], 10, nil)
	require.NoError(t, err)
	require.Len(t, ids, 3)
	require.Len(t, dists, 3)
	assert.Equal(t, uint64(1), ids[0], "a vector is its own nearest neighbor")
	assert.InDelta(t, 0, dists[0], 1e-6)
}

// Searches share nothing but the built state and the scratch pools. Run
// with -race.
func TestSearchIsSafeForConcurrentUse(t *testing.T) {
	const k = 10
	provider := distancer.NewCosineDistanceProvider()
	all := clusteredVectors(3064, 24, 40, 121)
	vectors, queries := all[:3000], all[3000:]

	uc := smallConfig()
	uc.EF = 2 * k
	index := buildTestIndex(t, vectors, provider, uc)

	// The reference results, computed alone.
	want := make([][]uint64, len(queries))
	for i, query := range queries {
		ids, _, err := index.SearchByVector(context.Background(), query, k, nil)
		require.NoError(t, err)
		want[i] = ids
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8*len(queries))
	mismatches := make(chan int, 8*len(queries))
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 3; round++ {
				for i, query := range queries {
					ids, _, err := index.SearchByVector(context.Background(), query, k, nil)
					if err != nil {
						errs <- err
						return
					}
					if !slices.Equal(ids, want[i]) {
						mismatches <- i
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	close(mismatches)

	for err := range errs {
		require.NoError(t, err)
	}
	for i := range mismatches {
		t.Errorf("query %d returned a different result under concurrency", i)
	}
}

func TestSearchRejectsWhatItCannotServe(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(300, 8, 5, 131)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)
	ctx := context.Background()

	t.Run("a query of the wrong length", func(t *testing.T) {
		_, _, err := index.SearchByVector(ctx, make([]float32, 7), 5, nil)
		require.Error(t, err)
	})

	t.Run("a query that is not finite", func(t *testing.T) {
		for name, value := range map[string]float32{
			"NaN": float32(math.NaN()), "infinite": float32(math.Inf(1)), "overflowing": 3e19,
		} {
			query := slices.Clone(vectors[0])
			query[3], query[4] = value, value
			ids, _, err := index.SearchByVector(ctx, query, 5, nil)
			require.Errorf(t, err, "%s query", name)
			require.Emptyf(t, ids, "%s query", name)
		}
	})

	t.Run("a non-positive k", func(t *testing.T) {
		_, _, err := index.SearchByVector(ctx, vectors[0], 0, nil)
		require.Error(t, err)
	})

	t.Run("a cancelled context", func(t *testing.T) {
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, _, err := index.SearchByVector(cancelled, vectors[0], 5, nil)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("a search by distance", func(t *testing.T) {
		_, _, err := index.SearchByVectorDistance(ctx, vectors[0], 0.5, 10, nil)
		require.ErrorIs(t, err, ErrNotSupported)
	})
}

func TestSelectBatchPicksTheBestRankedUnloadedPostings(t *testing.T) {
	tests := []struct {
		name         string
		rank         map[uint32]float32
		loaded       []uint32
		limit        int
		wantBatch    []uint32
		wantFrontier []uint32
	}{
		{
			name:         "fewer postings than the limit",
			rank:         map[uint32]float32{3: 0.5, 1: 0.2},
			limit:        4,
			wantBatch:    []uint32{1, 3},
			wantFrontier: []uint32{1, 3},
		},
		{
			name:         "only the best are picked, best first",
			rank:         map[uint32]float32{0: 0.9, 1: 0.1, 2: 0.5, 3: 0.3, 4: 0.7},
			limit:        2,
			wantBatch:    []uint32{1, 3},
			wantFrontier: []uint32{0, 1, 2, 3, 4},
		},
		{
			name:         "loaded postings are skipped and swept out",
			rank:         map[uint32]float32{0: 0.9, 1: 0.1, 2: 0.5, 3: 0.3},
			loaded:       []uint32{1, 3},
			limit:        2,
			wantBatch:    []uint32{2, 0},
			wantFrontier: []uint32{0, 2},
		},
		{
			name:         "ties are broken by posting id",
			rank:         map[uint32]float32{5: 0.4, 2: 0.4, 7: 0.4},
			limit:        2,
			wantBatch:    []uint32{2, 5},
			wantFrontier: []uint32{2, 5, 7},
		},
		{
			name:         "nothing known",
			rank:         map[uint32]float32{},
			limit:        3,
			wantBatch:    []uint32{},
			wantFrontier: []uint32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := newSearchScratch(8)
			for p := uint32(0); p < 8; p++ {
				if rank, ok := tt.rank[p]; ok {
					sc.rank[p] = rank
					sc.frontier = append(sc.frontier, p)
				}
			}
			for _, p := range tt.loaded {
				sc.markLoaded(p)
			}

			sc.selectBatch(tt.limit)

			batch := []uint32{}
			for _, pick := range sc.batch {
				batch = append(batch, uint32(pick.id))
			}
			assert.Equal(t, tt.wantBatch, batch)
			assert.Equal(t, tt.wantFrontier, append([]uint32{}, sc.frontier...))
		})
	}
}

// A scratch must come back from a search as clean as a new one, or the next
// search would inherit ranks and loaded postings.
func TestSearchScratchIsCleanAfterUse(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	all := clusteredVectors(2010, 24, 30, 141)
	vectors, queries := all[:2000], all[2000:]
	uc := smallConfig()
	uc.Distance = "l2-squared"
	uc.EF = 50
	index := buildTestIndex(t, vectors, provider, uc)
	st := index.state.Load()

	for _, query := range queries {
		_, _, err := index.SearchByVector(context.Background(), query, 10, nil)
		require.NoError(t, err)

		sc := st.borrowScratch()
		for p := range sc.rank {
			require.Equalf(t, inf32, sc.rank[p], "rank of posting %d", p)
			require.Falsef(t, sc.loaded[p], "loaded flag of posting %d", p)
		}
		require.Empty(t, sc.frontier)
		require.Empty(t, sc.touched)
		st.returnScratch(sc)
	}
}
